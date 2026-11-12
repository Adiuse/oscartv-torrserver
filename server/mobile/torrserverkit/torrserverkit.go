// Package torrserverkit exposes the embedded OscarTv torrent runtime through
// gomobile bind.
package torrserverkit

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"server"
	"server/log"
	"server/settings"
	"server/web"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type serverStopState struct {
	done chan struct{}
	err  error
}

var (
	runningMu       sync.RWMutex
	running         int32
	runtimePort     int
	embeddedRuntime *playbackRuntime
	serverStarted     bool
	serverStopping    bool
	serverGeneration  uint64
	serverWaitDone    chan struct{}
	activeStop        *serverStopState
)

func StartServer(port int, dataDir string) string {
	runningMu.Lock()
	defer runningMu.Unlock()

	if serverStopping {
		return "embedded TorrServer shutdown is still in progress"
	}

	if serverStarted {
		if atomic.LoadInt32(&running) == 1 {
			return ""
		}
		return "embedded TorrServer stopped unexpectedly; StopServer must complete before restart"
	}

	if embeddedRuntime != nil {
		if err := embeddedRuntime.close(); err != nil {
			return fmt.Sprintf(
				"previous torrent runtime cleanup failed: %v",
				err,
			)
		}
		embeddedRuntime = nil
	}

	activeStop = nil
	web.EmbeddedRouteInstaller = nil
	runtimePort = 0

	resolvedPort, err := reserveLoopbackPort(port)
	if err != nil {
		return err.Error()
	}

	if dataDir == "" {
		dataDir, _ = os.Getwd()
	}

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Sprintf("failed to create data directory: %v", err)
	}

	runtimeRoot := filepath.Join(dataDir, "playback")
	runtime, err := newPlaybackRuntime(runtimeRoot, resolvedPort)
	if err != nil {
		return err.Error()
	}

	settings.Embedded = true
	settings.Path = dataDir
	log.Init(
		filepath.Join(dataDir, "torrserver.log"),
		filepath.Join(dataDir, "web.log"),
	)

	settings.Args = &settings.ExecArgs{
		Port:     strconv.Itoa(resolvedPort),
		IPs:      []string{"127.0.0.1"},
		Path:     dataDir,
		RDB:      false,
		SearchWA: true,
	}

	web.EmbeddedRouteInstaller = runtime.mountRoutes

	if err := server.Start(); err != nil {
		web.EmbeddedRouteInstaller = nil
		server.Stop()
		cleanupErr := runtime.close()
		return errors.Join(err, cleanupErr).Error()
	}

	embeddedRuntime = runtime
	serverStarted = true
	serverGeneration++
	generation := serverGeneration
	runtimePort = resolvedPort
	atomic.StoreInt32(&running, 1)

	waitStarted := make(chan struct{})
	waitDone := make(chan struct{})
	serverWaitDone = waitDone

	go func() {
		defer close(waitDone)

		waitErr := server.WaitServerReady(waitStarted)

		runningMu.Lock()
		if serverGeneration == generation && !serverStopping {
			atomic.StoreInt32(&running, 0)
			runtimePort = 0
		}
		runningMu.Unlock()

		if waitErr != "" {
			log.TLogln("Embedded TorrServer stopped:", waitErr)
		}
	}()

	<-waitStarted

	return ""
}

func StopServer() string {
	runningMu.Lock()

	if serverStopping {
		state := activeStop
		runningMu.Unlock()

		if state == nil {
			return "embedded TorrServer shutdown state is unavailable"
		}
		return waitForServerStop(state)
	}

	runtime := embeddedRuntime
	wasStarted := serverStarted
	waitDone := serverWaitDone

	if runtime == nil && !wasStarted {
		runningMu.Unlock()
		return ""
	}

	embeddedRuntime = nil
	web.EmbeddedRouteInstaller = nil
	serverStopping = true

	state := &serverStopState{
		done: make(chan struct{}),
	}
	activeStop = state
	runningMu.Unlock()

	go func() {
		var cleanupErr error

		if wasStarted {
			server.Stop()
		}

		if waitDone != nil {
			<-waitDone
		}

		if runtime != nil {
			cleanupErr = runtime.close()
		}

		runningMu.Lock()
		if activeStop == state {
			serverStarted = false
			serverWaitDone = nil
			serverStopping = false
			runtimePort = 0
			atomic.StoreInt32(&running, 0)

			if cleanupErr != nil {
				embeddedRuntime = runtime
			}
			state.err = cleanupErr
		}
		runningMu.Unlock()

		close(state.done)
	}()

	return waitForServerStop(state)
}

func waitForServerStop(
	state *serverStopState,
) string {
	select {
	case <-state.done:
		if state.err != nil {
			return state.err.Error()
		}
		return ""
	case <-time.After(5 * time.Second):
		return "server shutdown timed out"
	}
}

func IsRunning() bool {
	runningMu.RLock()
	defer runningMu.RUnlock()
	return !serverStopping &&
		atomic.LoadInt32(&running) == 1
}

func ServerPort() int {
	runningMu.RLock()
	defer runningMu.RUnlock()
	if serverStopping ||
		atomic.LoadInt32(&running) != 1 {
		return 0
	}
	return runtimePort
}

func VerifyRuntimeCapabilities() string {
	runningMu.RLock()
	defer runningMu.RUnlock()

	if serverStopping ||
		atomic.LoadInt32(&running) != 1 {
		return `{"error":"torrent runtime is not running"}`
	}

	runtime := embeddedRuntime
	if runtime == nil {
		return `{"error":"torrent runtime is not running"}`
	}

	result, err := runtime.verifyCapabilities()
	if err != nil {
		return marshalError(err)
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return marshalError(err)
	}
	return string(payload)
}

func OpenPlayback(configJSON string) string {
	runningMu.RLock()
	defer runningMu.RUnlock()

	if serverStopping ||
		atomic.LoadInt32(&running) != 1 {
		return `{"error":"torrent runtime is not running"}`
	}

	runtime := embeddedRuntime
	if runtime == nil {
		return `{"error":"torrent runtime is not running"}`
	}

	result, err := runtime.openPlayback(configJSON)
	if err != nil {
		return marshalError(err)
	}
	return result
}

func ActivateCandidate(playbackID, candidateID string) string {
	return runtimeOperation(func(runtime *playbackRuntime) error {
		return runtime.activateCandidate(playbackID, candidateID)
	})
}

func SuspendCandidate(playbackID, candidateID string) string {
	return runtimeOperation(func(runtime *playbackRuntime) error {
		return runtime.suspendCandidate(playbackID, candidateID)
	})
}

func PromoteCandidate(playbackID, candidateID string) string {
	return runtimeOperation(func(runtime *playbackRuntime) error {
		return runtime.promoteCandidate(playbackID, candidateID)
	})
}

func SnapshotPlayback(playbackID string) string {
	runningMu.RLock()
	defer runningMu.RUnlock()

	if serverStopping ||
		atomic.LoadInt32(&running) != 1 {
		return `{"error":"torrent runtime is not running"}`
	}

	runtime := embeddedRuntime
	if runtime == nil {
		return `{"error":"torrent runtime is not running"}`
	}

	result, err := runtime.snapshotPlayback(playbackID)
	if err != nil {
		return marshalError(err)
	}
	return result
}

func ClosePlayback(playbackID string) string {
	return runtimeOperation(func(runtime *playbackRuntime) error {
		return runtime.closePlayback(playbackID)
	})
}

func UpdateForwardWindow(
	playbackID string,
	forwardWindowBytes int64,
) string {
	return runtimeOperation(func(runtime *playbackRuntime) error {
		session, err := runtime.session(playbackID)
		if err != nil {
			return err
		}
		return session.setForwardWindow(
			forwardWindowBytes,
		)
	})
}

func runtimeOperation(
	operation func(*playbackRuntime) error,
) string {
	runningMu.RLock()
	defer runningMu.RUnlock()

	if serverStopping ||
		atomic.LoadInt32(&running) != 1 {
		return "torrent runtime is not running"
	}

	runtime := embeddedRuntime
	if runtime == nil {
		return "torrent runtime is not running"
	}

	if err := operation(runtime); err != nil {
		return err.Error()
	}
	return ""
}

func reserveLoopbackPort(requested int) (int, error) {
	address := "127.0.0.1:0"
	if requested > 0 {
		address = "127.0.0.1:" + strconv.Itoa(requested)
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return 0, fmt.Errorf("loopback port unavailable: %w", err)
	}
	defer listener.Close()

	_, portValue, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return 0, err
	}

	port, err := strconv.Atoi(portValue)
	if err != nil {
		return 0, err
	}
	return port, nil
}
