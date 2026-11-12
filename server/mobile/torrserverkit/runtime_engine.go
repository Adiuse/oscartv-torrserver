package torrserverkit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	goruntime "runtime"
	runtimemetrics "runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	infohashv2 "github.com/anacrolix/torrent/types/infohash-v2"
)

type playbackRuntime struct {
	mu       sync.RWMutex
	root     string
	port     int
	closed   bool
	sessions map[string]*runtimePlaybackSession
}

type playbackConfig struct {
	SourceID           string            `json:"sourceId"`
	GroupID            string            `json:"groupId"`
	ContainerExtension string            `json:"containerExtension"`
	Candidates         []candidateConfig `json:"candidates"`
}

type candidateConfig struct {
	CandidateID string       `json:"candidateId"`
	InfoHashV1  string       `json:"infoHashV1,omitempty"`
	InfoHashV2  string       `json:"infoHashV2,omitempty"`
	Trackers    []string     `json:"trackers"`
	WebSeeds    []string     `json:"webSeeds"`
	Target      targetConfig `json:"target"`
}

type targetConfig struct {
	Name         string `json:"name"`
	RelativePath string `json:"relativePath,omitempty"`
	LengthBytes  string `json:"lengthBytes"`
}

type runtimePlaybackSession struct {
	mu             sync.RWMutex
	demandUpdateMu sync.Mutex

	id        string
	config    playbackConfig
	root      string
	client    *torrent.Client
	storage   *sessionStorage
	store     *blockStore
	candidates map[string]*runtimeCandidate

	primaryCandidateID string
	targetAssetKey     string
	targetLength       int64

	demandOffset int64
	demandLength int64

	requestedDemandOffset int64
	requestedDemandLength int64
	forwardWindowBytes     int64

	demandEpoch         uint64
	demandRequestCancel context.CancelFunc
	hasDemand           bool

	metrics sessionMetrics

	startedAt time.Time
	closed    bool
}

type runtimeCandidate struct {

	mu          sync.Mutex
	lifecycleMu sync.Mutex

	config  candidateConfig
	torrent *torrent.Torrent
	binding *storageBinding
	file    *torrent.File
	reader  torrent.Reader

	readerCancel  context.CancelFunc
	readerDone    chan struct{}
	readCancel    context.CancelFunc
	demandChanged chan struct{}
	demandOffset  int64
	demandLength  int64

	ready         chan struct{}
	readyErr      error
	desiredActive bool
}

type sessionMetrics struct {
	mu sync.Mutex

	rawBytes            uint64
	usefulVerifiedBytes uint64
	duplicateBytes      uint64
	servedBytes         uint64
	demandOffset        int64
	demandLength        int64

	candidates map[string]*candidateMetrics
	ownership  []ownedInterval
	credited   []byteInterval
}

type candidateMetrics struct {
	rawBytes            uint64
	usefulVerifiedBytes uint64
	duplicateBytes      uint64
	verificationSuccess int
	verificationFailure int
	firstVerifiedAtMs    *int64
	verifiedForSource   bool
}

type ownedInterval struct {
	start       int64
	end         int64
	candidateID string
	generation  uint64
}

type snapshotPayload struct {
	RawBytes            string                              `json:"rawBytes"`
	UsefulVerifiedBytes string                              `json:"usefulVerifiedBytes"`
	DuplicateBytes      string                              `json:"duplicateBytes"`
	DiskIOBytes         string                              `json:"diskIoBytes"`
	DiskReadBytes       string                              `json:"diskReadBytes"`
	DiskWriteBytes      string                              `json:"diskWriteBytes"`
	RAMBytes            string                              `json:"ramBytes"`
	CPUSeconds          *float64                            `json:"cpuSeconds,omitempty"`
	ServedBytes         string                              `json:"servedBytes"`
	SessionElapsedMs    int64                               `json:"sessionElapsedMs"`
	DemandEpoch         uint64                              `json:"demandEpoch"`
	ConnectionCount     int                                 `json:"connectionCount"`
	ActiveCandidateIDs  []string                            `json:"activeCandidateIds"`
	PrimaryCandidateID  string                              `json:"primaryCandidateId,omitempty"`
	Candidates          map[string]candidateSnapshotPayload `json:"candidates"`
}

type candidateSnapshotPayload struct {

	RawBytes                 string   `json:"rawBytes"`
	UsefulVerifiedBytes      string   `json:"usefulVerifiedBytes"`
	DuplicateBytes           string   `json:"duplicateBytes"`
	ActivePeers              int      `json:"activePeers"`
	VerificationSuccess      int      `json:"verificationSuccess"`
	VerificationFailure      int      `json:"verificationFailure"`
	TimeToFirstVerifiedByteMs *int64   `json:"timeToFirstVerifiedByteMs,omitempty"`
	UrgentCoverage           *float64 `json:"urgentCoverage,omitempty"`
	VerifiedForSource        bool     `json:"verifiedForSource"`
}

func newPlaybackRuntime(
	root string,
	port int,
) (*playbackRuntime, error) {
	if err := os.RemoveAll(root); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}

	return &playbackRuntime{
		root:     root,
		port:     port,
		sessions: make(map[string]*runtimePlaybackSession),
	}, nil
}

func (r *playbackRuntime) openPlayback(
	configJSON string,
) (string, error) {
	var config playbackConfig
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return "", fmt.Errorf("invalid playback config: %w", err)
	}
	if err := validatePlaybackConfig(config); err != nil {
		return "", err
	}

	playbackID, err := randomPlaybackID()
	if err != nil {
		return "", err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return "", errors.New("torrent playback runtime is closed")
	}
	if len(r.sessions) != 0 {
		return "", errors.New("another torrent playback is still active")
	}

	sessionRoot := filepath.Join(r.root, playbackID)
	session, err := newRuntimePlaybackSession(
		playbackID,
		config,
		sessionRoot,
	)
	if err != nil {
		return "", err
	}

	r.sessions[playbackID] = session

	result := map[string]any{
		"playbackId": playbackID,
		"localUrl": fmt.Sprintf(
			"http://127.0.0.1:%d/oscartv/torrent/%s/media.%s",
			r.port,
			playbackID,
			config.ContainerExtension,
		),
		"containerExtension": config.ContainerExtension,
	}
	buf, err := json.Marshal(result)
	if err != nil {
		delete(r.sessions, playbackID)
		cleanupErr := session.close()
		return "", errors.Join(err, cleanupErr)
	}
	return string(buf), nil
}

func newRuntimePlaybackSession(
	id string,
	config playbackConfig,
	root string,
) (*runtimePlaybackSession, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}

	session := &runtimePlaybackSession{
		id:         id,
		config:     config,
		root:       root,
		candidates: make(map[string]*runtimeCandidate),
		targetAssetKey: "shared:" + config.GroupID,
		startedAt:  time.Now(),
	}
	session.metrics.candidates = make(map[string]*candidateMetrics)

	store, err := newBlockStore(
		filepath.Join(root, "store"),
		sharedStoreMaximumBytes,
	)
	if err != nil {
		cleanupErr := os.RemoveAll(root)
		return nil, errors.Join(err, cleanupErr)
	}
	session.store = store

	storage := newSessionStorage(session, store)
	session.storage = storage
	store.onEvict = session.onStoreEviction
	store.onAttributionEvict = session.onAttributionEviction

	clientConfig := torrent.NewDefaultClientConfig()
	clientConfig.DefaultStorage = storage
	clientConfig.DataDir = filepath.Join(root, "anacrolix")
	client, err := torrent.NewClient(clientConfig)
	if err != nil {
		store.close()
		cleanupErr := os.RemoveAll(root)
		return nil, errors.Join(err, cleanupErr)
	}
	session.client = client

	for _, candidateConfig := range config.Candidates {
		candidate, err := session.addCandidate(candidateConfig)
		if err != nil {
			cleanupErr := session.close()
			return nil, errors.Join(err, cleanupErr)
		}
		session.candidates[candidateConfig.CandidateID] = candidate
		session.metrics.candidates[candidateConfig.CandidateID] =
			&candidateMetrics{}
	}

	first := config.Candidates[0]
	session.primaryCandidateID = first.CandidateID
	targetLength, err := strconv.ParseInt(
		first.Target.LengthBytes,
		10,
		64,
	)
	if err != nil || targetLength <= 0 {
		targetErr := err
		if targetErr == nil {
			targetErr = errors.New("invalid target length")
		}
		cleanupErr := session.close()
		return nil, errors.Join(targetErr, cleanupErr)
	}
	session.targetLength = targetLength

	return session, nil
}

func (s *runtimePlaybackSession) addCandidate(
	config candidateConfig,
) (*runtimeCandidate, error) {
	spec, storageHash, err := candidateTorrentSpec(config)
	if err != nil {
		return nil, err
	}

	binding, err := s.storage.register(
		storageHash,
		config,
	)
	if err != nil {
		return nil, err
	}

	t, _, err := s.client.AddTorrentSpec(spec)
	if err != nil {
		s.storage.unregister(storageHash)
		return nil, err
	}

	binding.attachTorrent(t)

	candidate := &runtimeCandidate{
		config:  config,
		torrent: t,
		binding: binding,
		ready:   make(chan struct{}),
	}

	go candidate.resolve(s)

	return candidate, nil
}

func candidateTorrentSpec(
	config candidateConfig,
) (*torrent.TorrentSpec, metainfo.Hash, error) {
	spec := &torrent.TorrentSpec{
		Trackers: [][]string{
			uniqueStrings(config.Trackers),
		},
		Webseeds: uniqueStrings(config.WebSeeds),
	}

	var storageHash metainfo.Hash

	if config.InfoHashV1 != "" {
		var hash metainfo.Hash
		if err := hash.FromHexString(
			strings.ToLower(config.InfoHashV1),
		); err != nil {
			return nil, storageHash, err
		}
		spec.InfoHash = hash
		storageHash = hash
	}

	if config.InfoHashV2 != "" {
		var hash infohashv2.T
		if err := hash.FromHexString(
			strings.ToLower(config.InfoHashV2),
		); err != nil {
			return nil, storageHash, err
		}
		spec.InfoHashV2.Set(hash)
		if storageHash.IsZero() {
			copy(storageHash[:], hash[:len(storageHash)])
		}
	}

	if storageHash.IsZero() {
		return nil, storageHash, errors.New("torrent candidate has no infohash")
	}

	return spec, storageHash, nil
}

func (c *runtimeCandidate) resolve(
	session *runtimePlaybackSession,
) {
	select {
	case <-c.torrent.GotInfo():
	case <-time.After(0):
		select {
		case <-c.torrent.GotInfo():
		}
	}

	file, err := resolveTorrentTarget(
		c.torrent,
		c.config.Target,
	)

	c.mu.Lock()
	c.file = file
	c.readyErr = err
	c.mu.Unlock()

	close(c.ready)

	if err == nil {
		_ = c.startReaderIfRequested(session)
	}
}

func resolveTorrentTarget(
	t *torrent.Torrent,
	target targetConfig,
) (*torrent.File, error) {
	length, err := strconv.ParseInt(
		target.LengthBytes,
		10,
		64,
	)
	if err != nil || length <= 0 {
		return nil, errors.New("invalid target length")
	}

	relativePath := normalizePath(target.RelativePath)
	name := normalizePath(target.Name)

	var pathMatches []*torrent.File
	var nameMatches []*torrent.File

	for _, file := range t.Files() {
		if file.Length() != length {
			continue
		}

		path := normalizePath(file.Path())

		if relativePath != "" &&
			(path == relativePath ||
				strings.HasSuffix(path, "/"+relativePath)) {
			pathMatches = append(pathMatches, file)
		}

		if basePath(path) == basePath(name) {
			nameMatches = append(nameMatches, file)
		}
	}

	if len(pathMatches) == 1 {
		return pathMatches[0], nil
	}
	if len(pathMatches) > 1 {
		return nil, errors.New("torrent target path is ambiguous")
	}
	if len(nameMatches) == 1 {
		return nameMatches[0], nil
	}
	if len(nameMatches) > 1 {
		return nil, errors.New("torrent target name is ambiguous")
	}
	return nil, errors.New("torrent target file not found")
}

func (c *runtimeCandidate) activate(
	session *runtimePlaybackSession,
) error {
	c.mu.Lock()
	c.desiredActive = true
	c.mu.Unlock()

	return c.startReaderIfRequested(session)
}

func (c *runtimeCandidate) startReaderIfRequested(
	session *runtimePlaybackSession,
) error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.mu.Lock()

	if !c.desiredActive {
		c.mu.Unlock()
		return nil
	}

	select {
	case <-c.ready:
		if c.readyErr != nil {
			err := c.readyErr
			c.mu.Unlock()
			return err
		}
	default:
		c.mu.Unlock()
		return nil
	}

	if c.reader != nil {
		c.mu.Unlock()
		return nil
	}

	file := c.file
	if file == nil {
		c.mu.Unlock()
		return errors.New("torrent target file unavailable")
	}

	c.mu.Unlock()

	session.mu.RLock()
	demandOffset := session.demandOffset
	demandLength := session.demandLength
	currentDemandLength := minInt64(session.requestedDemandLength, demandLength)
	demandGeneration := session.demandEpoch
	session.mu.RUnlock()

	if demandLength > 0 {
		if err := c.binding.setDemand(
			demandOffset,
			currentDemandLength,
			demandLength,
			demandGeneration,
		); err != nil {
			if errors.Is(err, errFullMovieVerificationEnvelope) {
				c.rejectFullMovieDemand(demandGeneration)
			}
			return err
		}
	} else if err := c.binding.clearDemand(demandGeneration); err != nil {
		return err
	}

	reader := file.NewReader()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	demandChanged := make(chan struct{}, 1)

	c.mu.Lock()
	if !c.desiredActive || c.reader != nil {
		c.mu.Unlock()
		cancel()
		reader.Close()
		return nil
	}

	c.reader = reader
	c.readerCancel = cancel
	c.readerDone = done
	c.demandChanged = demandChanged
	c.demandOffset = demandOffset
	c.demandLength = demandLength
	c.mu.Unlock()

	go c.runReader(
		ctx,
		reader,
		done,
		demandChanged,
	)

	if demandLength > 0 {
		demandChanged <- struct{}{}
	}

	return nil
}

func (c *runtimeCandidate) runReader(
	ctx context.Context,
	reader torrent.Reader,
	done chan struct{},
	demandChanged <-chan struct{},
) {
	defer close(done)
	defer reader.Close()

	var firstByte [1]byte

	for {
		select {
		case <-ctx.Done():
			return
		case <-demandChanged:
		}

		c.mu.Lock()
		offset := c.demandOffset
		length := c.demandLength
		readContext, cancelRead := context.WithCancel(ctx)
		c.readCancel = cancelRead
		c.mu.Unlock()

		if length > 0 && readContext.Err() == nil {
			reader.SetContext(readContext)

			if _, err := reader.Seek(
				offset,
				io.SeekStart,
			); err == nil {
				reader.SetReadahead(length)
				_, _ = reader.Read(firstByte[:])
			}
		}

		c.mu.Lock()
		c.readCancel = nil
		c.mu.Unlock()

		cancelRead()
	}
}

func (c *runtimeCandidate) suspend() error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.mu.Lock()
	c.desiredActive = false

	cancel := c.readerCancel
	done := c.readerDone

	c.reader = nil
	c.readerCancel = nil
	c.readerDone = nil
	c.readCancel = nil
	c.demandChanged = nil
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}

	return c.binding.clearDemand(c.binding.session.currentDemandGeneration())
}

func (c *runtimeCandidate) rejectFullMovieDemand(
	generation uint64,
) {
	c.mu.Lock()
	c.desiredActive = false
	cancel := c.readerCancel
	done := c.readerDone
	c.reader = nil
	c.readerCancel = nil
	c.readerDone = nil
	c.readCancel = nil
	c.demandChanged = nil
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	_ = c.binding.clearDemand(generation)
}

func (c *runtimeCandidate) updateDemand(
	offset int64,
	currentLength int64,
	length int64,
	generation uint64,
) error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	c.mu.Lock()
	if !c.desiredActive {
		c.mu.Unlock()
		return nil
	}

	c.demandOffset = offset
	c.demandLength = length

	reader := c.reader
	readCancel := c.readCancel
	demandChanged := c.demandChanged
	c.mu.Unlock()

	if readCancel != nil {
		readCancel()
	}

	if length > 0 {
		if err := c.binding.setDemand(
			offset,
			currentLength,
			length,
			generation,
		); err != nil {
			if errors.Is(err, errFullMovieVerificationEnvelope) {
				c.rejectFullMovieDemand(generation)
			}
			return err
		}
	} else if err := c.binding.clearDemand(generation); err != nil {
		return err
	}

	if reader == nil {
		return nil
	}

	select {
	case demandChanged <- struct{}{}:
	default:
	}

	return nil
}

func (r *playbackRuntime) activateCandidate(
	playbackID string,
	candidateID string,
) error {
	session, err := r.session(playbackID)
	if err != nil {
		return err
	}
	return session.activateCandidate(candidateID)
}

func (s *runtimePlaybackSession) activateCandidate(
	candidateID string,
) error {
	s.demandUpdateMu.Lock()
	defer s.demandUpdateMu.Unlock()

	candidate := s.candidates[candidateID]
	if candidate == nil {
		return errors.New("torrent candidate not found")
	}
	return candidate.activate(s)
}

func (r *playbackRuntime) suspendCandidate(
	playbackID string,
	candidateID string,
) error {
	session, err := r.session(playbackID)
	if err != nil {
		return err
	}

	session.demandUpdateMu.Lock()
	defer session.demandUpdateMu.Unlock()

	candidate := session.candidates[candidateID]
	if candidate == nil {
		return errors.New("torrent candidate not found")
	}
	return candidate.suspend()
}

func (r *playbackRuntime) promoteCandidate(
	playbackID string,
	candidateID string,
) error {
	session, err := r.session(playbackID)
	if err != nil {
		return err
	}

	session.demandUpdateMu.Lock()
	defer session.demandUpdateMu.Unlock()

	candidate := session.candidates[candidateID]
	if candidate == nil {
		return errors.New("torrent candidate not found")
	}

	candidate.lifecycleMu.Lock()
	defer candidate.lifecycleMu.Unlock()

	candidate.mu.Lock()
	ready := candidate.desiredActive &&
		candidate.file != nil &&
		candidate.reader != nil &&
		candidate.readyErr == nil
	candidate.mu.Unlock()

	if !ready {
		return errors.New("torrent candidate is not ready for promotion")
	}

	session.mu.Lock()
	session.primaryCandidateID = candidateID
	session.mu.Unlock()
	return nil
}

func (r *playbackRuntime) snapshotPlayback(
	playbackID string,
) (string, error) {
	session, err := r.session(playbackID)
	if err != nil {
		return "", err
	}

	payload := session.snapshot()
	buf, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func (s *runtimePlaybackSession) snapshot() snapshotPayload {
	s.metrics.mu.Lock()
	rawBytes := s.metrics.rawBytes
	usefulBytes := s.metrics.usefulVerifiedBytes
	duplicateBytes := s.metrics.duplicateBytes
	servedBytes := s.metrics.servedBytes
	demandStart := s.metrics.demandOffset
	demandLength := s.metrics.demandLength

	candidateCounters := make(
		map[string]candidateMetrics,
		len(s.metrics.candidates),
	)
	for id, metrics := range s.metrics.candidates {
		candidateCounters[id] = *metrics
	}

	coverageByCandidate := make(map[string]float64)
	if demandLength > 0 {
		demandEnd := demandStart + demandLength
		for _, owner := range s.metrics.ownership {
			start := maxInt64(owner.start, demandStart)
			end := minInt64(owner.end, demandEnd)
			if end > start {
				coverageByCandidate[owner.candidateID] +=
					float64(end-start) / float64(demandLength)
			}
		}
	}
	s.metrics.mu.Unlock()

	activeIDs := make([]string, 0)
	candidatePayloads := make(
		map[string]candidateSnapshotPayload,
		len(s.candidates),
	)
	connectionCount := 0

	for id, candidate := range s.candidates {
		candidate.mu.Lock()
		active := candidate.desiredActive
		t := candidate.torrent
		candidate.mu.Unlock()

		if active {
			activeIDs = append(activeIDs, id)
		}

		activePeers := 0
		if t != nil {
			stats := t.Stats()
			activePeers = stats.ActivePeers
			connectionCount += stats.ActivePeers
		}

		counter := candidateCounters[id]
		var urgentCoverage *float64
		if demandLength > 0 {
			value := coverageByCandidate[id]
			if value > 1 {
				value = 1
			}
			urgentCoverage = &value
		}

		candidatePayloads[id] = candidateSnapshotPayload{
			RawBytes: strconv.FormatUint(
				counter.rawBytes,
				10,
			),
			UsefulVerifiedBytes: strconv.FormatUint(
				counter.usefulVerifiedBytes,
				10,
			),
			DuplicateBytes: strconv.FormatUint(
				counter.duplicateBytes,
				10,
			),
			ActivePeers: activePeers,
			VerificationSuccess: counter.verificationSuccess,
			VerificationFailure: counter.verificationFailure,
			TimeToFirstVerifiedByteMs: counter.firstVerifiedAtMs,
			UrgentCoverage: urgentCoverage,
			VerifiedForSource: counter.verifiedForSource,
		}
	}

	s.mu.RLock()
	primaryID := s.primaryCandidateID
	demandEpoch := s.demandEpoch
	s.mu.RUnlock()

	var memory goruntime.MemStats
	goruntime.ReadMemStats(&memory)

	return snapshotPayload{
		RawBytes: strconv.FormatUint(
			rawBytes,
			10,
		),
		UsefulVerifiedBytes: strconv.FormatUint(
			usefulBytes,
			10,
		),
		DuplicateBytes: strconv.FormatUint(
			duplicateBytes,
			10,
		),
		DiskIOBytes: strconv.FormatUint(
			s.store.diskIOBytes(),
			10,
		),
		DiskReadBytes: strconv.FormatUint(
			s.store.diskReadBytes.Load(),
			10,
		),
		DiskWriteBytes: strconv.FormatUint(
			s.store.diskWriteBytes.Load(),
			10,
		),
		RAMBytes: strconv.FormatUint(
			memory.Alloc,
			10,
		),
		CPUSeconds: processCPUSeconds(),
		ServedBytes: strconv.FormatUint(
			servedBytes,
			10,
		),
		SessionElapsedMs: time.Since(s.startedAt).Milliseconds(),
		DemandEpoch:        demandEpoch,
		ConnectionCount:    connectionCount,
		ActiveCandidateIDs: activeIDs,
		PrimaryCandidateID: primaryID,
		Candidates:         candidatePayloads,
	}
}

func processCPUSeconds() *float64 {

	samples := []runtimemetrics.Sample{
		{
			Name: "/cpu/classes/total:cpu-seconds",
		},
	}
	runtimemetrics.Read(samples)

	if samples[0].Value.Kind() !=
		runtimemetrics.KindFloat64 {
		return nil
	}

	seconds := samples[0].Value.Float64()
	return &seconds
}

func (r *playbackRuntime) closePlayback(
	playbackID string,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	session := r.sessions[playbackID]
	if session == nil {
		return nil
	}

	delete(r.sessions, playbackID)
	return session.close()
}

func (s *runtimePlaybackSession) close() error {
	s.demandUpdateMu.Lock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.demandUpdateMu.Unlock()
		return nil
	}
	s.closed = true
	cancelDemandRequest := s.demandRequestCancel
	s.demandRequestCancel = nil
	s.mu.Unlock()
	s.demandUpdateMu.Unlock()

	if cancelDemandRequest != nil {
		cancelDemandRequest()
	}

	var closeErrors []error

	for _, candidate := range s.candidates {
		if err := candidate.suspend(); err != nil {
			closeErrors = append(
				closeErrors,
				fmt.Errorf(
					"suspend torrent candidate %s: %w",
					candidate.config.CandidateID,
					err,
				),
			)
		}
		if candidate.torrent != nil {
			candidate.torrent.Drop()
		}
	}

	if s.client != nil {
		s.client.Close()
	}
	if s.store != nil {
		s.store.close()
	}
	if err := os.RemoveAll(s.root); err != nil {
		closeErrors = append(closeErrors, err)
	}

	return errors.Join(closeErrors...)
}

func (r *playbackRuntime) session(
	id string,
) (*runtimePlaybackSession, error) {
	r.mu.RLock()
	session := r.sessions[id]
	r.mu.RUnlock()

	if session == nil {
		return nil, errors.New("torrent playback not found")
	}
	return session, nil
}

func (r *playbackRuntime) close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.closed = true

	var closeErrors []error

	for id, session := range r.sessions {
		if err := session.close(); err != nil {
			closeErrors = append(
				closeErrors,
				fmt.Errorf(
					"close torrent playback %s: %w",
					id,
					err,
				),
			)
		}
		delete(r.sessions, id)
	}

	if err := os.RemoveAll(r.root); err != nil {
		closeErrors = append(closeErrors, err)
	}

	return errors.Join(closeErrors...)
}

func (s *runtimePlaybackSession) currentDemandGeneration() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.demandEpoch
}

func (s *runtimePlaybackSession) beginDemandRequest(
	_ int64,
	cancel context.CancelFunc,
) uint64 {
	s.demandUpdateMu.Lock()
	s.mu.Lock()
	if s.closed {
		generation := s.demandEpoch
		s.mu.Unlock()
		s.demandUpdateMu.Unlock()
		if cancel != nil {
			cancel()
		}
		return generation
	}
	s.demandEpoch++
	generation := s.demandEpoch
	previousCancel := s.demandRequestCancel
	s.demandRequestCancel = cancel
	s.mu.Unlock()
	s.demandUpdateMu.Unlock()

	if previousCancel != nil {
		previousCancel()
	}
	return generation
}

func (s *runtimePlaybackSession) endDemandRequest(
	generation uint64,
) {
	s.mu.Lock()
	if s.demandEpoch == generation {
		s.demandRequestCancel = nil
	}
	s.mu.Unlock()
}

func (s *runtimePlaybackSession) setDemandForGeneration(
	offset int64,
	length int64,
	generation uint64,
) error {
	s.demandUpdateMu.Lock()
	defer s.demandUpdateMu.Unlock()

	s.mu.RLock()
	currentGeneration := s.demandEpoch
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return errors.New("torrent playback session is closed")
	}
	if generation != currentGeneration {
		return errors.New("stale torrent HTTP range generation")
	}
	return s.setDemandLocked(
		offset,
		length,
		false,
	)
}

func (s *runtimePlaybackSession) setDemand(
    offset int64,
    length int64,
) error {
    s.demandUpdateMu.Lock()
    defer s.demandUpdateMu.Unlock()

    return s.setDemandLocked(
        offset,
        length,
        true,
    )
}

func (s *runtimePlaybackSession) setForwardWindow(
    forwardWindowBytes int64,
) error {
    if forwardWindowBytes < 0 {
        forwardWindowBytes = 0
    }
    if forwardWindowBytes > sharedStoreMaximumBytes {
        forwardWindowBytes = sharedStoreMaximumBytes
    }

    s.demandUpdateMu.Lock()
    defer s.demandUpdateMu.Unlock()

    s.mu.Lock()
    s.forwardWindowBytes = forwardWindowBytes
    hasDemand := s.hasDemand
    offset := s.requestedDemandOffset
    length := s.requestedDemandLength
    s.mu.Unlock()

    if !hasDemand {
        return nil
    }

    return s.setDemandLocked(
        offset,
        length,
        false,
    )
}

func (s *runtimePlaybackSession) setDemandLocked(
    offset int64,
    length int64,
    trackReadPosition bool,
) error {
    if offset < 0 {
        offset = 0
    }
    if length < 0 {
        length = 0
    }

    s.mu.Lock()
    if s.closed {
        s.mu.Unlock()
        return errors.New("torrent playback session is closed")
    }

    var cancelDemandRequest context.CancelFunc
    if trackReadPosition &&
        s.hasDemand &&
        offset != s.requestedDemandOffset+s.requestedDemandLength {
        s.demandEpoch++
        cancelDemandRequest = s.demandRequestCancel
        s.demandRequestCancel = nil
    }

    s.requestedDemandOffset = offset
    s.requestedDemandLength = length

    effectiveLength := length
    if s.forwardWindowBytes > 0 {
        if effectiveLength > sharedStoreMaximumBytes-s.forwardWindowBytes {
            effectiveLength = sharedStoreMaximumBytes
        } else {
            effectiveLength += s.forwardWindowBytes
        }
    }
    effectiveLength = minInt64(effectiveLength, sharedStoreMaximumBytes)

    if s.targetLength > 0 {
        remainingLength :=
            maxInt64(
                0,
                s.targetLength-offset,
            )

        effectiveLength =
            minInt64(
                effectiveLength,
                remainingLength,
            )
    }

    s.hasDemand = true
    s.demandOffset = offset
    s.demandLength = effectiveLength
    currentLength := minInt64(length, effectiveLength)

    s.store.mu.Lock()
    s.metrics.mu.Lock()
    s.metrics.demandOffset = offset
    s.metrics.demandLength = effectiveLength

    for _, owner := range s.metrics.ownership {
        if owner.end <= offset ||
            owner.start >= offset+effectiveLength {
            continue
        }

        s.creditUsefulIntervalLocked(
            owner.candidateID,
            byteInterval{
                start: owner.start,
                end: owner.end,
            },
        )
    }

    s.metrics.mu.Unlock()
    s.store.mu.Unlock()
    demandGeneration := s.demandEpoch
    s.mu.Unlock()

    if cancelDemandRequest != nil {
        cancelDemandRequest()
    }
    for _, candidate := range s.candidates {
        candidate.mu.Lock()
        cancelRead := candidate.readCancel
        candidate.mu.Unlock()
        if cancelRead != nil {
            cancelRead()
        }
    }

    if err := s.store.setDemandRangesForGeneration(
        s.targetAssetKey,
        offset,
        offset+currentLength,
        offset+effectiveLength,
        demandGeneration,
    ); err != nil {
        return err
    }

    var firstCandidateError error
    updatedCandidate := false
    for _, candidate := range s.candidates {
        candidate.mu.Lock()
        active := candidate.desiredActive
        candidate.mu.Unlock()

        if !active {
            continue
        }
        if err := candidate.updateDemand(
            offset,
            currentLength,
            effectiveLength,
            demandGeneration,
        ); err != nil {
            if firstCandidateError == nil {
                firstCandidateError = err
            }
            continue
        }
        updatedCandidate = true
    }

    if !updatedCandidate && firstCandidateError != nil {
        return firstCandidateError
    }
    return nil
}

func (s *runtimePlaybackSession) recordWrite(

	candidateID string,
	raw int64,
	duplicate int64,
) {
	if raw <= 0 {
		return
	}

	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()

	s.metrics.rawBytes += uint64(raw)
	if duplicate > 0 {
		s.metrics.duplicateBytes += uint64(duplicate)
	}

	counter := s.metrics.candidates[candidateID]
	if counter == nil {
		counter = &candidateMetrics{}
		s.metrics.candidates[candidateID] = counter
	}
	counter.rawBytes += uint64(raw)
	if duplicate > 0 {
		counter.duplicateBytes += uint64(duplicate)
	}
}

func (s *runtimePlaybackSession) recordVerification(
	candidateID string,
	success bool,
	verifiedTarget bool,
) {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()

	counter := s.metrics.candidates[candidateID]
	if counter == nil {
		counter = &candidateMetrics{}
		s.metrics.candidates[candidateID] = counter
	}

	if success {
		counter.verificationSuccess++

		if verifiedTarget {
			counter.verifiedForSource = true

			if counter.firstVerifiedAtMs == nil {
				firstVerifiedAt := time.Since(s.startedAt).Milliseconds()
				counter.firstVerifiedAtMs = &firstVerifiedAt
			}
		}
	} else {
		counter.verificationFailure++
	}
}

func (s *runtimePlaybackSession) recordOwnership(
	candidateID string,
	generation uint64,
	intervals []byteInterval,
) {
	if len(intervals) == 0 {
		return
	}

	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()

	for _, interval := range intervals {
		s.metrics.ownership = append(
			s.metrics.ownership,
			ownedInterval{
				start:       interval.start,
				end:         interval.end,
				candidateID: candidateID,
				generation:  generation,
			},
		)

		s.creditUsefulIntervalLocked(
			candidateID,
			interval,
		)
	}
}

func (s *runtimePlaybackSession) creditUsefulIntervalLocked(
	candidateID string,
	interval byteInterval,
) {
	if s.metrics.demandLength <= 0 {
		return
	}

	demandStart := s.metrics.demandOffset
	demandEnd := demandStart + s.metrics.demandLength
	start := maxInt64(interval.start, demandStart)
	end := minInt64(interval.end, demandEnd)
	if end <= start {
		return
	}

	uncredited := subtractCovered(
		byteInterval{
			start: start,
			end:   end,
		},
		s.metrics.credited,
	)
	if len(uncredited) == 0 {
		return
	}

	counter := s.metrics.candidates[candidateID]
	if counter == nil {
		counter = &candidateMetrics{}
		s.metrics.candidates[candidateID] = counter
	}

	for _, useful := range uncredited {
		count := uint64(useful.end - useful.start)
		s.metrics.usefulVerifiedBytes += count
		counter.usefulVerifiedBytes += count
		s.metrics.credited = addInterval(
			s.metrics.credited,
			useful,
		)
	}
}

func (s *runtimePlaybackSession) recordServed(
	start int64,
	end int64,
) {
	if end <= start {
		return
	}

	s.metrics.mu.Lock()
	s.metrics.servedBytes += uint64(end - start)
	s.metrics.mu.Unlock()
}

func (s *runtimePlaybackSession) removeOwnership(
	start int64,
	end int64,
	generation uint64,
) {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()

	next := make([]ownedInterval, 0, len(s.metrics.ownership))
	for _, owner := range s.metrics.ownership {
		if owner.generation > generation ||
			owner.end <= start || owner.start >= end {
			next = append(next, owner)
			continue
		}

		if owner.start < start {
			next = append(
				next,
				ownedInterval{
					start:       owner.start,
					end:         start,
					candidateID: owner.candidateID,
					generation:  owner.generation,
				},
			)
		}
		if owner.end > end {
			next = append(
				next,
				ownedInterval{
					start:       end,
					end:         owner.end,
					candidateID: owner.candidateID,
					generation:  owner.generation,
				},
			)
		}
	}
	s.metrics.ownership = next
}

func (s *runtimePlaybackSession) onSharedVerified(
	sourceCandidateID string,
	start int64,
	end int64,
) {
	for id, candidate := range s.candidates {
		if id == sourceCandidateID {
			continue
		}
		candidate.binding.verifySharedRange(
			start,
			end,
		)
	}
}

func (s *runtimePlaybackSession) onAttributionEviction(
	asset string,
	start int64,
	end int64,
	generation uint64,
) {
	if asset == s.targetAssetKey {
		s.removeOwnership(start, end, generation)
	}
}

func (s *runtimePlaybackSession) onStoreEviction(
	asset string,
	start int64,
	end int64,
	generation uint64,
) {
	if asset == s.targetAssetKey {
		for _, candidate := range s.candidates {
			candidate.binding.invalidateSharedRange(
				start,
				end,
				generation,
			)
		}
		return
	}

	for _, candidate := range s.candidates {
		if candidate.binding.privateAssetKey == asset {
			candidate.binding.invalidatePrivateRange(
				start,
				end,
				generation,
			)
			return
		}
	}
}

func validatePlaybackConfig(
	config playbackConfig,
) error {
	if config.SourceID == "" {
		return errors.New("torrent source id is required")
	}
	if config.GroupID == "" {
		return errors.New("torrent asset group is required")
	}
	if config.ContainerExtension != "mkv" &&
		config.ContainerExtension != "mp4" {
		return errors.New("unsupported torrent container")
	}
	if len(config.Candidates) == 0 {
		return errors.New("torrent candidate is required")
	}

	seen := make(map[string]bool)
	for _, candidate := range config.Candidates {
		if candidate.CandidateID == "" ||
			seen[candidate.CandidateID] {
			return errors.New("invalid torrent candidate id")
		}
		seen[candidate.CandidateID] = true

		if candidate.InfoHashV1 == "" &&
			candidate.InfoHashV2 == "" {
			return errors.New("torrent candidate infohash is required")
		}

		length, err := strconv.ParseInt(
			candidate.Target.LengthBytes,
			10,
			64,
		)
		if err != nil || length <= 0 {
			return errors.New("invalid torrent target length")
		}
	}

	return nil
}

func randomPlaybackID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func uniqueStrings(
	values []string,
) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func normalizePath(
	value string,
) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "\\", "/")
	value = strings.Trim(value, "/")
	return strings.ToLower(value)
}

func basePath(
	value string,
) string {
	if index := strings.LastIndex(value, "/"); index >= 0 {
		return value[index+1:]
	}
	return value
}

func marshalError(
	err error,
) string {
	buf, marshalErr := json.Marshal(
		map[string]string{
			"error": err.Error(),
		},
	)
	if marshalErr != nil {
		return `{"error":"torrent runtime error"}`
	}
	return string(buf)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
