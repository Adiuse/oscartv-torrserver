package torrserverkit

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
)

type runtimeCapabilityResult struct {
	Torrent           bool `json:"torrent"`
	MultiSwarmTorrent bool `json:"multiSwarmTorrent"`
}

func (r *playbackRuntime) verifyCapabilities() (runtimeCapabilityResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return runtimeCapabilityResult{}, errors.New("torrent playback runtime is closed")
	}
	if len(r.sessions) != 0 {
		return runtimeCapabilityResult{}, errors.New("torrent capability probe is unavailable during playback")
	}

	probeRoot, err := os.MkdirTemp(r.root, "capability-probe-")
	if err != nil {
		return runtimeCapabilityResult{}, err
	}
	defer os.RemoveAll(probeRoot)

	return probeRuntimeCapabilities(probeRoot)
}

func probeRuntimeCapabilities(
	root string,
) (runtimeCapabilityResult, error) {
	result := runtimeCapabilityResult{}

	store, err := newBlockStore(
		filepath.Join(root, "store"),
		4*storeBlockSize,
	)
	if err != nil {
		return result, err
	}
	defer store.close()

	session := &runtimePlaybackSession{
		id:             "capability-probe",
		targetAssetKey: "shared:capability-probe",
		targetLength:   128,
		candidates:     make(map[string]*runtimeCandidate),
		store:          store,
		startedAt:      time.Now(),
	}
	session.metrics.candidates = make(map[string]*candidateMetrics)
	session.metrics.demandOffset = 32
	session.metrics.demandLength = 32

	storage := newSessionStorage(session, store)
	session.storage = storage
	store.onEvict = session.onStoreEviction
	store.onAttributionEvict = session.onAttributionEviction

	clientConfig := torrent.NewDefaultClientConfig()
	clientConfig.DefaultStorage = storage
	clientConfig.DataDir = filepath.Join(root, "client")
	clientConfig.NoDHT = true
	clientConfig.DisableTrackers = true
	clientConfig.DisableUTP = true
	clientConfig.DisableTCP = true
	clientConfig.DisableIPv6 = true
	clientConfig.DisablePEX = true
	clientConfig.DisableWebseeds = true
	clientConfig.DisableWebtorrent = true
	clientConfig.NoDefaultPortForwarding = true
	clientConfig.ListenPort = 0
	clientConfig.NoUpload = true

	client, err := torrent.NewClient(clientConfig)
	if err != nil {
		return result, err
	}
	defer client.Close()

	payload := []byte("OscarTV capability fixture: identical media bytes across independent torrent swarms; no tracker, peer, or network transfer is used.")
	if len(payload) < 128 {
		payload = append(payload, bytes.Repeat([]byte{0x5a}, 128-len(payload))...)
	}
	payload = payload[:128]

	candidateA, hashA, err := registerCapabilityProbeSwarm(
		session,
		client,
		"probe-a",
		"swarm-a",
		payload,
	)
	if err != nil {
		return result, nil
	}
	session.candidates[candidateA.config.CandidateID] = candidateA

	if err := store.setDemandRangesForGeneration(
		session.targetAssetKey,
		32,
		64,
		64,
		1,
	); err != nil {
		return result, nil
	}
	if err := candidateA.binding.setDemand(32, 32, 32, 1); err != nil {
		return result, nil
	}

	pieceData := payload[32:64]
	pieceA := candidateA.torrent.Piece(1)
	if n, writeErr := pieceA.Storage().WriteAt(pieceData, 0); writeErr != nil || n != len(pieceData) {
		return result, nil
	}
	if err := pieceA.VerifyData(); err != nil ||
		!candidateA.binding.isComplete(1) {
		return result, nil
	}
	session.metrics.mu.Lock()
	firstSwarmVerificationSuccess := session.metrics.candidates[candidateA.config.CandidateID].verificationSuccess
	session.metrics.mu.Unlock()
	if firstSwarmVerificationSuccess == 0 {
		return result, nil
	}
	result.Torrent = true

	candidateB, hashB, err := registerCapabilityProbeSwarm(
		session,
		client,
		"probe-b",
		"swarm-b",
		payload,
	)
	if err != nil {
		return result, nil
	}
	if hashA == hashB ||
		len(client.Torrents()) != 2 ||
		len(storage.bindings) != 2 {
		return result, nil
	}
	if err := candidateB.binding.setDemand(32, 32, 32, 1); err != nil {
		return result, nil
	}

	if candidateB.binding.isComplete(1) {
		return result, nil
	}
	sharedBytes := make([]byte, len(pieceData))
	if n, readErr := candidateB.binding.readRange(32, sharedBytes); readErr != nil ||
		n != len(sharedBytes) ||
		!bytes.Equal(sharedBytes, pieceData) {
		return result, nil
	}

	session.candidates[candidateB.config.CandidateID] = candidateB
	if !waitForCapabilityProbeCompletion(candidateB.binding, 1, 3*time.Second) ||
		!candidateA.binding.isComplete(1) {
		return result, nil
	}

	session.metrics.mu.Lock()
	verificationSuccess := session.metrics.candidates[candidateB.config.CandidateID].verificationSuccess
	session.metrics.mu.Unlock()
	if verificationSuccess == 0 {
		return result, nil
	}

	result.MultiSwarmTorrent = true
	return result, nil
}

func registerCapabilityProbeSwarm(
	session *runtimePlaybackSession,
	client *torrent.Client,
	candidateID string,
	swarmName string,
	payload []byte,
) (*runtimeCandidate, metainfo.Hash, error) {
	info := &metainfo.Info{
		Name:        swarmName,
		PieceLength: 32,
		Files: []metainfo.FileInfo{{
			Path:   []string{"media", "probe.mkv"},
			Length: int64(len(payload)),
		}},
	}
	if err := info.GeneratePieces(func(metainfo.FileInfo) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(payload)), nil
	}); err != nil {
		return nil, metainfo.Hash{}, err
	}

	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		return nil, metainfo.Hash{}, err
	}
	infoHash := metainfo.HashBytes(infoBytes)
	config := candidateConfig{
		CandidateID: candidateID,
		Target: targetConfig{
			Name:         "probe.mkv",
			RelativePath: "media/probe.mkv",
			LengthBytes:  strconv.FormatInt(int64(len(payload)), 10),
		},
	}

	binding, err := session.storage.register(infoHash, config)
	if err != nil {
		return nil, metainfo.Hash{}, err
	}

	spec := &torrent.TorrentSpec{}
	spec.InfoHash = infoHash
	spec.InfoBytes = infoBytes
	spec.DisplayName = swarmName
	torrentHandle, _, err := client.AddTorrentSpec(spec)
	if err != nil {
		session.storage.unregister(infoHash)
		return nil, metainfo.Hash{}, err
	}
	select {
	case <-torrentHandle.GotInfo():
	case <-time.After(3 * time.Second):
		session.storage.unregister(infoHash)
		return nil, metainfo.Hash{}, fmt.Errorf("capability probe swarm %s did not resolve metadata", candidateID)
	}

	binding.attachTorrent(torrentHandle)
	binding.mu.RLock()
	resolved := binding.info != nil &&
		binding.targetEnd-binding.targetStart == int64(len(payload))
	binding.mu.RUnlock()
	if !resolved {
		session.storage.unregister(infoHash)
		return nil, metainfo.Hash{}, fmt.Errorf("capability probe swarm %s did not register its target storage", candidateID)
	}

	candidate := &runtimeCandidate{
		config:  config,
		torrent: torrentHandle,
		binding: binding,
		ready:   make(chan struct{}),
	}
	session.metrics.candidates[candidateID] = &candidateMetrics{}
	return candidate, infoHash, nil
}

func waitForCapabilityProbeCompletion(
	binding *storageBinding,
	pieceIndex int,
	timeout time.Duration,
) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if binding.isComplete(pieceIndex) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return binding.isComplete(pieceIndex)
}
