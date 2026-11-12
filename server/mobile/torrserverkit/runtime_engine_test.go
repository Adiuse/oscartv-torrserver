package torrserverkit

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/anacrolix/torrent/metainfo"
)

func TestRuntimeDemandGenerationAdvancesOnSeek(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 4*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	session := &runtimePlaybackSession{
		targetLength:   4 * storeBlockSize,
		targetAssetKey: "shared:seek",
		candidates:     make(map[string]*runtimeCandidate),
		store:          store,
	}

	if err := session.setDemand(0, 128); err != nil {
		t.Fatal(err)
	}
	if err := session.setDemand(128, 128); err != nil {
		t.Fatal(err)
	}
	if session.demandEpoch != 0 {
		t.Fatalf("sequential demand advanced generation to %d", session.demandEpoch)
	}
	if err := session.setDemand(2*storeBlockSize, 128); err != nil {
		t.Fatal(err)
	}
	if session.demandEpoch != 1 {
		t.Fatalf("seek generation = %d, want 1", session.demandEpoch)
	}
	if store.demandGeneration[session.targetAssetKey] != 1 {
		t.Fatalf("storage generation = %d, want 1", store.demandGeneration[session.targetAssetKey])
	}
	demand, ok := store.demandRange(session.targetAssetKey)
	if !ok || demand.start != 2*storeBlockSize || demand.end != 2*storeBlockSize+128 {
		t.Fatalf("forward seek demand = %+v, present=%v", demand, ok)
	}

	if err := session.setDemand(0, 128); err != nil {
		t.Fatal(err)
	}
	if session.demandEpoch != 2 {
		t.Fatalf("backward seek generation = %d, want 2", session.demandEpoch)
	}
	demand, ok = store.demandRange(session.targetAssetKey)
	if !ok || demand.start != 0 || demand.end != 128 {
		t.Fatalf("backward seek retained the gap: %+v, present=%v", demand, ok)
	}
}

func TestRuntimeDemandClipsAdditiveWindowAtTargetEnd(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 4*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	session := &runtimePlaybackSession{
		targetLength:       3 * storeBlockSize,
		targetAssetKey:     "shared:clip",
		candidates:         make(map[string]*runtimeCandidate),
		store:              store,
		forwardWindowBytes: storeBlockSize,
	}
	if err := session.setDemand(2*storeBlockSize, storeBlockSize); err != nil {
		t.Fatal(err)
	}
	demand, ok := store.demandRange(session.targetAssetKey)
	if !ok {
		t.Fatal("demand was not installed")
	}
	if demand.start != 2*storeBlockSize || demand.end != 3*storeBlockSize {
		t.Fatalf("demand = [%d,%d), want [%d,%d)",
			demand.start, demand.end, 2*storeBlockSize, 3*storeBlockSize)
	}
	current := store.currentDemand[session.targetAssetKey]
	if current.start != 2*storeBlockSize || current.end != 3*storeBlockSize {
		t.Fatalf("current range = %+v", current)
	}
	if _, ok := store.forwardDemand[session.targetAssetKey]; ok {
		t.Fatal("forward range extended beyond the target end")
	}
}


func TestRuntimeForwardWindowUpdateExtendsExistingDemand(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 4*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	session := &runtimePlaybackSession{
		targetLength:   4 * storeBlockSize,
		targetAssetKey: "shared:forward-update",
		candidates:     make(map[string]*runtimeCandidate),
		store:          store,
	}
	if err := session.setDemand(0, storeBlockSize); err != nil {
		t.Fatal(err)
	}
	if err := session.setForwardWindow(storeBlockSize); err != nil {
		t.Fatal(err)
	}
	demand, ok := store.demandRange(session.targetAssetKey)
	if !ok {
		t.Fatal("updated demand was not installed")
	}
	if demand.start != 0 || demand.end != 2*storeBlockSize {
		t.Fatalf("updated additive demand = [%d,%d), want [0,%d)",
			demand.start, demand.end, 2*storeBlockSize)
	}
	current := store.currentDemand[session.targetAssetKey]
	forward := store.forwardDemand[session.targetAssetKey]
	if current.start != 0 || current.end != storeBlockSize {
		t.Fatalf("current range after forward update = %+v", current)
	}
	if forward.start != storeBlockSize || forward.end != 2*storeBlockSize {
		t.Fatalf("forward range after update = %+v", forward)
	}
}


func TestRuntimeDemandRejectsLatePriorHttpRangeAfterSeek(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 4*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	session := &runtimePlaybackSession{
		targetLength:   4 * storeBlockSize,
		targetAssetKey: "shared:stale-range",
		candidates:     make(map[string]*runtimeCandidate),
		store:          store,
	}
	oldGeneration := session.beginDemandRequest(0, func() {})
	if err := session.setDemandForGeneration(0, 128, oldGeneration); err != nil {
		t.Fatal(err)
	}
	middleGeneration := session.beginDemandRequest(storeBlockSize, func() {})
	if err := session.setDemandForGeneration(storeBlockSize, 128, middleGeneration); err != nil {
		t.Fatal(err)
	}
	newGeneration := session.beginDemandRequest(2 * storeBlockSize, func() {})
	if err := session.setDemandForGeneration(2*storeBlockSize, 128, newGeneration); err != nil {
		t.Fatal(err)
	}
	if err := session.setDemandForGeneration(128, 128, oldGeneration); err == nil {
		t.Fatal("late HTTP range from the first generation was accepted")
	}
	if err := session.setDemandForGeneration(storeBlockSize, 128, middleGeneration); err == nil {
		t.Fatal("late HTTP range from the intermediate generation was accepted")
	}
	demand, ok := store.demandRange(session.targetAssetKey)
	if !ok || demand.start != 2*storeBlockSize {
		t.Fatalf("rapid stale ranges replaced the latest seek demand: %+v, present=%v", demand, ok)
	}
}


func TestBeginDemandRequestCancelsSupersededRange(t *testing.T) {
	session := &runtimePlaybackSession{}
	oldCanceled := false
	newCanceled := false
	oldGeneration := session.beginDemandRequest(0, func() {
		oldCanceled = true
	})
	newGeneration := session.beginDemandRequest(1024, func() {
		newCanceled = true
	})
	if newGeneration <= oldGeneration {
		t.Fatalf("new range generation %d did not advance past %d", newGeneration, oldGeneration)
	}
	if !oldCanceled {
		t.Fatal("new HTTP range did not cancel the superseded reader")
	}
	session.endDemandRequest(oldGeneration)

	session.mu.RLock()
	currentCancel := session.demandRequestCancel
	session.mu.RUnlock()
	if currentCancel == nil {
		t.Fatal("finishing the old request cleared the new request cancellation")
	}
	currentCancel()
	if !newCanceled {
		t.Fatal("current request cancellation was not retained")
	}
}


func TestFullMovieEnvelopeRejectionDeactivatesCandidate(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 2*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if err := store.setDemandForGeneration("private:candidate", 0, 128, 3); err != nil {
		t.Fatal(err)
	}
	candidate := &runtimeCandidate{
		desiredActive: true,
		binding: &storageBinding{
			store:           store,
			privateAssetKey: "private:candidate",
		},
	}
	candidate.rejectFullMovieDemand(3)
	candidate.mu.Lock()
	active := candidate.desiredActive
	candidate.mu.Unlock()
	if active {
		t.Fatal("candidate remained active after full-target envelope rejection")
	}
	if _, ok := store.demandRange("private:candidate"); ok {
		t.Fatal("rejected candidate retained a private storage demand")
	}
}

func TestPieceCompletionIsNotCommittedAfterPhysicalEviction(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	info := &metainfo.Info{
		Name:        "target.mkv",
		Length:      16,
		PieceLength: 16,
		Pieces:      make([]byte, 20),
	}
	session := &runtimePlaybackSession{
		metrics: sessionMetrics{
			candidates: map[string]*candidateMetrics{
				"candidate": {},
			},
		},
	}
	binding := &storageBinding{
		session:         session,
		store:           store,
		candidateID:     "candidate",
		privateAssetKey: "private:candidate",
		sharedAssetKey:  "shared:asset",
		info:            info,
		targetStart:     0,
		targetEnd:       16,
		completion:      make(map[int]bool),
		completionGeneration: make(map[int]uint64),
	}
	piece := &runtimePiece{
		meta:    info.Piece(0),
		binding: binding,
	}
	if err := piece.MarkComplete(); err == nil {
		t.Fatal("piece completion succeeded after its bytes were physically evicted")
	}
	if binding.isComplete(0) {
		t.Fatal("evicted piece was marked complete")
	}
	if session.metrics.candidates["candidate"].verificationFailure != 1 {
		t.Fatal("failed completion was not recorded as a verification failure")
	}
}


func TestRuntimeHttpChunksKeepOneDemandGeneration(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 4*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	session := &runtimePlaybackSession{
		targetLength:   4 * storeBlockSize,
		targetAssetKey: "shared:http-chunks",
		candidates:     make(map[string]*runtimeCandidate),
		store:          store,
	}
	generation := session.beginDemandRequest(0, func() {})
	if err := session.setDemandForGeneration(0, 256, generation); err != nil {
		t.Fatal(err)
	}
	if err := session.setDemandForGeneration(256, 256, generation); err != nil {
		t.Fatal(err)
	}
	if session.currentDemandGeneration() != generation {
		t.Fatal("contiguous HTTP chunks advanced the demand generation")
	}
	if store.demandGeneration[session.targetAssetKey] != generation {
		t.Fatal("contiguous HTTP chunks changed the storage generation")
	}
}


func TestSessionSeekStopsCandidateReadersBeforeSharedPruneFailure(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 2*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	asset := "private:seek-prune-failure"
	if err := store.setDemand(asset, 0, 64); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write(asset, 0, []byte("verified"), true); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	blockPath := store.assets[asset][0].path
	store.mu.Unlock()
	if err := os.Remove(blockPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(blockPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blockPath, "keep"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	readContext, cancelRead := context.WithCancel(context.Background())
	defer cancelRead()
	candidate := &runtimeCandidate{
		desiredActive: true,
		readCancel:    cancelRead,
	}
	session := &runtimePlaybackSession{
		targetLength:          storeBlockSize,
		targetAssetKey:        asset,
		requestedDemandOffset: 0,
		requestedDemandLength: 64,
		demandLength:          64,
		hasDemand:             true,
		candidates: map[string]*runtimeCandidate{
			"candidate": candidate,
		},
		store: store,
	}
	if err := session.setDemand(32, 32); err == nil {
		t.Fatal("shared pruning failure was not returned")
	}
	if readContext.Err() == nil {
		t.Fatal("candidate reader remained active after shared pruning failed")
	}
}

func TestCandidateUpdateStopsReaderBeforePrivatePruneFailure(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 2*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	asset := "private:candidate-prune-failure"
	if err := store.setDemand(asset, 0, 64); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write(asset, 0, []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+-"), true); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	blockPath := store.assets[asset][0].path
	store.mu.Unlock()
	if err := os.Remove(blockPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(blockPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blockPath, "keep"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	readContext, cancelRead := context.WithCancel(context.Background())
	defer cancelRead()
	candidate := &runtimeCandidate{
		desiredActive: true,
		readCancel:    cancelRead,
		binding: &storageBinding{
			store:           store,
			privateAssetKey: asset,
			info: &metainfo.Info{
				Name:        "target.mkv",
				Length:      128,
				PieceLength: 16,
			},
			targetStart: 0,
			targetEnd:   128,
		},
	}
	if err := candidate.updateDemand(32, 32, 32, 2); err == nil {
		t.Fatal("private pruning failure was not returned")
	}
	if readContext.Err() == nil {
		t.Fatal("candidate reader remained active after private pruning failed")
	}
}
