package torrserverkit

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/anacrolix/torrent/metainfo"
)

func TestBlockStorePrunesOutsideDemand(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 4*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	if err := store.setDemand("asset", 0, 3*storeBlockSize); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("a"), int(storeBlockSize))
	if _, err := store.write("asset", 0, payload, true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write("asset", storeBlockSize, payload, true); err != nil {
		t.Fatal(err)
	}
	if err := store.setDemand("asset", storeBlockSize, 2*storeBlockSize); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if err := store.read("asset", 0, got); err == nil {
		t.Fatal("bytes outside the active demand remained readable")
	}
	if err := store.read("asset", storeBlockSize, got); err != nil {
		t.Fatalf("retained demand range is not readable: %v", err)
	}
	if _, err := os.Stat(filepath.Join(store.root, hashAssetKey("asset"), "0000000000000000.blk")); !os.IsNotExist(err) {
		t.Fatalf("pruned block still exists, stat error=%v", err)
	}
}

func TestBlockStoreRejectsWritesOutsideDemand(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 2*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	if err := store.setDemand("asset", storeBlockSize, 2*storeBlockSize); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write("asset", 0, []byte("outside"), true); err == nil {
		t.Fatal("write outside demand was accepted")
	}
}

func TestBlockStoreConcurrentReadWriteAndPrune(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 8*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	if err := store.setDemand("asset", 0, 4*storeBlockSize); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			payload := bytes.Repeat([]byte{byte(worker + 1)}, 4096)
			for iteration := 0; iteration < 32; iteration++ {
				offset := int64(worker*4096 + iteration*16384)
				if _, err := store.write("asset", offset, payload, true); err != nil {
					return
				}
				buffer := make([]byte, len(payload))
				_ = store.read("asset", offset, buffer)
			}
		}(worker)
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		for iteration := 0; iteration < 16; iteration++ {
			start := int64(iteration%3) * storeBlockSize
			_ = store.setDemand("asset", start, start+2*storeBlockSize)
		}
	}()
	workers.Wait()
}

func TestRuntimeDemandAddsForwardWindowToRequestedRange(t *testing.T) {
	session := &runtimePlaybackSession{
		id: "demand-test",
		targetLength: 8 * storeBlockSize,
		targetAssetKey: "shared:test",
		candidates: make(map[string]*runtimeCandidate),
	}
	store, err := newBlockStore(t.TempDir(), 4*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	session.store = store
	session.forwardWindowBytes = storeBlockSize

	if err := session.setDemand(2*storeBlockSize, storeBlockSize); err != nil {
		t.Fatal(err)
	}
	demand, ok := store.demandRange(session.targetAssetKey)
	if !ok {
		t.Fatal("demand was not installed")
	}
	if demand.start != 2*storeBlockSize || demand.end != 4*storeBlockSize {
		t.Fatalf("unexpected additive demand: [%d,%d)", demand.start, demand.end)
	}
}

func TestBlockStoreWaitReadHonorsCancellationAndResumes(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if err := store.setDemand("asset", 0, 128); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.waitRead(ctx, "asset", 0, make([]byte, 16)); err == nil {
		t.Fatal("waitRead ignored canceled context")
	}

	payload := []byte("reconnected")
	if _, err := store.write("asset", 0, payload, true); err != nil {
		t.Fatal(err)
	}
	reconnectedContext, cancelReconnected := context.WithCancel(context.Background())
	defer cancelReconnected()
	buffer := make([]byte, len(payload))
	if err := store.waitRead(reconnectedContext, "asset", 0, buffer); err != nil {
		t.Fatalf("read did not resume after source reconnection: %v", err)
	}
	if !bytes.Equal(buffer, payload) {
		t.Fatalf("resumed read = %q, want %q", buffer, payload)
	}
}


func TestBlockStoreBudgetIncludesSharedAndPrivateAssets(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 2*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	if err := store.setDemand("shared:asset", 0, 2*storeBlockSize); err != nil {
		t.Fatal(err)
	}
	if err := store.setDemand("private:candidate", 0, 2*storeBlockSize); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("s"), int(storeBlockSize))
	if _, err := store.write("shared:asset", 0, payload, true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write("private:candidate", 0, payload, true); err != nil {
		t.Fatal(err)
	}
	if store.allocated != 2*storeBlockSize {
		t.Fatalf("shared and private allocations = %d, want %d", store.allocated, 2*storeBlockSize)
	}
	if err := store.setDemand("private:second-candidate", 0, storeBlockSize); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write("private:second-candidate", 0, payload, true); err == nil {
		t.Fatal("a second private candidate exceeded the shared/private budget")
	}
	if _, err := store.write("private:candidate", storeBlockSize, payload, true); err == nil {
		t.Fatal("combined shared/private budget was exceeded")
	}
	if store.allocated > store.maxBytes {
		t.Fatalf("allocated %d exceeds hard budget %d", store.allocated, store.maxBytes)
	}
}

func TestBlockStoreRetainsRecentVerifiedRangesAndEvictsOldestFirst(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 2*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	if err := store.setDemandForGeneration("shared:asset", 0, 2*storeBlockSize, 1); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("v"), int(storeBlockSize))
	for offset := int64(0); offset < 2*storeBlockSize; offset += storeBlockSize {
		if _, err := store.writeVerified("shared:asset", offset, payload, func(uint64, []byteInterval) {}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.setDemandForGeneration("shared:asset", 2*storeBlockSize, 3*storeBlockSize, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write("shared:asset", 2*storeBlockSize, payload, true); err != nil {
		t.Fatal(err)
	}

	buffer := make([]byte, len(payload))
	if err := store.read("shared:asset", 0, buffer); err == nil {
		t.Fatal("oldest recent verified range was not evicted")
	}
	if err := store.read("shared:asset", storeBlockSize, buffer); err != nil {
		t.Fatalf("newer recent verified range was evicted first: %v", err)
	}
	if err := store.read("shared:asset", 2*storeBlockSize, buffer); err != nil {
		t.Fatalf("current demand range is not readable: %v", err)
	}
}

func TestBlockStoreIgnoresStaleDemandGeneration(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 2*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	if err := store.setDemandForGeneration("asset", 0, storeBlockSize, 8); err != nil {
		t.Fatal(err)
	}
	if err := store.setDemandForGeneration("asset", storeBlockSize, 2*storeBlockSize, 7); err != nil {
		t.Fatal(err)
	}
	demand, ok := store.demandRange("asset")
	if !ok || demand.start != 0 || demand.end != storeBlockSize {
		t.Fatalf("stale demand replaced the current generation: %+v, present=%v", demand, ok)
	}
}

func TestStorageBindingRejectsFullTargetPieceEnvelope(t *testing.T) {
	info := &metainfo.Info{
		Name:        "target.mkv",
		Length:      64,
		PieceLength: 16,
	}
	binding := &storageBinding{
		info:        info,
		targetStart: 16,
		targetEnd:   32,
	}
	if _, err := binding.demandWindow(0, 1); !errors.Is(err, errFullMovieVerificationEnvelope) {
		t.Fatalf("full-target verification envelope was not rejected: %v", err)
	}

	binding.targetEnd = 48
	window, err := binding.demandWindow(0, 1)
	if err != nil {
		t.Fatalf("partial target envelope was rejected: %v", err)
	}
	if window.start != 16 || window.end != 32 {
		t.Fatalf("unexpected piece-aligned envelope: %+v", window)
	}
}

func TestStorageCompletionIgnoresStaleEvictionRevision(t *testing.T) {
	binding := &storageBinding{
		completion:           make(map[int]bool),
		completionGeneration: make(map[int]uint64),
	}
	binding.setCompletion(4, true, 12)
	binding.setCompletion(4, false, 11)
	if !binding.isComplete(4) {
		t.Fatal("stale eviction cleared newer verified completion")
	}
	binding.setCompletion(4, false, 13)
	if binding.isComplete(4) {
		t.Fatal("current eviction did not clear verified completion")
	}
}

func TestRuntimeOwnershipIgnoresStaleEvictionRevision(t *testing.T) {
	session := &runtimePlaybackSession{
		metrics: sessionMetrics{
			ownership: []ownedInterval{{
				start:       0,
				end:         100,
				candidateID: "candidate",
				generation:  12,
			}},
		},
	}
	session.removeOwnership(0, 100, 11)
	if len(session.metrics.ownership) != 1 {
		t.Fatal("stale eviction removed newer byte ownership")
	}
	session.removeOwnership(0, 100, 12)
	if len(session.metrics.ownership) != 0 {
		t.Fatal("current eviction did not remove byte ownership")
	}
}


func TestBlockStoreEvictsRecentBeforeForwardAndForwardBeforeCurrent(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 3*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	payload := bytes.Repeat([]byte("p"), int(storeBlockSize))

	if err := store.setDemandForGeneration("shared:recent", 2*storeBlockSize, 3*storeBlockSize, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.writeVerified("shared:recent", 2*storeBlockSize, payload, func(uint64, []byteInterval) {}); err != nil {
		t.Fatal(err)
	}
	if err := store.clearDemandForGeneration("shared:recent", 2); err != nil {
		t.Fatal(err)
	}

	if err := store.setDemandRangesForGeneration("private:active", 0, storeBlockSize, 3*storeBlockSize, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write("private:active", 0, payload, true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write("private:active", storeBlockSize, payload, true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write("private:active", 2*storeBlockSize, payload, true); err != nil {
		t.Fatal(err)
	}

	buffer := make([]byte, len(payload))
	if err := store.read("shared:recent", 2*storeBlockSize, buffer); err == nil {
		t.Fatal("recent verified data was retained ahead of the forward window")
	}
	if err := store.read("private:active", storeBlockSize, buffer); err != nil {
		t.Fatalf("forward data was evicted before recent data: %v", err)
	}
	if err := store.read("private:active", 0, buffer); err != nil {
		t.Fatalf("current range was evicted: %v", err)
	}

	if err := store.setDemandRangesForGeneration("private:active", 0, storeBlockSize, 4*storeBlockSize, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write("private:active", 3*storeBlockSize, payload, true); err != nil {
		t.Fatal(err)
	}
	if err := store.read("private:active", 0, buffer); err != nil {
		t.Fatalf("current range lost protection under forward pressure: %v", err)
	}
	if err := store.read("private:active", storeBlockSize, buffer); err == nil {
		t.Fatal("oldest forward range was not selected for eviction")
	}
	if err := store.read("private:active", 2*storeBlockSize, buffer); err != nil {
		t.Fatalf("newer forward range was evicted before the older one: %v", err)
	}
}

func TestBlockStoreRejectsBudgetAboveRuntimeLimit(t *testing.T) {
	if _, err := newBlockStore(t.TempDir(), sharedStoreMaximumBytes+1); err == nil {
		t.Fatal("block store accepted a budget above the 512 MiB runtime limit")
	}
}


func TestPieceAvailabilityMatchesPrivateOrSharedReadFallback(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 2*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	if err := store.setDemand("private:candidate", 0, 16); err != nil {
		t.Fatal(err)
	}
	if err := store.setDemand("shared:asset", 0, 16); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write("private:candidate", 0, []byte("private!"), true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write("shared:asset", 8, []byte("shared!!"), true); err != nil {
		t.Fatal(err)
	}

	binding := &storageBinding{
		store:           store,
		privateAssetKey: "private:candidate",
		sharedAssetKey:  "shared:asset",
		targetStart:     0,
		targetEnd:       16,
	}
	store.mu.Lock()
	available := binding.pieceRangeAvailableLocked(0, 16)
	store.mu.Unlock()
	if available {
		t.Fatal("piece availability combined partial private and shared ranges that the reader cannot combine")
	}

	if _, err := store.write("shared:asset", 0, []byte("shared!!"), true); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	available = binding.pieceRangeAvailableLocked(0, 16)
	store.mu.Unlock()
	if !available {
		t.Fatal("complete shared target range was not accepted as readable")
	}
}


func TestRuntimeOwnershipRemovesOnlyPhysicallyEvictedBytes(t *testing.T) {
	session := &runtimePlaybackSession{
		metrics: sessionMetrics{
			ownership: []ownedInterval{{
				start:       0,
				end:         100,
				candidateID: "candidate",
				generation:  4,
			}},
		},
	}
	session.removeOwnership(25, 75, 5)
	if len(session.metrics.ownership) != 2 {
		t.Fatalf("ownership fragments = %d, want 2", len(session.metrics.ownership))
	}
	if session.metrics.ownership[0].start != 0 ||
		session.metrics.ownership[0].end != 25 ||
		session.metrics.ownership[1].start != 75 ||
		session.metrics.ownership[1].end != 100 {
		t.Fatalf("eviction removed bytes outside its physical range: %+v", session.metrics.ownership)
	}
}


func TestBlockStorePartialPruneRetainsDemandAndAttributesExactBytes(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 2*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	asset := "private:partial-prune"
	if err := store.setDemand(asset, 0, 64); err != nil {
		t.Fatal(err)
	}
	if _, err := store.write(asset, 0, []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+-"), true); err != nil {
		t.Fatal(err)
	}

	var evicted []byteInterval
	store.onAttributionEvict = func(
		gotAsset string,
		start int64,
		end int64,
		_ uint64,
	) {
		if gotAsset != asset {
			t.Fatalf("eviction asset = %q, want %q", gotAsset, asset)
		}
		evicted = append(evicted, byteInterval{start: start, end: end})
	}

	if err := store.setDemand(asset, 32, 64); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 32)
	if err := store.read(asset, 0, buffer); err == nil {
		t.Fatal("pruned prefix remained readable")
	}
	if err := store.read(asset, 32, buffer); err != nil {
		t.Fatalf("retained suffix is not readable: %v", err)
	}
	if len(evicted) != 1 || evicted[0].start != 0 || evicted[0].end != 32 {
		t.Fatalf("evicted ranges = %+v, want exactly [0,32)", evicted)
	}
}

func TestBlockStorePruningFailureInvalidatesUnreadableBlocks(t *testing.T) {
	tests := []struct {
		name       string
		writeData  []byte
		newStart   int64
		newEnd     int64
		readOffset int64
		readLength int
	}{
		{
			name:       "full block removal fails",
			writeData:  []byte("verified"),
			newStart:   storeBlockSize,
			newEnd:     2 * storeBlockSize,
			readOffset: 0,
			readLength: 8,
		},
		{
			name:       "partial block rewrite and removal fail",
			writeData:  []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+-"),
			newStart:   32,
			newEnd:     64,
			readOffset: 32,
			readLength: 32,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := newBlockStore(t.TempDir(), 2*storeBlockSize)
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()

			asset := "private:prune-failure"
			if err := store.setDemand(asset, 0, storeBlockSize); err != nil {
				t.Fatal(err)
			}
			if _, err := store.write(asset, 0, test.writeData, true); err != nil {
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

			if err := store.setDemand(asset, test.newStart, test.newEnd); err == nil {
				t.Fatal("pruning failure was not returned")
			}

			store.mu.Lock()
			block := store.assets[asset][0]
			validCount := len(block.valid)
			allocated := store.allocated
			store.mu.Unlock()
			if validCount != 0 {
				t.Fatalf("failed-prune block retained %d readable intervals", validCount)
			}
			if allocated != storeBlockSize {
				t.Fatalf("failed-prune allocation = %d, want conservative %d", allocated, storeBlockSize)
			}
			if err := store.read(asset, test.readOffset, make([]byte, test.readLength)); err == nil {
				t.Fatal("bytes from a failed physical prune remained readable")
			}
		})
	}
}

func TestStorageBindingDemandWindowUsesMultiplePieceLengths(t *testing.T) {
	tests := []struct {
		name        string
		totalLength int64
		pieceLength int64
		targetStart int64
		targetEnd   int64
		offset      int64
		length      int64
		wantStart   int64
		wantEnd     int64
		wantFull    bool
	}{
		{
			name:        "single-piece target is rejected",
			totalLength: 64,
			pieceLength: 64,
			targetStart: 0,
			targetEnd:   64,
			offset:      1,
			length:      1,
			wantFull:    true,
		},
		{
			name:        "multiple large pieces remain window bounded",
			totalLength: 256,
			pieceLength: 64,
			targetStart: 64,
			targetEnd:   192,
			offset:      1,
			length:      1,
			wantStart:   64,
			wantEnd:     128,
		},
		{
			name:        "small piece alignment preserves partial target",
			totalLength: 64,
			pieceLength: 16,
			targetStart: 8,
			targetEnd:   40,
			offset:      0,
			length:      1,
			wantStart:   0,
			wantEnd:     16,
		},
		{
			name:        "multiple piece envelope covering target is rejected",
			totalLength: 128,
			pieceLength: 32,
			targetStart: 32,
			targetEnd:   96,
			offset:      31,
			length:      2,
			wantFull:    true,
		},
		{
			name:        "tail demand is clipped and piece aligned",
			totalLength: 256,
			pieceLength: 64,
			targetStart: 64,
			targetEnd:   192,
			offset:      127,
			length:      64,
			wantStart:   128,
			wantEnd:     192,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binding := &storageBinding{
				info: &metainfo.Info{
					Name:        "target.mkv",
					Length:      test.totalLength,
					PieceLength: test.pieceLength,
				},
				targetStart: test.targetStart,
				targetEnd:   test.targetEnd,
			}
			window, err := binding.demandWindow(test.offset, test.length)
			if test.wantFull {
				if !errors.Is(err, errFullMovieVerificationEnvelope) {
					t.Fatalf("full-file envelope error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if window.start != test.wantStart || window.end != test.wantEnd {
				t.Fatalf("window = [%d,%d), want [%d,%d)",
					window.start, window.end, test.wantStart, test.wantEnd)
			}
		})
	}
}

func TestPieceCompletionStateIsIndependentPerCandidateBinding(t *testing.T) {
	first := &storageBinding{
		completion:           make(map[int]bool),
		completionGeneration: make(map[int]uint64),
	}
	second := &storageBinding{
		completion:           make(map[int]bool),
		completionGeneration: make(map[int]uint64),
	}
	first.setCompletion(2, true, 1)
	if !first.isComplete(2) {
		t.Fatal("first candidate piece was not marked complete")
	}
	if second.isComplete(2) {
		t.Fatal("first candidate verification leaked into the second candidate")
	}
}

func TestClearDemandRetainsExactlyTwoMostRecentSharedRanges(t *testing.T) {
	store, err := newBlockStore(t.TempDir(), 4*storeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()

	asset := "shared:recent-clear"
	payload := bytes.Repeat([]byte("r"), int(storeBlockSize))
	for index := int64(0); index < 3; index++ {
		start := index * storeBlockSize
		end := start + storeBlockSize
		generation := uint64(index + 1)
		if err := store.setDemandForGeneration(asset, start, end, generation); err != nil {
			t.Fatal(err)
		}
		if _, err := store.writeVerified(asset, start, payload, func(uint64, []byteInterval) {}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.clearDemandForGeneration(asset, 4); err != nil {
		t.Fatal(err)
	}

	buffer := make([]byte, storeBlockSize)
	if err := store.read(asset, 0, buffer); err == nil {
		t.Fatal("oldest shared range was retained after clearing demand")
	}
	for _, index := range []int64{1, 2} {
		if err := store.read(asset, index*storeBlockSize, buffer); err != nil {
			t.Fatalf("recent shared range %d was not retained: %v", index, err)
		}
	}
}

