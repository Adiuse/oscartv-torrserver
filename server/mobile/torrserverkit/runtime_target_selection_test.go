// Path: server/mobile/torrserverkit/runtime_target_selection_test.go
package torrserverkit

import (
	"errors"
	"testing"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
)

func testIndexedMetainfo() *metainfo.Info {
	return &metainfo.Info{
		Name: "collection",
		PieceLength: 16384,
		Files: []metainfo.FileInfo{
			{Length: 100, Path: []string{"first.mkv"}},
			{Length: 200, Path: []string{"second.mp4"}},
			{Length: 200, Path: []string{"third.mkv"}},
		},
	}
}

func TestStorageSelectsExactIndexedTarget(t *testing.T) {
	index := 2
	binding := &storageBinding{
		target: targetConfig{
			Name: "third.mkv",
			RelativePath: "third.mkv",
			LengthBytes: "200",
			FileIndex: &index,
		},
	}
	if err := binding.resolveTarget(testIndexedMetainfo()); err != nil {
		t.Fatal(err)
	}
	if binding.targetStart != 300 || binding.targetEnd != 500 {
		t.Fatalf("wrong indexed range: %d..%d", binding.targetStart, binding.targetEnd)
	}
}

func TestStorageRejectsIndexMismatchWithoutGuessing(t *testing.T) {
	index := 1
	binding := &storageBinding{
		target: targetConfig{
			Name: "third.mkv",
			RelativePath: "third.mkv",
			LengthBytes: "200",
			FileIndex: &index,
		},
	}
	if err := binding.resolveTarget(testIndexedMetainfo()); err == nil {
		t.Fatal("index mismatch must not fall back to same-sized file")
	}
}

func TestStoragePreservesLegacyNameTarget(t *testing.T) {
	binding := &storageBinding{
		target: targetConfig{
			Name: "second.mp4",
			LengthBytes: "200",
		},
	}
	if err := binding.resolveTarget(testIndexedMetainfo()); err != nil {
		t.Fatal(err)
	}
	if binding.targetStart != 100 || binding.targetEnd != 300 {
		t.Fatalf("wrong legacy range: %d..%d", binding.targetStart, binding.targetEnd)
	}
}

func TestInvalidInspectionIdentity(t *testing.T) {
	if err := validateInspectionConfig(inspectionConfig{
		RequestID:"request",
		CandidateID:"candidate",
		InfoHashV1:"wrong",
	}); err == nil {
		t.Fatal("invalid infohash accepted")
	}
	if err := validateInspectionConfig(inspectionConfig{
		RequestID:"request",
		CandidateID:"candidate",
		InfoHashV1:"abcdef0123456789abcdef0123456789abcdef01",
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCandidateReadinessReportsPendingReadyAndFailure(t *testing.T) {
	pending := &runtimeCandidate{ready: make(chan struct{})}
	failed := &runtimeCandidate{
		ready: make(chan struct{}),
		readyErr: errors.New("untrusted target information"),
	}
	close(failed.ready)
	runtime := &playbackRuntime{
		sessions: map[string]*runtimePlaybackSession{
			"playback": {
				candidates: map[string]*runtimeCandidate{
					"pending": pending,
					"failed": failed,
				},
			},
		},
	}

	check := func(candidateID, expected string) {
		t.Helper()
		result, err := runtime.candidateReadiness("playback", candidateID)
		if err != nil || result != expected {
			t.Fatalf("candidate %s: got %q, error %v, expected %q", candidateID, result, err, expected)
		}
	}

	check("pending", `{"status":"PENDING"}`)
	check("failed", `{"status":"FAILED"}`)

	pending.mu.Lock()
	pending.file = &torrent.File{}
	pending.mu.Unlock()
	close(pending.ready)
	check("pending", `{"status":"READY"}`)

	if _, err := runtime.candidateReadiness("playback", "missing"); err == nil {
		t.Fatal("missing candidate must not look ready")
	}
}
