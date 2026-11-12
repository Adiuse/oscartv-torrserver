package torrserverkit

import "testing"

func TestRuntimeCapabilityProbeUsesTwoRealSwarmRegistrations(t *testing.T) {
	capabilities, err := probeRuntimeCapabilities(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !capabilities.Torrent {
		t.Fatal("real single-swarm torrent registration was not verified")
	}
	if !capabilities.MultiSwarmTorrent {
		t.Fatal("distinct real swarms did not verify shared storage independently")
	}
}

func TestRuntimeCapabilityProbeRejectsActivePlayback(t *testing.T) {
	runtime := &playbackRuntime{
		root: t.TempDir(),
		sessions: map[string]*runtimePlaybackSession{
			"active": {},
		},
	}
	if _, err := runtime.verifyCapabilities(); err == nil {
		t.Fatal("capability probe was allowed while playback was active")
	}
}
