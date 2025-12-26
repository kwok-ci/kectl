package e2e

import (
	"os"
	"os/exec"
	"testing"
)

func TestRecordAndReplay(t *testing.T) {
	// Record a snapshot
	recordCmd := exec.Command(kectl,
		"--endpoints="+endpoint,
		"snapshot",
		"record",
		"--path=./snapshot.yaml",
		"--snapshot=true",
	)
	recordCmd.Stderr = os.Stderr
	err := recordCmd.Run()
	if err != nil {
		t.Fatalf("Failed to record snapshot: %v", err)
	}

	// Replay the recorded snapshot
	replayCmd := exec.Command(kectl,
		"--endpoints="+endpoint,
		"snapshot",
		"replay",
		"--path=./snapshot.yaml",
		"--snapshot=true",
	)
	replayCmd.Stderr = os.Stderr
	err = replayCmd.Run()
	if err != nil {
		t.Fatalf("Failed to replay snapshot: %v", err)
	}
}
