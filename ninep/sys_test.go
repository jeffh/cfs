package ninep

import (
	"os"
	"testing"
	"time"
)

func TestStat(t *testing.T) {
	f, err := os.CreateTemp("", "")
	threshold := time.Now().Add(-time.Second)
	if err != nil {
		t.Error(err.Error())
	}
	defer func() { _ = os.Remove(f.Name()) }()
	defer func() { _ = f.Close() }()

	info, err := os.Stat(f.Name())
	if err != nil {
		t.Error(err.Error())
	}

	at, ok := Atime(info)
	if !ok {
		t.Errorf("Failed to read access time")
	}

	if at.Before(threshold) {
		t.Errorf("expected access time to be recent: got %v, but expected to be after %v", at, threshold)
	}
}
