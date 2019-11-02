package ninep

import "testing"

func TestEncodesTRead(t *testing.T) {
	m := make([]byte, 2048)
	Tread(m).fill(1, 2, 3, 4)
	msg := Tread(Tread(m).Bytes())
	if msg.Tag() != 1 {
		t.Fatalf("expected tag to match: %d != %d", msg.Tag(), 1)
	}
	if msg.Fid() != 2 {
		t.Fatalf("expected fid to match: %d != %d", msg.Fid(), 2)
	}
	if msg.Offset() != 3 {
		t.Fatalf("expected offset to match: %d != %d", msg.Fid(), 3)
	}
	if msg.Count() != 4 {
		t.Fatalf("expected offset to match: %d != %d", msg.Fid(), 4)
	}
}
