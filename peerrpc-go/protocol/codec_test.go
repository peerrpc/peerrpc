package protocol

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestWriteReadFrame_RoundTrip(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		{0x00},
		{0xff, 0x01, 0x02, 0x03},
		bytes.Repeat([]byte{0xab}, 64),
		bytes.Repeat([]byte{0xcd}, 4096),
	}
	for i, payload := range cases {
		payload := payload
		t.Run(strings.ReplaceAll(t.Name()+"#"+itoa(i), " ", "_"), func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteFrame(&buf, payload); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}
			got, err := ReadFrame(&buf)
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if len(got) != len(payload) {
				t.Fatalf("size mismatch: got=%d want=%d", len(got), len(payload))
			}
			for i := range got {
				if got[i] != payload[i] {
					t.Fatalf("byte %d: got=%#x want=%#x", i, got[i], payload[i])
				}
			}
		})
	}
}

func TestWriteFrame_TwoFramesBackToBack(t *testing.T) {
	var buf bytes.Buffer
	a := []byte("first-frame")
	b := []byte("second-frame-longer-than-first")
	if err := WriteFrame(&buf, a); err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(&buf, b); err != nil {
		t.Fatal(err)
	}
	got1, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("read1: %v", err)
	}
	got2, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("read2: %v", err)
	}
	if string(got1) != string(a) {
		t.Errorf("frame1: got %q want %q", got1, a)
	}
	if string(got2) != string(b) {
		t.Errorf("frame2: got %q want %q", got2, b)
	}
}

func TestReadFrame_Oversized(t *testing.T) {
	// Announce MaxFrameSize+1.
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x04, 0x00, 0x01}) // 0x00040001 = 262145
	_, err := ReadFrame(&buf)
	if !errors.Is(err, ErrOversizedFrame) {
		t.Fatalf("got err=%v, want ErrOversizedFrame", err)
	}
}

func TestReadFrame_EOF(t *testing.T) {
	var buf bytes.Buffer // empty
	_, err := ReadFrame(&buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("got err=%v, want io.EOF", err)
	}
}

func TestReadFrame_Truncated(t *testing.T) {
	// Announce 16 bytes but only supply 4.
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x00, 0x00, 0x10, 'a', 'b', 'c', 'd'})
	_, err := ReadFrame(&buf)
	if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		t.Fatalf("got err=%v, want EOF-family", err)
	}
}

// itoa avoids importing strconv just for one diagnostic string.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
