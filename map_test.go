package bucket_test

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/niaow/bucket"
)

func TestMapEncodeSimple(t *testing.T) {
	var buf bytes.Buffer
	n, err := bucket.Map{"hello world": "xyzzy"}.WriteTo(&buf)
	if err != nil {
		t.Errorf("failed to encode: %s", err.Error())
		return
	}
	expect := "\x2C\x14hello worldxyzzy\x00"
	if n != int64(len(expect)) {
		t.Errorf("expected %d bytes written but reported %d (actually wrote %d)", len(expect), n, buf.Len())
	}
	got := buf.String()
	if got != expect {
		t.Errorf("expected encoding %q but got %q", expect, got)
	}
}

func TestMapDecodeSimple(t *testing.T) {
	m := make(bucket.Map)
	raw := "\x2C\x14hello worldxyzzy\x00"
	n, err := m.ReadFrom(strings.NewReader(raw))
	if err != nil {
		t.Errorf("failed to decode: %s", err.Error())
		return
	}
	if n != int64(len(raw)) {
		t.Errorf("expected %d bytes read but reported %d", len(raw), n)
	}
	expect := bucket.Map{"hello world": "xyzzy"}
	if !reflect.DeepEqual(m, expect) {
		t.Errorf("expected %v but got %v", expect, m)
	}
}
