package jobperf

import (
	"testing"
)

func TestParseBytes(t *testing.T) {

	for _, tc := range []struct {
		in        string
		out       Bytes
		shouldErr bool
	}{
		{in: "0", out: Bytes(0), shouldErr: false},
		{in: "5", out: Bytes(5), shouldErr: false},
		{in: "5b", out: Bytes(5), shouldErr: false},
		{in: "5kb", out: Bytes(5120), shouldErr: false},
		{in: "5 kb", out: Bytes(5120), shouldErr: false},
		{in: "5 kb ", out: Bytes(5120), shouldErr: false},
		{in: " 5 kb ", out: Bytes(5120), shouldErr: false},
		{in: "5kib", out: Bytes(5120), shouldErr: false},
		{in: "5KB", out: Bytes(5120), shouldErr: false},
		{in: "5kB", out: Bytes(5120), shouldErr: false},
		{in: "5mb", out: Bytes(5242880), shouldErr: false},
		{in: "5mib", out: Bytes(5242880), shouldErr: false},
		{in: "5gb", out: Bytes(5368709120), shouldErr: false},
		{in: "5gib", out: Bytes(5368709120), shouldErr: false},
		{in: "5tb", out: Bytes(5 * 1024 * 1024 * 1024 * 1024), shouldErr: false},
		{in: "5yb", shouldErr: true},
		{in: "5yb", shouldErr: true},
		{in: "-5b", shouldErr: true},
		{in: "9223372036854775807b", out: Bytes(9223372036854775807), shouldErr: false},
		{in: "9223372036854775808b", shouldErr: true}, // overflows (signed) 64bits
		{in: "9007199254740991kb", out: Bytes(9223372036854774784), shouldErr: false},
		{in: "9007199254740992kb", shouldErr: true},                          // overflows (signed) 64bits
		{in: "8388607tb", out: Bytes(9223370937343148032), shouldErr: false}, // overflows (signed) 64bits
		{in: "8388608tb", shouldErr: true},                                   // overflows (signed) 64bits
		{in: "50000000tb", shouldErr: true},                                  // overflows (signed) 64bits
	} {
		out, err := ParseBytes(tc.in)
		if tc.shouldErr {
			if err == nil {
				t.Errorf("input %v should produce error but did not", tc.in)
			}
		} else {
			if err != nil {
				t.Errorf("input %v should not produce error but it did: %v", tc.in, err)
			} else if out != tc.out {
				t.Errorf("input %v should produce %v but it produced %v", tc.in, tc.out, out)
			}
		}
	}
}

func TestBytesString(t *testing.T) {

	for _, tc := range []struct {
		in  string
		out string
	}{
		{in: "1", out: "1 B"},
		{in: "2kb", out: "2.00 KiB"},
		{in: "12345kb", out: "12.06 MiB"},
	} {
		parsed, err := ParseBytes(tc.in)
		if err != nil {
			t.Errorf("got err on input %v: %v", tc.in, err)
			continue
		}
		out := parsed.String()
		if out != tc.out {
			t.Errorf("on input %v expected output %v but got %v", tc.in, tc.out, out)
		}
	}
}
