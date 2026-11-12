package torrserverkit

import "testing"

func TestParseByteRange(t *testing.T) {
	tests := []struct {
		name        string
		header      string
		length      int64
		wantStart   int64
		wantEnd     int64
		wantPartial bool
		wantError   bool
	}{
		{name: "full response", length: 100, wantStart: 0, wantEnd: 99},
		{name: "head range", header: "bytes=0-15", length: 100, wantStart: 0, wantEnd: 15, wantPartial: true},
		{name: "tail range", header: "bytes=90-99", length: 100, wantStart: 90, wantEnd: 99, wantPartial: true},
		{name: "closed range", header: "bytes=10-19", length: 100, wantStart: 10, wantEnd: 19, wantPartial: true},
		{name: "open ended range", header: "bytes=90-", length: 100, wantStart: 90, wantEnd: 99, wantPartial: true},
		{name: "suffix range", header: "bytes=-10", length: 100, wantStart: 90, wantEnd: 99, wantPartial: true},
		{name: "suffix longer than file", header: "bytes=-200", length: 100, wantStart: 0, wantEnd: 99, wantPartial: true},
		{name: "range clipped to file", header: "bytes=90-120", length: 100, wantStart: 90, wantEnd: 99, wantPartial: true},
		{name: "start outside file", header: "bytes=100-", length: 100, wantError: true},
		{name: "reversed range", header: "bytes=20-10", length: 100, wantError: true},
		{name: "multiple ranges unsupported", header: "bytes=0-1,4-5", length: 100, wantError: true},
		{name: "invalid unit", header: "items=0-1", length: 100, wantError: true},
		{name: "zero length target", header: "bytes=0-1", length: 0, wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			start, end, partial, err := parseByteRange(test.header, test.length)
			if (err != nil) != test.wantError {
				t.Fatalf("error = %v, wantError=%v", err, test.wantError)
			}
			if err != nil {
				return
			}
			if start != test.wantStart || end != test.wantEnd || partial != test.wantPartial {
				t.Fatalf("range = [%d,%d], partial=%v; want [%d,%d], partial=%v",
					start, end, partial, test.wantStart, test.wantEnd, test.wantPartial)
			}
		})
	}
}
