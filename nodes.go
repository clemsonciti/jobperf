package jobperf

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type Bytes int64

// ParseBytes will parse a string into bytes. It assumes any units are in IEC (i.e. powers of 2).
func ParseBytes(in string) (Bytes, error) {
	s := strings.TrimSpace(in)
	firstNonDigit := strings.IndexFunc(s, func(r rune) bool {
		return !unicode.IsDigit(r)
	})

	if firstNonDigit == -1 {
		firstNonDigit = len(s)
	}
	value, err := strconv.Atoi(s[:firstNonDigit])
	if err != nil {
		return 0, fmt.Errorf("failed to parse string %s: %w", in, err)
	}
	var unit string
	if firstNonDigit == len(s) {
		unit = "b"
	} else {
		unit = strings.ToLower(strings.TrimSpace(s[firstNonDigit:]))
	}
	unitTable := map[string]int{
		"b":   1,
		"kb":  1 << 10,
		"kib": 1 << 10,
		"mb":  1 << 20,
		"mib": 1 << 20,
		"gb":  1 << 30,
		"gib": 1 << 30,
		"tb":  1 << 40,
		"tib": 1 << 40,
	}
	mult, found := unitTable[unit]
	if !found {
		return 0, fmt.Errorf("failed to parse bytes string %s: unknown unit %v", in, unit)
	}
	scaledVal := value * mult
	if value != 0 && scaledVal/value != mult {
		return 0, fmt.Errorf("failed to parse bytes string %s: does not fit in int64", in)
	}
	return Bytes(scaledVal), nil
}

func (b Bytes) String() string {
	if b < (1 << 10) {
		return fmt.Sprintf("%d B", b)
	}
	if b < (1 << 20) {
		return fmt.Sprintf("%.2f KiB", float64(b)/1024.0)
	}
	if b < (1 << 30) {
		return fmt.Sprintf("%.2f MiB", float64(b)/1024.0/1024.0)
	}
	if b < (1 << 40) {
		return fmt.Sprintf("%.2f GiB", float64(b)/1024.0/1024.0/1024.0)
	}
	return fmt.Sprintf("%.2f TiB", float64(b)/1024.0/1024.0/1024.0/1024.0)
}

type Node struct {
	Hostname string
	NCores   int
	Memory   Bytes
	NGPUs    int
}

type GPUStat struct {
	ProductName   string
	ComputeUsage  int // In percent
	ID            string
	MemUsageBytes Bytes
	MemTotalBytes Bytes
	SampleTime    time.Time
	Hostname      string
}
