package util

import (
	"fmt"
	"net"
	"strings"
	"time"
)

const loopbackIP = "127.0.0.1"

// SelectIntOpt takes an option and a default value and returns the default value if
// the option is equal to zero, and the option otherwise.
func SelectIntOpt(opt, def int) int {
	if opt == 0 {
		return def
	}
	return opt
}

// SelectDurationOpt takes an option and a default value and returns the default value if
// the option is equal to zero, and the option otherwise.
func SelectDurationOpt(opt, def time.Duration) time.Duration {
	if opt == time.Duration(0) {
		return def
	}
	return opt
}

// GetLocalIP returns the local IP address.
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return loopbackIP
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}

	return loopbackIP
}

// SafeSplit splits the give string by space and handles quotation marks
func SafeSplit(s string) []string {
	split := strings.Split(s, " ")

	var result []string
	var inquote string
	var block string
	for _, i := range split {
		if inquote == "" {
			if strings.HasPrefix(i, "'") || strings.HasPrefix(i, "\"") {
				inquote = string(i[0])
				block = strings.TrimPrefix(i, inquote) + " "
			} else {
				result = append(result, i)
			}
		} else {
			if !strings.HasSuffix(i, inquote) {
				block += i + " "
			} else {
				block += strings.TrimSuffix(i, inquote)
				inquote = ""
				result = append(result, block)
				block = ""
			}
		}
	}

	return result
}

// ClockEntry represents a single entry in the vector clock.
type ClockEntry struct {
	NodeID  string    // Unique identifier for the node
	Counter int       // Version counter
	Updated time.Time // Last update time
}

// VectorClock represents a distributed versioning structure.
type VectorClock struct {
	Entries map[string]*ClockEntry
}

// NewVectorClock initializes a new VectorClock.
func NewVectorClock() *VectorClock {
	return &VectorClock{
		Entries: make(map[string]*ClockEntry),
	}
}

// Update increments the counter for the given node or creates a new entry.
func (vc *VectorClock) Update(nodeID string) {
	if entry, exists := vc.Entries[nodeID]; exists {
		entry.Counter++
		entry.Updated = time.Now()
	} else {
		vc.Entries[nodeID] = &ClockEntry{
			NodeID:  nodeID,
			Counter: 1,
			Updated: time.Now(),
		}
	}
}

// Compare checks the relationship between two vector clocks.
func (vc *VectorClock) Compare(other *VectorClock) string {
	isNewer, isOlder := false, false

	for nodeID, otherEntry := range other.Entries {
		if thisEntry, exists := vc.Entries[nodeID]; exists {
			if thisEntry.Counter < otherEntry.Counter {
				isOlder = true
			} else if thisEntry.Counter > otherEntry.Counter {
				isNewer = true
			}
		} else {
			isOlder = true
		}
	}

	for nodeID := range vc.Entries {
		if _, exists := other.Entries[nodeID]; !exists {
			isNewer = true
		}
	}

	if isNewer && isOlder {
		return "CONCURRENT"
	} else if isNewer {
		return "NEWER"
	} else if isOlder {
		return "OLDER"
	}
	return "EQUAL"
}

// String converts the vector clock to a human-readable string.
func (vc *VectorClock) String() string {
	result := ""
	for nodeID, entry := range vc.Entries {
		result += fmt.Sprintf("%s:%d ", nodeID, entry.Counter)
	}
	return result
}
