package main

import (
	"testing"
	"time"
)

func TestMarkOfflineAfterThreshold(t *testing.T) {
	h := newHubWithTargets([]string{"ws://device/ws"})
	now := time.Now().UTC()
	h.status["ws://device/ws"] = DeviceStatus{
		DeviceID: "device-1",
		Target:   "ws://device/ws",
		State:    "online",
		Online:   true,
		LastSeen: now.Add(-offlineAfter),
	}

	h.markOffline(now)
	if got := h.status["ws://device/ws"].State; got != "online" {
		t.Fatalf("state at threshold = %q, want online", got)
	}

	h.markOffline(now.Add(time.Nanosecond))
	status := h.status["ws://device/ws"]
	if status.State != "offline" || status.Online {
		t.Fatalf("state after threshold = %#v, want offline", status)
	}
}

func TestNeverConnectedTargetBecomesOffline(t *testing.T) {
	h := newHubWithTargets([]string{"ws://device/ws"})
	started := h.status["ws://device/ws"].Since

	h.markOffline(started.Add(offlineAfter))
	if got := h.status["ws://device/ws"].State; got != "connecting" {
		t.Fatalf("state at threshold = %q, want connecting", got)
	}

	h.markOffline(started.Add(offlineAfter + time.Nanosecond))
	status := h.status["ws://device/ws"]
	if status.State != "offline" || status.Online {
		t.Fatalf("state after threshold = %#v, want offline", status)
	}
}
