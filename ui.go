package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sort"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

const (
	ansiReset   = "\033[0m"
	ansiBold    = "\033[1m"
	ansiRed     = "\033[31m"
	ansiYellow  = "\033[33m"
	ansiGreen   = "\033[32m"
	ansiCyan    = "\033[36m"
	clearScreen = "\033[2J\033[H"
)

func fmtBytes(b uint64) string {
	const (
		kb = 1 << 10
		mb = 1 << 20
		gb = 1 << 30
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.1f GB", float64(b)/gb)
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/mb)
	case b >= kb:
		return fmt.Sprintf("%.1f KB", float64(b)/kb)
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func fmtRate(b uint64) string {
	return fmtBytes(b) + "/s"
}

func cpuColor(pct float64) string {
	switch {
	case pct >= 80:
		return ansiRed
	case pct >= 50:
		return ansiYellow
	default:
		return ansiGreen
	}
}

func fmtAge(t time.Time) string {
	d := time.Since(t).Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
}

func clampPct(p float64) float64 {
	switch {
	case p < 0:
		return 0
	case p > 100:
		return 100
	default:
		return p
	}
}

func shortID(id string) string {
	if len(id) <= 16 {
		return id
	}
	return id[:8] + "..." + id[len(id)-8:]
}

func truncate(s string, max int) string {
	if max <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	if max <= 3 {
		return string(r[:max])
	}
	return string(r[:max-3]) + "..."
}

func padRight(s string, n int) string {
	r := []rune(s)
	if len(r) >= n {
		return string(r[:n])
	}
	return s + strings.Repeat(" ", n-len(r))
}

func fitTwo(left, right string, width int) string {
	lr := []rune(left)
	rr := []rune(right)
	if len(lr)+1+len(rr) <= width {
		return left + strings.Repeat(" ", width-len(lr)-len(rr)) + right
	}
	if width <= len(rr)+1 {
		return truncate(right, width)
	}
	maxLeft := width - len(rr) - 1
	left = truncate(left, maxLeft)
	lr = []rune(left)
	return left + strings.Repeat(" ", width-len(lr)-len(rr)) + right
}

func progressBar(pct float64, width int) string {
	if width < 8 {
		width = 8
	}
	p := clampPct(pct)
	filled := int((p / 100.0) * float64(width))
	if filled > width {
		filled = width
	}
	if filled < 0 {
		filled = 0
	}
	return "[" + strings.Repeat("#", filled) + strings.Repeat("-", width-filled) + "]"
}

func coreSparkline(values []float64, maxChars int) string {
	if len(values) == 0 {
		return "n/a"
	}
	if maxChars <= 0 {
		return ""
	}
	levels := []rune(" .:-=+*#%@")
	var b strings.Builder
	for i, v := range values {
		if i >= maxChars {
			break
		}
		idx := int((clampPct(v) / 100.0) * float64(len(levels)-1))
		if idx < 0 {
			idx = 0
		}
		if idx >= len(levels) {
			idx = len(levels) - 1
		}
		b.WriteRune(levels[idx])
	}
	if len(values) > maxChars && maxChars >= 3 {
		r := []rune(b.String())
		if len(r) >= 3 {
			return string(r[:len(r)-3]) + "..."
		}
	}
	return b.String()
}

func terminalWidth() int {
	if colsEnv := strings.TrimSpace(os.Getenv("COLUMNS")); colsEnv != "" {
		if n, err := strconv.Atoi(colsEnv); err == nil && n > 0 {
			return n
		}
	}

	type winsize struct {
		Row    uint16
		Col    uint16
		Xpixel uint16
		Ypixel uint16
	}

	ws := &winsize{}
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		os.Stdout.Fd(),
		uintptr(syscall.TIOCGWINSZ),
		uintptr(unsafe.Pointer(ws)),
	)
	if errno == 0 && ws.Col > 0 {
		return int(ws.Col)
	}

	return 120
}

func gridDims(total, width int) (int, int) {
	const (
		minCard = 44
		maxCard = 64
		gap     = 2
	)
	if total <= 1 {
		cw := width
		if cw > maxCard {
			cw = maxCard
		}
		if cw < minCard {
			cw = minCard
		}
		return 1, cw
	}

	maxCols := width / (minCard + gap)
	if maxCols < 1 {
		maxCols = 1
	}
	if maxCols > 3 {
		maxCols = 3
	}
	if maxCols > total {
		maxCols = total
	}

	for cols := maxCols; cols >= 1; cols-- {
		cw := (width - ((cols - 1) * gap)) / cols
		if cw >= minCard {
			if cw > maxCard {
				cw = maxCard
			}
			return cols, cw
		}
	}

	return 1, minCard
}

func makeCard(d DeviceMetrics, width int) []string {
	if width < 44 {
		width = 44
	}
	inner := width - 2
	mk := func(content string) string {
		return "|" + padRight(truncate(content, inner), inner) + "|"
	}

	cpuPct := clampPct(d.CPUUsage)
	memPct := 0.0
	if d.TotalMemory > 0 {
		memPct = clampPct((float64(d.UsedMemory) / float64(d.TotalMemory)) * 100)
	}

	host := d.Hostname
	if host == "" {
		host = "Unknown host"
	}
	ts := ""
	if !d.Timestamp.IsZero() {
		ts = d.Timestamp.Local().Format("15:04:05")
	}

	barWidth := inner - 2
	if barWidth > 30 {
		barWidth = 30
	}
	if barWidth < 10 {
		barWidth = 10
	}

	coreAvail := inner - len("Cores: ")
	if coreAvail < 3 {
		coreAvail = 3
	}

	lines := []string{
		"+" + strings.Repeat("-", inner) + "+",
		mk(fitTwo(host, ts, inner)),
		mk("id: " + shortID(d.DeviceID)),
		mk(fitTwo("CPU", fmt.Sprintf("%.1f%%", cpuPct), inner)),
		mk(progressBar(cpuPct, barWidth)),
		mk("Cores: " + coreSparkline(d.CoreCPUUsage, coreAvail)),
		mk(fitTwo("Memory", fmt.Sprintf("%s / %s (%.1f%%)", fmtBytes(d.UsedMemory), fmtBytes(d.TotalMemory), memPct), inner)),
		mk(progressBar(memPct, barWidth)),
		"|" + strings.Repeat("-", inner) + "|",
		mk(fitTwo("Disk R: "+fmtRate(d.DiskRead), "Disk W: "+fmtRate(d.DiskWrite), inner)),
		mk(fitTwo("Disk usage", fmt.Sprintf("%.1f%%", clampPct(d.DiskUsagePct)), inner)),
		mk(progressBar(clampPct(d.DiskUsagePct), barWidth)),
		mk(fitTwo("Net RX: "+fmtRate(d.NetRX), "Net TX: "+fmtRate(d.NetTX), inner)),
		"+" + strings.Repeat("-", inner) + "+",
	}

	return lines
}

func statusSummary(devices []DeviceMetrics, now time.Time) string {
	if len(devices) == 0 {
		return "Status: CONNECTING"
	}

	fresh := 0
	for _, d := range devices {
		if !d.Timestamp.IsZero() && now.Sub(d.Timestamp) <= 10*time.Second {
			fresh++
		}
	}

	switch {
	case fresh == len(devices):
		return fmt.Sprintf("Status: CONNECTED (%d/%d fresh)", fresh, len(devices))
	case fresh == 0:
		return fmt.Sprintf("Status: DISCONNECTED (0/%d fresh)", len(devices))
	default:
		return fmt.Sprintf("Status: DEGRADED (%d/%d fresh)", fresh, len(devices))
	}
}

func render(devices []DeviceMetrics) {
	now := time.Now()
	fmt.Fprint(os.Stdout, clearScreen)
	fmt.Fprintf(os.Stdout, "%sMonitorHub%s  %s\n",
		ansiBold, ansiReset, now.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(os.Stdout, "%s%s%s\n\n", ansiCyan, statusSummary(devices, now), ansiReset)

	if len(devices) == 0 {
		fmt.Fprintln(os.Stdout, "  waiting for metrics...")
		return
	}

	sort.Slice(devices, func(i, j int) bool {
		if devices[i].Hostname == devices[j].Hostname {
			return devices[i].DeviceID < devices[j].DeviceID
		}
		return devices[i].Hostname < devices[j].Hostname
	})

	width := terminalWidth()
	cols, cardWidth := gridDims(len(devices), width)
	const gap = "  "

	cards := make([][]string, 0, len(devices))
	for _, d := range devices {
		cards = append(cards, makeCard(d, cardWidth))
	}

	cardHeight := len(cards[0])
	for row := 0; row < len(cards); row += cols {
		end := row + cols
		if end > len(cards) {
			end = len(cards)
		}
		for line := 0; line < cardHeight; line++ {
			for i := row; i < end; i++ {
				if i > row {
					fmt.Fprint(os.Stdout, gap)
				}
				fmt.Fprint(os.Stdout, cards[i][line])
			}
			fmt.Fprintln(os.Stdout)
		}
		fmt.Fprintln(os.Stdout)
	}

	stale := 0
	for _, d := range devices {
		if d.Timestamp.IsZero() || now.Sub(d.Timestamp) > 10*time.Second {
			stale++
		}
	}
	if stale > 0 {
		fmt.Fprintf(os.Stdout, "%s%d stale device(s)%s\n", ansiYellow, stale, ansiReset)
	}

	freshest := devices[0].Timestamp
	for _, d := range devices[1:] {
		if d.Timestamp.After(freshest) {
			freshest = d.Timestamp
		}
	}
	if !freshest.IsZero() {
		fmt.Fprintf(os.Stdout, "Last update: %s ago\n", fmtAge(freshest))
	}

	fmt.Fprintf(os.Stdout, "\n%s%d device(s)  Ctrl+C to quit%s\n", ansiCyan, len(devices), ansiReset)
}

func runTUI(ctx context.Context, h *hub) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	render(h.snapshot())

	for {
		select {
		case <-ctx.Done():
			fmt.Fprint(os.Stdout, clearScreen)
			return
		case <-ticker.C:
			render(h.snapshot())
		}
	}
}
