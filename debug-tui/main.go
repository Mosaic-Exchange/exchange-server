package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// --- JSON types matching the Java debug endpoint ---

type ExecutorSnapshot struct {
	ActiveThreads  int   `json:"activeThreads"`
	PoolSize       int   `json:"poolSize"`
	MaxPoolSize    int   `json:"maxPoolSize"`
	QueueSize      int   `json:"queueSize"`
	CompletedTasks int64 `json:"completedTasks"`
}

type ExecutorPair struct {
	Remote ExecutorSnapshot `json:"remote"`
	Local  ExecutorSnapshot `json:"local"`
}

type PendingDetail struct {
	RequestID int    `json:"requestId"`
	Streaming bool   `json:"streaming"`
	ElapsedMs int64  `json:"elapsedMs"`
}

type Requests struct {
	PendingOutbound    int `json:"pendingOutbound"`
	ActiveStreamsServer int `json:"activeStreamsServer"`
	PendingHandshakes  int `json:"pendingHandshakes"`
}

type ClusterNode struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Status    string `json:"status"`
	Services  string `json:"services"`
	Heartbeat int64  `json:"heartbeat"`
	Self      bool   `json:"self"`
}

type DebugData struct {
	Node           string                  `json:"node"`
	UptimeMs       int64                   `json:"uptimeMs"`
	Requests       Requests                `json:"requests"`
	PendingDetails []PendingDetail         `json:"pendingDetails"`
	Executors      map[string]ExecutorPair `json:"executors"`
	Cluster        []ClusterNode           `json:"cluster"`
}

// --- Bubbletea model ---

type tickMsg time.Time

type dataMsg struct {
	data *DebugData
	err  error
}

type model struct {
	url       string
	data      *DebugData
	err       error
	width     int
	height    int
	lastFetch time.Time
	pollRate  time.Duration
}

func initialModel(url string, pollRate time.Duration) model {
	return model{
		url:      url,
		pollRate: pollRate,
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(fetchData(m.url), tick(m.pollRate))
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "r":
			return m, fetchData(m.url)
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tickMsg:
		return m, tea.Batch(fetchData(m.url), tick(m.pollRate))
	case dataMsg:
		if msg.err != nil {
			m.err = msg.err
			m.data = nil
		} else {
			m.err = nil
			m.data = msg.data
		}
		m.lastFetch = time.Now()
	}
	return m, nil
}

func (m model) View() string {
	if m.width == 0 {
		return "Initializing..."
	}

	// Styles
	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#FF6F00")).
		Background(lipgloss.Color("#1A1A2E")).
		Padding(0, 1).
		Width(m.width)

	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#00E5FF")).
		MarginTop(1).
		MarginBottom(0)

	boxStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#444466")).
		Padding(0, 1).
		MarginBottom(0)

	dimStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#666688"))
	valStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#EEFF41"))
	okStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#69F0AE"))
	warnStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FF5252"))
	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#B0BEC5")).Width(22)
	nodeStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#CE93D8"))

	var b strings.Builder

	// Title bar
	status := okStyle.Render(" CONNECTED")
	if m.err != nil {
		status = warnStyle.Render(" DISCONNECTED")
	}
	titleText := fmt.Sprintf(" MOSAIC DEBUG DASHBOARD %s", status)
	b.WriteString(titleStyle.Render(titleText))
	b.WriteString("\n")

	if m.err != nil {
		errBox := boxStyle.Width(m.width - 4).Render(
			warnStyle.Render("Error: ") + dimStyle.Render(m.err.Error()) +
				"\n" + dimStyle.Render("Retrying every "+m.pollRate.String()+"... (r=refresh, q=quit)"),
		)
		b.WriteString(errBox)
		return b.String()
	}

	if m.data == nil {
		b.WriteString(dimStyle.Render("  Fetching data..."))
		return b.String()
	}

	d := m.data
	halfWidth := m.width/2 - 3
	if halfWidth < 30 {
		halfWidth = m.width - 4
	}

	// --- Row 1: Node Info + Request Stats side by side ---
	uptime := formatDuration(time.Duration(d.UptimeMs) * time.Millisecond)

	nodeInfo := headerStyle.Render("  NODE") + "\n" +
		boxStyle.Width(halfWidth).Render(
			labelStyle.Render("  Address")+valStyle.Render(d.Node)+"\n"+
				labelStyle.Render("  Uptime")+dimStyle.Render(uptime),
		)

	pendingColor := okStyle
	if d.Requests.PendingOutbound > 0 {
		pendingColor = valStyle
	}

	reqInfo := headerStyle.Render("  REQUESTS") + "\n" +
		boxStyle.Width(halfWidth).Render(
			labelStyle.Render("  Pending Outbound")+pendingColor.Render(fmt.Sprintf("%d", d.Requests.PendingOutbound))+"\n"+
				labelStyle.Render("  Active Streams (svr)")+valStyle.Render(fmt.Sprintf("%d", d.Requests.ActiveStreamsServer))+"\n"+
				labelStyle.Render("  Pending Handshakes")+dimStyle.Render(fmt.Sprintf("%d", d.Requests.PendingHandshakes)),
		)

	if halfWidth < m.width-4 {
		b.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, nodeInfo, "  ", reqInfo))
	} else {
		b.WriteString(nodeInfo + "\n" + reqInfo)
	}
	b.WriteString("\n")

	// --- Pending Request Details ---
	if len(d.PendingDetails) > 0 {
		b.WriteString(headerStyle.Render("  ACTIVE REQUESTS"))
		b.WriteString("\n")

		var rows strings.Builder
		rows.WriteString(fmt.Sprintf("  %-6s %-10s %-12s\n",
			dimStyle.Render("ID"), dimStyle.Render("TYPE"), dimStyle.Render("ELAPSED")))

		sort.Slice(d.PendingDetails, func(i, j int) bool {
			return d.PendingDetails[i].ElapsedMs > d.PendingDetails[j].ElapsedMs
		})

		for _, pd := range d.PendingDetails {
			reqType := "req/res"
			if pd.Streaming {
				reqType = "stream"
			}
			elapsed := formatDuration(time.Duration(pd.ElapsedMs) * time.Millisecond)
			color := okStyle
			if pd.ElapsedMs > 10000 {
				color = warnStyle
			} else if pd.ElapsedMs > 3000 {
				color = valStyle
			}
			rows.WriteString(fmt.Sprintf("  %-6s %-10s %-12s\n",
				valStyle.Render(fmt.Sprintf("#%d", pd.RequestID)),
				dimStyle.Render(reqType),
				color.Render(elapsed)))
		}

		b.WriteString(boxStyle.Width(m.width - 4).Render(rows.String()))
		b.WriteString("\n")
	}

	// --- Executor Pools ---
	if len(d.Executors) > 0 {
		b.WriteString(headerStyle.Render("  EXECUTOR POOLS"))
		b.WriteString("\n")

		svcNames := make([]string, 0, len(d.Executors))
		for name := range d.Executors {
			svcNames = append(svcNames, name)
		}
		sort.Strings(svcNames)

		for _, name := range svcNames {
			pair := d.Executors[name]
			b.WriteString(boxStyle.Width(m.width - 4).Render(
				nodeStyle.Render("  "+name) + "\n" +
					renderExecutor("Remote", pair.Remote, labelStyle, valStyle, okStyle, warnStyle, dimStyle) +
					renderExecutor("Local ", pair.Local, labelStyle, valStyle, okStyle, warnStyle, dimStyle),
			))
			b.WriteString("\n")
		}
	}

	// --- Cluster ---
	b.WriteString(headerStyle.Render("  CLUSTER"))
	b.WriteString("\n")

	var clusterRows strings.Builder
	clusterRows.WriteString(fmt.Sprintf("  %-24s %-10s %-10s %-10s %s\n",
		dimStyle.Render("NODE"),
		dimStyle.Render("TYPE"),
		dimStyle.Render("STATUS"),
		dimStyle.Render("HEARTBEAT"),
		dimStyle.Render("SERVICES")))

	for _, n := range d.Cluster {
		statusColor := okStyle
		if n.Status != "ALIVE" {
			statusColor = warnStyle
		}
		selfMark := ""
		if n.Self {
			selfMark = dimStyle.Render(" (self)")
		}
		services := n.Services
		if services == "" {
			services = "-"
		}

		clusterRows.WriteString(fmt.Sprintf("  %-24s %-10s %-10s %-10s %s\n",
			nodeStyle.Render(n.ID)+selfMark,
			dimStyle.Render(n.Type),
			statusColor.Render(n.Status),
			dimStyle.Render(fmt.Sprintf("%d", n.Heartbeat)),
			dimStyle.Render(services)))
	}

	b.WriteString(boxStyle.Width(m.width - 4).Render(clusterRows.String()))
	b.WriteString("\n")

	// Footer
	b.WriteString(dimStyle.Render(fmt.Sprintf("  Last update: %s  |  r=refresh  q=quit  |  polling every %s",
		m.lastFetch.Format("15:04:05"), m.pollRate.String())))

	return b.String()
}

func renderExecutor(label string, es ExecutorSnapshot,
	labelSty, valSty, okSty, warnSty, dimSty lipgloss.Style) string {

	active := fmt.Sprintf("%d/%d", es.ActiveThreads, es.MaxPoolSize)
	activeColor := okSty
	if es.ActiveThreads == es.MaxPoolSize && es.MaxPoolSize > 0 {
		activeColor = warnSty
	} else if es.ActiveThreads > 0 {
		activeColor = valSty
	}

	bar := renderBar(es.ActiveThreads, es.MaxPoolSize, 12)

	return fmt.Sprintf("    %s  %s %s  %s %s  %s\n",
		dimSty.Render(label),
		activeColor.Render(active),
		bar,
		dimSty.Render(fmt.Sprintf("queue:%d", es.QueueSize)),
		dimSty.Render(fmt.Sprintf("done:%d", es.CompletedTasks)),
		"",
	)
}

func renderBar(active, max, width int) string {
	if max == 0 {
		return lipgloss.NewStyle().Foreground(lipgloss.Color("#444466")).Render(strings.Repeat(".", width))
	}
	filled := (active * width) / max
	if active > 0 && filled == 0 {
		filled = 1
	}
	empty := width - filled

	filledColor := lipgloss.Color("#69F0AE")
	if active == max {
		filledColor = lipgloss.Color("#FF5252")
	} else if active > max/2 {
		filledColor = lipgloss.Color("#EEFF41")
	}

	return lipgloss.NewStyle().Foreground(filledColor).Render(strings.Repeat("█", filled)) +
		lipgloss.NewStyle().Foreground(lipgloss.Color("#333355")).Render(strings.Repeat("░", empty))
}

func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}

// --- Commands ---

func tick(d time.Duration) tea.Cmd {
	return tea.Tick(d, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func fetchData(url string) tea.Cmd {
	return func() tea.Msg {
		client := &http.Client{Timeout: 3 * time.Second}
		resp, err := client.Get(url)
		if err != nil {
			return dataMsg{err: err}
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return dataMsg{err: err}
		}

		var data DebugData
		if err := json.Unmarshal(body, &data); err != nil {
			return dataMsg{err: fmt.Errorf("invalid JSON: %w", err)}
		}
		return dataMsg{data: &data}
	}
}

func main() {
	host := flag.String("host", "localhost", "Debug server host")
	port := flag.Int("port", 9090, "Debug server port")
	rate := flag.Duration("rate", 1*time.Second, "Poll interval")
	flag.Parse()

	url := fmt.Sprintf("http://%s:%d/debug", *host, *port)

	p := tea.NewProgram(
		initialModel(url, *rate),
		tea.WithAltScreen(),
	)

	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
