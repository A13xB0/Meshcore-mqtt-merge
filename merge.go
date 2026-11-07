package main

import (
	"crypto/sha256"
	"encoding/json"
	"strings"
	"sync"
	"time"
)

type Msg struct {
	Source  string
	Topic   string
	Payload []byte
}

type Out struct {
	Topic   string
	Payload []byte
	Retain  bool
}

type Counts struct{ In, Out, Dup, Suppressed uint64 }

type pin struct {
	iata string
	rank int
}

type Merger struct {
	Window    time.Duration
	TopicRoot string
	Now       func() time.Time

	mu        sync.Mutex
	rank      map[string]int
	seen      map[[32]byte]time.Time
	lastSweep time.Time
	pins      map[string]pin
	online    map[string]map[string]bool
	stats     map[string]*Counts
}

func NewMerger(sources []string, window time.Duration, topicRoot string) *Merger {
	m := &Merger{
		Window:    window,
		TopicRoot: topicRoot,
		Now:       time.Now,
		rank:      map[string]int{},
		seen:      map[[32]byte]time.Time{},
		pins:      map[string]pin{},
		online:    map[string]map[string]bool{},
		stats:     map[string]*Counts{},
	}
	for i, s := range sources {
		m.rank[s] = i
		m.stats[s] = &Counts{}
	}
	return m
}

// Handle decides whether a message from one input is forwarded to the output.
func (m *Merger) Handle(in Msg) (Out, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.Now()
	m.sweep(now)
	c := m.stats[in.Source]
	if c == nil {
		c = &Counts{}
		m.stats[in.Source] = c
	}
	c.In++

	var body struct {
		Raw    string `json:"raw"`
		Status string `json:"status"`
	}
	_ = json.Unmarshal(in.Payload, &body)

	parts := strings.Split(in.Topic, "/")
	topic := in.Topic
	var key [32]byte
	retain := false

	if len(parts) == 4 && parts[1] != "client" {
		obs := strings.ToUpper(parts[2])
		kind := parts[3]
		parts[1] = m.region(obs, parts[1], in.Source)
		topic = strings.Join(parts, "/")

		content := in.Payload
		if body.Raw != "" {
			content = []byte(strings.ToUpper(body.Raw))
		}
		key = hash(obs, kind, string(content))

		if kind == "status" {
			retain = true
			if !m.status(obs, in.Source, body.Status) {
				c.Suppressed++
				return Out{}, false
			}
		}
	} else {
		key = hash(in.Topic, string(in.Payload))
	}

	if t, ok := m.seen[key]; ok && now.Sub(t) < m.Window {
		c.Dup++
		return Out{}, false
	}
	m.seen[key] = now
	c.Out++

	if m.TopicRoot != "" {
		parts = strings.SplitN(topic, "/", 2)
		parts[0] = m.TopicRoot
		topic = strings.Join(parts, "/")
	}
	return Out{Topic: topic, Payload: in.Payload, Retain: retain}, true
}

// SourceDown forgets an input's online claims once we can no longer hear its LWTs.
func (m *Merger) SourceDown(src string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, set := range m.online {
		delete(set, src)
	}
}

func (m *Merger) Stats() map[string]Counts {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]Counts, len(m.stats))
	for k, v := range m.stats {
		out[k] = *v
	}
	return out
}

// region pins each observer to the IATA code used on the highest-priority input that carries it.
func (m *Merger) region(obs, iata, src string) string {
	r, ok := m.rank[src]
	if !ok {
		r = len(m.rank)
	}
	p, ok := m.pins[obs]
	if !ok || r <= p.rank {
		m.pins[obs] = pin{iata, r}
		return iata
	}
	return p.iata
}

// status reports whether a status change should be forwarded: offline only once no input sees the observer online.
func (m *Merger) status(obs, src, st string) bool {
	set := m.online[obs]
	if set == nil {
		set = map[string]bool{}
		m.online[obs] = set
	}
	if st == "" || strings.EqualFold(st, "online") {
		set[src] = true
		return true
	}
	delete(set, src)
	return len(set) == 0
}

func (m *Merger) sweep(now time.Time) {
	if now.Sub(m.lastSweep) < m.Window {
		return
	}
	m.lastSweep = now
	for k, t := range m.seen {
		if now.Sub(t) >= m.Window {
			delete(m.seen, k)
		}
	}
}

func hash(parts ...string) [32]byte {
	return sha256.Sum256([]byte(strings.Join(parts, "\x00")))
}
