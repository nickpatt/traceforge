// Package sampling implements tail-based sampling with out-of-order trace
// reconstruction. Spans are buffered per trace_id until the trace is "complete"
// (root seen + quiet period) or a max-wait timeout fires, then policies decide
// whether to keep the whole trace.
package sampling

import (
	"sort"
	"sync"
	"time"

	"github.com/nickpatt/traceforge/internal/model"
)

// Policy configures the tail-sampling decision.
type Policy struct {
	// KeepAllErrors keeps any trace that has at least one ERROR span.
	KeepAllErrors bool
	// LatencyThreshold keeps traces whose root duration >= this value.
	LatencyThreshold time.Duration
	// BaseSampleRate is the probability [0,1] of keeping otherwise-uninteresting
	// traces (baseline traffic).
	BaseSampleRate float64
}

// Config tunes the buffer and completion detection.
type Config struct {
	QuietPeriod time.Duration // no new spans for this long after root => decide
	MaxWait     time.Duration // hard cap from first span => force decide
	MaxTraces   int           // memory guard: force-evict oldest beyond this
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{QuietPeriod: 2 * time.Second, MaxWait: 10 * time.Second, MaxTraces: 100_000}
}

type traceBuffer struct {
	spans     []*model.Span
	firstSeen time.Time
	lastSeen  time.Time
	hasRoot   bool
}

// Decision is the outcome for a completed trace.
type Decision struct {
	TraceID string
	Spans   []*model.Span // reconstructed (sorted) spans
	Keep    bool
	Reason  string
}

// Sampler buffers spans and emits decisions.
type Sampler struct {
	cfg    Config
	policy Policy
	rng    func() float64

	mu      sync.Mutex
	buffers map[string]*traceBuffer
}

// New creates a Sampler.
func New(cfg Config, policy Policy, rng func() float64) *Sampler {
	return &Sampler{
		cfg: cfg, policy: policy, rng: rng,
		buffers: make(map[string]*traceBuffer),
	}
}

// Add buffers a span. It never blocks; completion is driven by Sweep.
func (s *Sampler) Add(sp *model.Span) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.buffers[sp.TraceID]
	if b == nil {
		b = &traceBuffer{firstSeen: time.Now()}
		s.buffers[sp.TraceID] = b
	}
	b.spans = append(b.spans, sp)
	b.lastSeen = time.Now()
	if sp.IsRoot() {
		b.hasRoot = true
	}
	// Memory guard: if we exceed MaxTraces, the oldest are force-decided by
	// the next Sweep via MaxWait; a production impl would evict here.
}

// Sweep returns decisions for all traces that are now complete or timed out.
// Call it on a ticker (e.g. every 500ms).
func (s *Sampler) Sweep(now time.Time) []Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	var decisions []Decision
	for id, b := range s.buffers {
		complete := b.hasRoot && now.Sub(b.lastSeen) >= s.cfg.QuietPeriod
		timedOut := now.Sub(b.firstSeen) >= s.cfg.MaxWait
		if !complete && !timedOut {
			continue
		}
		decisions = append(decisions, s.decide(id, b))
		delete(s.buffers, id)
	}
	return decisions
}

// Buffered returns the number of traces currently buffered.
func (s *Sampler) Buffered() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buffers)
}

// decide applies policies and reconstructs (sorts) the trace.
func (s *Sampler) decide(id string, b *traceBuffer) Decision {
	sort.Slice(b.spans, func(i, j int) bool {
		return b.spans[i].StartUnixNs < b.spans[j].StartUnixNs
	})

	// error policy
	if s.policy.KeepAllErrors {
		for _, sp := range b.spans {
			if sp.StatusCode == model.StatusError {
				return Decision{id, b.spans, true, "error"}
			}
		}
	}
	// latency policy (root duration)
	if s.policy.LatencyThreshold > 0 {
		for _, sp := range b.spans {
			if sp.IsRoot() && time.Duration(sp.DurationNs()) >= s.policy.LatencyThreshold {
				return Decision{id, b.spans, true, "slow"}
			}
		}
	}
	// probabilistic baseline
	if s.rng() < s.policy.BaseSampleRate {
		return Decision{id, b.spans, true, "probabilistic"}
	}
	return Decision{id, b.spans, false, "dropped"}
}
