package search

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/grafana/sobek"
	"github.com/paradedb/benchmarker/metrics"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

type phaseSpec struct {
	backends     []string
	positions    map[string]int
	duration     time.Duration
	prewarm      time.Duration
	queryWorkers int
	hasUpdater   bool
}

type activePhase struct {
	warmIterations      uint64
	nextWarmIteration   uint64
	warmCompleted       uint64
	warmWorkers         int
	queryArrivals       int
	updaterArrived      bool
	prewarming          bool
	prewarmDeadline     time.Time
	measurementStarting bool
	measuring           bool
	finalizing          bool
	deadline            time.Time
	remaining           int
	nextIteration       uint64
}

type phaseState struct {
	mu      sync.Mutex
	changed chan struct{}
	spec    phaseSpec
	current int
	active  activePhase
	err     error
}

// PhaseCoordinator is a VU-local handle to one process-wide benchmark phase
// sequence. The shared state serializes backend phases while callbacks execute
// on their owning VU runtimes.
type PhaseCoordinator struct {
	vu    modules.VU
	state *phaseState
}

const (
	updatePrewarmCount      = 2
	updatePrewarmPause      = time.Second
	collectorInterval       = 500 * time.Millisecond
	prewarmQueryCancelGrace = 250 * time.Millisecond
)

func (r *RootModule) measurementDeadline(alias string) (time.Time, bool) {
	r.phaseMu.Lock()
	state := r.phases
	r.phaseMu.Unlock()
	if state == nil {
		return time.Time{}, false
	}
	return state.measurementDeadline(alias)
}

// queryPhase reports whether a client belongs to a coordinated benchmark
// phase and, while it is running queries, whether those queries are prewarm or
// measured work. Prewarm queries receive a short grace period beyond the
// logical boundary so ordinary queries drain naturally and VUs retain their
// established cadence. Only queries that remain in flight past that grace are
// canceled.
func (r *RootModule) queryPhase(alias string) (time.Time, bool, bool) {
	r.phaseMu.Lock()
	state := r.phases
	r.phaseMu.Unlock()
	if state == nil {
		return time.Time{}, false, false
	}
	return state.queryPhase(alias)
}

func (m *ModuleInstance) newPhases(config map[string]interface{}) *PhaseCoordinator {
	spec, err := parsePhaseSpec(config)
	if err != nil {
		common.Throw(m.vu.Runtime(), err)
		return nil
	}

	m.root.phaseMu.Lock()
	defer m.root.phaseMu.Unlock()
	if m.root.phases == nil {
		m.root.phases = &phaseState{
			changed: make(chan struct{}),
			spec:    spec,
		}
	} else if !samePhaseSpec(m.root.phases.spec, spec) {
		common.Throw(m.vu.Runtime(), fmt.Errorf("phases: every VU must use the same phase configuration"))
		return nil
	}
	return &PhaseCoordinator{vu: m.vu, state: m.root.phases}
}

func parsePhaseSpec(config map[string]interface{}) (phaseSpec, error) {
	rawBackends, ok := config["backends"].([]interface{})
	if !ok || len(rawBackends) == 0 {
		return phaseSpec{}, fmt.Errorf("phases: non-empty backends array is required")
	}
	backends := make([]string, len(rawBackends))
	positions := make(map[string]int, len(rawBackends))
	for i, raw := range rawBackends {
		backend, ok := raw.(string)
		if !ok || backend == "" {
			return phaseSpec{}, fmt.Errorf("phases: backend %d must be a non-empty string", i)
		}
		if _, duplicate := positions[backend]; duplicate {
			return phaseSpec{}, fmt.Errorf("phases: duplicate backend %s", backend)
		}
		backends[i] = backend
		positions[backend] = i
	}

	durationText, _ := config["duration"].(string)
	duration, err := time.ParseDuration(durationText)
	if err != nil || duration <= 0 {
		return phaseSpec{}, fmt.Errorf("phases: invalid duration %q", durationText)
	}
	prewarmText, _ := config["prewarm"].(string)
	prewarm := time.Duration(0)
	if prewarmText != "" {
		prewarm, err = time.ParseDuration(prewarmText)
		if err != nil || prewarm < 0 {
			return phaseSpec{}, fmt.Errorf("phases: invalid prewarm duration %q", prewarmText)
		}
	}
	queryWorkers, ok := phasePositiveInt(config["vus"])
	if !ok {
		return phaseSpec{}, fmt.Errorf("phases: vus must be a positive integer")
	}
	hasUpdater, _ := config["updates"].(bool)

	return phaseSpec{
		backends:     backends,
		positions:    positions,
		duration:     duration,
		prewarm:      prewarm,
		queryWorkers: queryWorkers,
		hasUpdater:   hasUpdater,
	}, nil
}

func phasePositiveInt(value interface{}) (int, bool) {
	var number float64
	switch typed := value.(type) {
	case int:
		number = float64(typed)
	case int64:
		number = float64(typed)
	case float64:
		number = typed
	default:
		return 0, false
	}
	if math.IsNaN(number) || math.IsInf(number, 0) || number < 1 || math.Trunc(number) != number || number > float64(maxSafeInteger) {
		return 0, false
	}
	return int(number), true
}

func samePhaseSpec(left, right phaseSpec) bool {
	return left.duration == right.duration &&
		left.prewarm == right.prewarm &&
		left.queryWorkers == right.queryWorkers &&
		left.hasUpdater == right.hasUpdater &&
		reflect.DeepEqual(left.backends, right.backends)
}

func (p *PhaseCoordinator) phaseIndex(alias string) (int, error) {
	index, ok := p.state.spec.positions[alias]
	if !ok {
		return 0, fmt.Errorf("phases: unknown backend %s", alias)
	}
	return index, nil
}

// Run distributes the initial pg_prewarm work, runs the ordinary query stream
// unmeasured on every connected query VU for the configured prewarm duration,
// and then continues the same stream for the full measurement duration.
func (p *PhaseCoordinator) Run(call sobek.FunctionCall) sobek.Value {
	rt := p.vu.Runtime()
	if len(call.Arguments) < 5 {
		common.Throw(rt, fmt.Errorf("phases.run requires (backend, warmIterations, warm, startMeasurement, query)"))
		return sobek.Undefined()
	}
	alias := call.Arguments[0].String()
	warmIterationsValue := call.Arguments[1].ToFloat()
	if math.IsNaN(warmIterationsValue) || math.IsInf(warmIterationsValue, 0) || warmIterationsValue < 0 || math.Trunc(warmIterationsValue) != warmIterationsValue || warmIterationsValue > float64(maxSafeInteger) {
		common.Throw(rt, fmt.Errorf("phases.run warmIterations must be a non-negative integer"))
		return sobek.Undefined()
	}
	warmIterations := uint64(warmIterationsValue)
	if p.state.spec.prewarm == 0 {
		warmIterations = 0
	}
	warm, ok := sobek.AssertFunction(call.Arguments[2])
	if !ok {
		common.Throw(rt, fmt.Errorf("phases.run warm argument must be a function"))
		return sobek.Undefined()
	}
	startMeasurement, ok := sobek.AssertFunction(call.Arguments[3])
	if !ok {
		common.Throw(rt, fmt.Errorf("phases.run startMeasurement argument must be a function"))
		return sobek.Undefined()
	}
	query, ok := sobek.AssertFunction(call.Arguments[4])
	if !ok {
		common.Throw(rt, fmt.Errorf("phases.run query argument must be a function"))
		return sobek.Undefined()
	}
	index, err := p.phaseIndex(alias)
	if err != nil {
		common.Throw(rt, err)
		return sobek.Undefined()
	}
	ctx := p.context()
	leader, err := p.state.arriveQuery(ctx, index, warmIterations)
	if err != nil {
		p.state.fail(err)
		common.Throw(rt, err)
		return sobek.Undefined()
	}
	if leader {
		if warmIterations > 0 {
			fmt.Printf("[pg_prewarm] %s: starting %d operations\n", alias, warmIterations)
			metrics.EmitPrewarmProgress(p.vu, alias, 0)
		}
	}
	if err := p.state.waitForWarm(ctx, index); err != nil {
		p.state.fail(err)
		common.Throw(rt, err)
		return sobek.Undefined()
	}
	for {
		iteration, ok, err := p.state.takeWarmIteration(index)
		if err != nil {
			p.state.fail(err)
			common.Throw(rt, err)
			return sobek.Undefined()
		}
		if !ok {
			break
		}
		if _, err := warm(sobek.Undefined(), rt.ToValue(iteration)); err != nil {
			wrapped := fmt.Errorf("prewarm %s: %w", alias, err)
			p.state.fail(wrapped)
			common.Throw(rt, wrapped)
			return sobek.Undefined()
		}
		completed, total, err := p.state.completeWarmIteration(index)
		if err != nil {
			p.state.fail(err)
			common.Throw(rt, err)
			return sobek.Undefined()
		}
		metrics.EmitPrewarmProgress(p.vu, alias, float64(completed)*50/float64(total))
	}
	p.state.finishWarmWorker(index)
	if leader {
		if err := p.state.waitForReady(ctx, index); err != nil {
			p.state.fail(err)
			common.Throw(rt, err)
			return sobek.Undefined()
		}
		if warmIterations > 0 {
			fmt.Printf("[pg_prewarm] %s: completed %d operations\n", alias, warmIterations)
		}
		if err := p.state.startPrewarm(index); err != nil {
			p.state.fail(err)
			common.Throw(rt, err)
			return sobek.Undefined()
		}
		if p.state.spec.prewarm > 0 {
			fmt.Printf("[prewarm] %s: running query stream for %s\n", alias, p.state.spec.prewarm)
			if warmIterations == 0 {
				metrics.EmitPrewarmProgress(p.vu, alias, 0)
			}
		}
	}

	prewarmDeadline, err := p.state.waitForPrewarm(ctx, index)
	if err != nil {
		p.state.fail(err)
		common.Throw(rt, err)
		return sobek.Undefined()
	}
	for time.Now().Before(prewarmDeadline) {
		iteration, err := p.state.takeIteration(index)
		if err != nil {
			common.Throw(rt, err)
			return sobek.Undefined()
		}
		if _, err := query(sobek.Undefined(), rt.ToValue(iteration)); err != nil {
			p.state.fail(fmt.Errorf("prewarm query %s: %w", alias, err))
			common.Throw(rt, err)
			return sobek.Undefined()
		}
		elapsed := p.state.spec.prewarm - time.Until(prewarmDeadline)
		baseProgress := float64(0)
		if warmIterations > 0 {
			baseProgress = 50
		}
		progress := baseProgress + math.Min(100-baseProgress, float64(elapsed)*(100-baseProgress)/float64(p.state.spec.prewarm))
		metrics.EmitPrewarmProgress(p.vu, alias, progress)
		if err := ctx.Err(); err != nil {
			p.state.fail(err)
			common.Throw(rt, err)
			return sobek.Undefined()
		}
	}
	starter, err := p.state.claimMeasurementStart(index)
	if err != nil {
		p.state.fail(err)
		common.Throw(rt, err)
		return sobek.Undefined()
	}
	if starter {
		if _, err := startMeasurement(sobek.Undefined()); err != nil {
			wrapped := fmt.Errorf("measurement start %s: %w", alias, err)
			p.state.fail(wrapped)
			common.Throw(rt, wrapped)
			return sobek.Undefined()
		}
		if err := p.state.startMeasurement(index); err != nil {
			p.state.fail(err)
			common.Throw(rt, err)
			return sobek.Undefined()
		}
		if p.state.spec.prewarm > 0 {
			fmt.Printf("[prewarm] %s: query stream is warm; starting measurement\n", alias)
		}
	}

	deadline, err := p.state.waitForMeasurement(ctx, index)
	if err != nil {
		p.state.fail(err)
		common.Throw(rt, err)
		return sobek.Undefined()
	}
	for time.Now().Before(deadline) {
		iteration, err := p.state.takeIteration(index)
		if err != nil {
			common.Throw(rt, err)
			return sobek.Undefined()
		}
		if _, err := query(sobek.Undefined(), rt.ToValue(iteration)); err != nil {
			p.state.fail(fmt.Errorf("benchmark %s: %w", alias, err))
			common.Throw(rt, err)
			return sobek.Undefined()
		}
		if err := ctx.Err(); err != nil {
			p.state.fail(err)
			common.Throw(rt, err)
			return sobek.Undefined()
		}
	}
	p.state.finishWorker(index)
	return sobek.Undefined()
}

// RunUpdates invokes serial updates on an absolute schedule during a backend's
// measurement window. Slots missed while an update is running are skipped.
func (p *PhaseCoordinator) RunUpdates(call sobek.FunctionCall) sobek.Value {
	rt := p.vu.Runtime()
	if len(call.Arguments) < 4 {
		common.Throw(rt, fmt.Errorf("phases.runUpdates requires (backend, rate, prewarmUpdate, update)"))
		return sobek.Undefined()
	}
	alias := call.Arguments[0].String()
	rateValue := call.Arguments[1].ToFloat()
	if math.IsNaN(rateValue) || math.IsInf(rateValue, 0) || rateValue < 1 || math.Trunc(rateValue) != rateValue || rateValue > 1e9 {
		common.Throw(rt, fmt.Errorf("phases.runUpdates rate must be an integer between 1 and 1000000000"))
		return sobek.Undefined()
	}
	prewarmUpdate, ok := sobek.AssertFunction(call.Arguments[2])
	if !ok {
		common.Throw(rt, fmt.Errorf("phases.runUpdates prewarmUpdate argument must be a function"))
		return sobek.Undefined()
	}
	update, ok := sobek.AssertFunction(call.Arguments[3])
	if !ok {
		common.Throw(rt, fmt.Errorf("phases.runUpdates update argument must be a function"))
		return sobek.Undefined()
	}
	index, err := p.phaseIndex(alias)
	if err != nil {
		common.Throw(rt, err)
		return sobek.Undefined()
	}
	ctx := p.context()
	if err := p.state.waitForTurn(ctx, index); err != nil {
		p.state.fail(err)
		common.Throw(rt, err)
		return sobek.Undefined()
	}
	fmt.Printf("[prewarm] %s: starting %d updates\n", alias, updatePrewarmCount)
	for iteration := 0; iteration < updatePrewarmCount; iteration++ {
		if _, err := prewarmUpdate(sobek.Undefined()); err != nil {
			wrapped := fmt.Errorf("prewarm update %s: %w", alias, err)
			p.state.fail(wrapped)
			common.Throw(rt, wrapped)
			return sobek.Undefined()
		}
		if err := waitUntil(ctx, time.Now().Add(updatePrewarmPause)); err != nil {
			p.state.fail(err)
			common.Throw(rt, err)
			return sobek.Undefined()
		}
	}
	fmt.Printf("[prewarm] %s: completed %d updates\n", alias, updatePrewarmCount)
	if err := p.state.markUpdaterReady(index); err != nil {
		p.state.fail(err)
		common.Throw(rt, err)
		return sobek.Undefined()
	}
	deadline, err := p.state.waitForMeasurement(ctx, index)
	if err != nil {
		p.state.fail(err)
		common.Throw(rt, err)
		return sobek.Undefined()
	}
	interval := time.Duration(float64(time.Second) / rateValue)
	next := deadline.Add(-p.state.spec.duration)
	for next.Before(deadline) {
		if err := waitUntil(ctx, next); err != nil {
			p.state.fail(err)
			common.Throw(rt, err)
			return sobek.Undefined()
		}
		if !time.Now().Before(deadline) {
			break
		}
		if _, err := update(sobek.Undefined()); err != nil {
			p.state.fail(fmt.Errorf("updates %s: %w", alias, err))
			common.Throw(rt, err)
			return sobek.Undefined()
		}
		next = nextUpdateStart(next, time.Now(), interval)
	}
	p.state.finishWorker(index)
	return sobek.Undefined()
}

func nextUpdateStart(previous, completed time.Time, interval time.Duration) time.Time {
	next := previous.Add(interval)
	if next.Before(completed) {
		delay := completed.Sub(next)
		next = completed.Add(interval - delay%interval)
	}
	return next
}

// RunCollector invokes the ordinary metrics collector with the active backend,
// finalizes each completed phase, and takes one global final snapshot.
func (p *PhaseCoordinator) RunCollector(call sobek.FunctionCall) sobek.Value {
	rt := p.vu.Runtime()
	if len(call.Arguments) < 3 {
		common.Throw(rt, fmt.Errorf("phases.runCollector requires (collect, collectPhaseFinal, collectFinal)"))
		return sobek.Undefined()
	}
	collect, ok := sobek.AssertFunction(call.Arguments[0])
	if !ok {
		common.Throw(rt, fmt.Errorf("phases.runCollector collect argument must be a function"))
		return sobek.Undefined()
	}
	collectPhaseFinal, ok := sobek.AssertFunction(call.Arguments[1])
	if !ok {
		common.Throw(rt, fmt.Errorf("phases.runCollector collectPhaseFinal argument must be a function"))
		return sobek.Undefined()
	}
	collectFinal, ok := sobek.AssertFunction(call.Arguments[2])
	if !ok {
		common.Throw(rt, fmt.Errorf("phases.runCollector collectFinal argument must be a function"))
		return sobek.Undefined()
	}
	ctx := p.context()
	for {
		if err := ctx.Err(); err != nil {
			p.state.fail(err)
			common.Throw(rt, err)
			return sobek.Undefined()
		}
		current, measuring, finalizing, done, err := p.state.progress()
		if err != nil {
			common.Throw(rt, err)
			return sobek.Undefined()
		}
		if finalizing {
			alias := p.state.spec.backends[current]
			hasNext := current+1 < len(p.state.spec.backends)
			if _, err := collectPhaseFinal(sobek.Undefined(), rt.ToValue(alias), rt.ToValue(hasNext)); err != nil {
				p.state.fail(fmt.Errorf("final metrics collection for %s: %w", alias, err))
				common.Throw(rt, err)
				return sobek.Undefined()
			}
			if err := p.state.finishFinal(current); err != nil {
				p.state.fail(err)
				common.Throw(rt, err)
				return sobek.Undefined()
			}
			continue
		}
		if done {
			break
		}
		alias := p.state.spec.backends[current]
		collectionStarted := time.Now()
		if _, err := collect(sobek.Undefined(), rt.ToValue(alias), rt.ToValue(measuring)); err != nil {
			p.state.fail(fmt.Errorf("metrics collector: %w", err))
			common.Throw(rt, err)
			return sobek.Undefined()
		}
		if !measuring {
			if err := p.state.waitForCollector(ctx, current, collectionStarted.Add(collectorInterval)); err != nil {
				p.state.fail(err)
				common.Throw(rt, err)
				return sobek.Undefined()
			}
		}
	}
	if _, err := collectFinal(sobek.Undefined()); err != nil {
		p.state.fail(fmt.Errorf("final metrics collection: %w", err))
		common.Throw(rt, err)
	}
	return sobek.Undefined()
}

func (p *PhaseCoordinator) context() context.Context {
	if p.vu != nil {
		if ctx := p.vu.Context(); ctx != nil {
			return ctx
		}
	}
	return context.Background()
}

func (s *phaseState) arriveQuery(ctx context.Context, index int, warmIterations uint64) (bool, error) {
	if err := s.waitForTurn(ctx, index); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return false, s.err
	}
	if index != s.current || s.active.queryArrivals >= s.spec.queryWorkers {
		return false, fmt.Errorf("phase %s received too many query workers", s.spec.backends[index])
	}
	leader := s.active.queryArrivals == 0
	if leader {
		s.active.warmIterations = warmIterations
	} else if warmIterations != s.active.warmIterations {
		return false, fmt.Errorf("phase %s query workers disagree on warm iteration count", s.spec.backends[index])
	}
	s.active.queryArrivals++
	s.signalLocked()
	return leader, nil
}

func (s *phaseState) markUpdaterReady(index int) error {
	if !s.spec.hasUpdater {
		return fmt.Errorf("phase %s has no updater", s.spec.backends[index])
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if index != s.current || s.active.updaterArrived {
		return fmt.Errorf("phase %s received an extra updater", s.spec.backends[index])
	}
	s.active.updaterArrived = true
	s.signalLocked()
	return nil
}

func (s *phaseState) waitForWarm(ctx context.Context, index int) error {
	for {
		s.mu.Lock()
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return err
		}
		if index != s.current {
			s.mu.Unlock()
			return fmt.Errorf("phase %s advanced before warm-up", s.spec.backends[index])
		}
		if s.active.queryArrivals == s.spec.queryWorkers {
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *phaseState) takeWarmIteration(index int) (uint64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return 0, false, s.err
	}
	if index != s.current || s.active.measuring || s.active.queryArrivals != s.spec.queryWorkers {
		return 0, false, fmt.Errorf("phase %s is not warming", s.spec.backends[index])
	}
	if s.active.nextWarmIteration == s.active.warmIterations {
		return 0, false, nil
	}
	iteration := s.active.nextWarmIteration
	s.active.nextWarmIteration++
	return iteration, true, nil
}

func (s *phaseState) completeWarmIteration(index int) (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return 0, 0, s.err
	}
	if index != s.current || s.active.warmCompleted >= s.active.nextWarmIteration {
		return 0, 0, fmt.Errorf("phase %s completed an unclaimed warm iteration", s.spec.backends[index])
	}
	s.active.warmCompleted++
	return s.active.warmCompleted, s.active.warmIterations, nil
}

func (s *phaseState) finishWarmWorker(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil || index != s.current || s.active.warmWorkers >= s.spec.queryWorkers {
		return
	}
	s.active.warmWorkers++
	s.signalLocked()
}

func (s *phaseState) readyLocked() bool {
	return s.active.queryArrivals == s.spec.queryWorkers &&
		s.active.warmWorkers == s.spec.queryWorkers &&
		s.active.warmCompleted == s.active.warmIterations &&
		(!s.spec.hasUpdater || s.active.updaterArrived)
}

func (s *phaseState) waitForReady(ctx context.Context, index int) error {
	for {
		s.mu.Lock()
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return err
		}
		if index != s.current {
			s.mu.Unlock()
			return fmt.Errorf("phase %s advanced before its measurement boundary", s.spec.backends[index])
		}
		if s.readyLocked() {
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *phaseState) startPrewarm(index int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if index != s.current || s.active.prewarming || s.active.measuring ||
		s.active.measurementStarting || s.active.finalizing || !s.readyLocked() {
		return fmt.Errorf("phase %s is not ready to start query prewarm", s.spec.backends[index])
	}
	s.active.prewarming = true
	s.active.prewarmDeadline = time.Now().Add(s.spec.prewarm)
	s.signalLocked()
	return nil
}

func (s *phaseState) waitForPrewarm(ctx context.Context, index int) (time.Time, error) {
	for {
		s.mu.Lock()
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return time.Time{}, err
		}
		if index != s.current {
			s.mu.Unlock()
			return time.Time{}, fmt.Errorf("phase %s advanced before query prewarm", s.spec.backends[index])
		}
		if s.active.prewarming {
			deadline := s.active.prewarmDeadline
			s.mu.Unlock()
			return deadline, nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		}
	}
}

// claimMeasurementStart elects the first query VU to finish naturally after
// the logical prewarm boundary, or to be canceled after the drain grace. That
// VU resets phase-scoped telemetry and opens measurement; other VUs retain
// their existing cadence as their in-flight prewarm queries finish.
func (s *phaseState) claimMeasurementStart(index int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return false, s.err
	}
	if index != s.current || s.active.finalizing {
		return false, fmt.Errorf("phase %s is not query prewarming", s.spec.backends[index])
	}
	if s.active.measuring {
		return false, nil
	}
	if !s.active.prewarming {
		return false, fmt.Errorf("phase %s is not query prewarming", s.spec.backends[index])
	}
	if time.Now().Before(s.active.prewarmDeadline) {
		return false, fmt.Errorf("phase %s reached its measurement boundary before query prewarm expired", s.spec.backends[index])
	}
	if s.active.measurementStarting {
		return false, nil
	}
	s.active.measurementStarting = true
	s.signalLocked()
	return true, nil
}

func (s *phaseState) startMeasurement(index int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if index != s.current || !s.active.prewarming || !s.active.measurementStarting ||
		s.active.measuring || s.active.finalizing || !s.readyLocked() {
		return fmt.Errorf("phase %s is not ready to start measurement", s.spec.backends[index])
	}
	s.active.prewarming = false
	s.active.measurementStarting = false
	s.active.measuring = true
	s.active.deadline = time.Now().Add(s.spec.duration)
	s.active.remaining = s.spec.queryWorkers
	if s.spec.hasUpdater {
		s.active.remaining++
	}
	s.signalLocked()
	return nil
}

func (s *phaseState) waitForTurn(ctx context.Context, index int) error {
	for {
		s.mu.Lock()
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return err
		}
		if index < s.current {
			s.mu.Unlock()
			return fmt.Errorf("phase %s has already completed", s.spec.backends[index])
		}
		if index == s.current {
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *phaseState) waitForMeasurement(ctx context.Context, index int) (time.Time, error) {
	for {
		s.mu.Lock()
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return time.Time{}, err
		}
		if index != s.current {
			s.mu.Unlock()
			return time.Time{}, fmt.Errorf("phase %s advanced before measurement", s.spec.backends[index])
		}
		if s.active.measuring {
			deadline := s.active.deadline
			s.mu.Unlock()
			return deadline, nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		}
	}
}

func (s *phaseState) waitForCollector(ctx context.Context, index int, until time.Time) error {
	for {
		s.mu.Lock()
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return err
		}
		if index != s.current || s.active.measuring || s.active.finalizing {
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()

		delay := time.Until(until)
		if delay <= 0 {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-changed:
			timer.Stop()
		case <-timer.C:
			return nil
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

func (s *phaseState) measurementDeadline(alias string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	index, ok := s.spec.positions[alias]
	if !ok || index != s.current || !s.active.measuring {
		return time.Time{}, false
	}
	return s.active.deadline, true
}

func (s *phaseState) queryPhase(alias string) (time.Time, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	index, ok := s.spec.positions[alias]
	if !ok {
		return time.Time{}, false, false
	}
	if index != s.current {
		return time.Time{}, false, true
	}
	if s.active.prewarming {
		return s.active.prewarmDeadline.Add(prewarmQueryCancelGrace), false, true
	}
	if s.active.measuring {
		return s.active.deadline, true, true
	}
	return time.Time{}, false, true
}

func (s *phaseState) takeIteration(index int) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return 0, s.err
	}
	if index != s.current || (!s.active.prewarming && !s.active.measuring) {
		return 0, fmt.Errorf("phase %s is not running queries", s.spec.backends[index])
	}
	iteration := s.active.nextIteration
	s.active.nextIteration++
	return iteration, nil
}

func (s *phaseState) finishWorker(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil || index != s.current || s.active.remaining <= 0 {
		return
	}
	s.active.remaining--
	if s.active.remaining == 0 {
		s.active.measuring = false
		s.active.finalizing = true
	}
	s.signalLocked()
}

func (s *phaseState) finishFinal(index int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if index != s.current || !s.active.finalizing {
		return fmt.Errorf("phase %s is not ready for final metrics collection", s.spec.backends[index])
	}
	s.current++
	s.active = activePhase{}
	s.signalLocked()
	return nil
}

func (s *phaseState) done() (bool, error) {
	_, _, _, done, err := s.progress()
	return done, err
}

func (s *phaseState) progress() (int, bool, bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return 0, false, false, false, s.err
	}
	done := s.current == len(s.spec.backends)
	return s.current, !done && s.active.measuring, !done && s.active.finalizing, done, nil
}

func (s *phaseState) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
		s.signalLocked()
	}
}

func (s *phaseState) signalLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func waitUntil(ctx context.Context, deadline time.Time) error {
	delay := time.Until(deadline)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
