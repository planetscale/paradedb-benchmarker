package search

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/lib"
)

func testPhaseState(backends []string, workers int, updates bool) *phaseState {
	positions := make(map[string]int, len(backends))
	for index, backend := range backends {
		positions[backend] = index
	}
	return &phaseState{
		changed: make(chan struct{}),
		spec: phaseSpec{
			backends:     backends,
			positions:    positions,
			duration:     time.Second,
			queryWorkers: workers,
			hasUpdater:   updates,
		},
	}
}

func startTestMeasurement(state *phaseState, index int) error {
	if err := state.startPrewarm(index); err != nil {
		return err
	}
	starter, err := state.claimMeasurementStart(index)
	if err != nil {
		return err
	}
	if !starter {
		return fmt.Errorf("phase %d did not elect a measurement starter", index)
	}
	return state.startMeasurement(index)
}

func TestPhaseStateWarmsAndMeasuresBackendsInOrder(t *testing.T) {
	state := testPhaseState([]string{"paradedb", "custom"}, 2, false)
	ctx := context.Background()

	tinArrived := make(chan bool, 1)
	go func() {
		leader, err := state.arriveQuery(ctx, 1, 2)
		if err != nil {
			tinArrived <- false
			return
		}
		tinArrived <- leader
	}()

	select {
	case <-tinArrived:
		t.Fatal("custom phase arrived before ParadeDB completed")
	case <-time.After(10 * time.Millisecond):
	}

	leader, err := state.arriveQuery(ctx, 0, 2)
	if err != nil || !leader {
		t.Fatalf("first ParadeDB worker = leader %v, error %v", leader, err)
	}
	leader, err = state.arriveQuery(ctx, 0, 2)
	if err != nil || leader {
		t.Fatalf("second ParadeDB worker = leader %v, error %v", leader, err)
	}
	if err := state.waitForWarm(ctx, 0); err != nil {
		t.Fatalf("wait for ParadeDB warm-up: %v", err)
	}
	for want := uint64(0); want < 2; want++ {
		iteration, ok, err := state.takeWarmIteration(0)
		if err != nil || !ok || iteration != want {
			t.Fatalf("warm iteration = %d, %v, error %v; want %d, true", iteration, ok, err, want)
		}
		if _, _, err := state.completeWarmIteration(0); err != nil {
			t.Fatalf("complete warm iteration: %v", err)
		}
	}
	state.finishWarmWorker(0)
	state.finishWarmWorker(0)
	if err := startTestMeasurement(state, 0); err != nil {
		t.Fatalf("start ParadeDB measurement: %v", err)
	}

	deadline, err := state.waitForMeasurement(ctx, 0)
	if err != nil {
		t.Fatalf("wait for ParadeDB measurement: %v", err)
	}
	if !deadline.After(time.Now()) {
		t.Fatalf("measurement deadline %v is not in the future", deadline)
	}
	first, err := state.takeIteration(0)
	if err != nil {
		t.Fatalf("first iteration: %v", err)
	}
	second, err := state.takeIteration(0)
	if err != nil {
		t.Fatalf("second iteration: %v", err)
	}
	if first != 0 || second != 1 {
		t.Fatalf("iterations = %d, %d; want 0, 1", first, second)
	}

	state.finishWorker(0)
	select {
	case <-tinArrived:
		t.Fatal("custom phase arrived while one ParadeDB worker remained")
	case <-time.After(10 * time.Millisecond):
	}
	state.finishWorker(0)
	select {
	case <-tinArrived:
		t.Fatal("custom phase arrived before ParadeDB final metrics completed")
	case <-time.After(10 * time.Millisecond):
	}
	if err := state.finishFinal(0); err != nil {
		t.Fatalf("finish ParadeDB metrics: %v", err)
	}

	select {
	case leader := <-tinArrived:
		if !leader {
			t.Fatal("first Custom worker did not claim warmup")
		}
	case <-time.After(time.Second):
		t.Fatal("custom phase remained blocked after ParadeDB completed")
	}
}

func TestPhaseStateWaitsForUpdaterBeforeStartingMeasurement(t *testing.T) {
	state := testPhaseState([]string{"custom"}, 1, true)
	ctx := context.Background()

	leader, err := state.arriveQuery(ctx, 0, 0)
	if err != nil || !leader {
		t.Fatalf("query arrival = leader %v, error %v", leader, err)
	}
	if err := state.waitForWarm(ctx, 0); err != nil {
		t.Fatalf("wait for warm-up: %v", err)
	}
	state.finishWarmWorker(0)

	ready := make(chan error, 1)
	go func() {
		ready <- state.waitForReady(ctx, 0)
	}()
	select {
	case <-ready:
		t.Fatal("measurement boundary became ready before updater arrived")
	case <-time.After(10 * time.Millisecond):
	}

	if err := state.markUpdaterReady(0); err != nil {
		t.Fatalf("updater arrival: %v", err)
	}
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("wait for measurement readiness: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("measurement boundary remained blocked after updater arrived")
	}
	if err := startTestMeasurement(state, 0); err != nil {
		t.Fatalf("start measurement: %v", err)
	}

	state.finishWorker(0)
	done, err := state.done()
	if err != nil {
		t.Fatalf("done after query worker: %v", err)
	}
	if done {
		t.Fatal("phase completed before updater worker finished")
	}
	state.finishWorker(0)
	done, err = state.done()
	if err != nil || done {
		t.Fatalf("done before final metrics = %v, error %v", done, err)
	}
	if err := state.finishFinal(0); err != nil {
		t.Fatalf("finish final metrics: %v", err)
	}
	done, err = state.done()
	if err != nil || !done {
		t.Fatalf("done after final metrics = %v, error %v", done, err)
	}
}

func TestPhaseStateWaitsForEveryWorkerBeforeWarmup(t *testing.T) {
	state := testPhaseState([]string{"custom"}, 2, false)
	ctx := context.Background()
	if leader, err := state.arriveQuery(ctx, 0, 2); err != nil || !leader {
		t.Fatalf("first arrival = leader %v, error %v", leader, err)
	}

	warm := make(chan error, 1)
	go func() { warm <- state.waitForWarm(ctx, 0) }()
	select {
	case <-warm:
		t.Fatal("warm-up began before second worker arrived")
	case <-time.After(10 * time.Millisecond):
	}
	if leader, err := state.arriveQuery(ctx, 0, 2); err != nil || leader {
		t.Fatalf("second arrival = leader %v, error %v", leader, err)
	}
	select {
	case err := <-warm:
		if err != nil {
			t.Fatalf("wait for warm-up: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("warm-up remained blocked after every worker arrived")
	}
}

func TestPhaseStateRejectsMismatchedWarmIterationCounts(t *testing.T) {
	state := testPhaseState([]string{"custom"}, 2, false)
	ctx := context.Background()
	if _, err := state.arriveQuery(ctx, 0, 10); err != nil {
		t.Fatalf("first arrival: %v", err)
	}
	if _, err := state.arriveQuery(ctx, 0, 11); err == nil {
		t.Fatal("mismatched warm iteration counts were accepted")
	}
}

func TestPhaseStateExposesOnlyTheActiveMeasurementDeadline(t *testing.T) {
	state := testPhaseState([]string{"paradedb", "custom"}, 1, false)
	want := time.Now().Add(time.Minute)
	state.active.measuring = true
	state.active.deadline = want

	if got, ok := state.measurementDeadline("paradedb"); !ok || !got.Equal(want) {
		t.Fatalf("active deadline = %v, %v; want %v, true", got, ok, want)
	}
	if _, ok := state.measurementDeadline("custom"); ok {
		t.Fatal("future backend exposed the active measurement deadline")
	}
	state.active.measuring = false
	if _, ok := state.measurementDeadline("paradedb"); ok {
		t.Fatal("inactive phase exposed a measurement deadline")
	}
}

func TestPhaseStateWaiterHonorsCancellation(t *testing.T) {
	state := testPhaseState([]string{"paradedb", "custom"}, 1, false)
	state.active.measuring = true
	state.active.remaining = 1
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := state.arriveQuery(ctx, 1, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("arrival error = %v, want context canceled", err)
	}
}

func TestParsePhaseSpecRejectsDuplicateBackends(t *testing.T) {
	_, err := parsePhaseSpec(map[string]interface{}{
		"backends": []interface{}{"custom", "custom"},
		"duration": "1s",
		"vus":      float64(1),
		"updates":  false,
	})
	if err == nil {
		t.Fatal("duplicate backends were accepted")
	}
}

type phaseRuntimeVU struct {
	modules.VU
	runtime *sobek.Runtime
}

func (v phaseRuntimeVU) Runtime() *sobek.Runtime { return v.runtime }
func (phaseRuntimeVU) Context() context.Context  { return context.Background() }
func (phaseRuntimeVU) State() *lib.State         { return nil }

func TestPhaseCoordinatorKeepsEveryVUOnOneContinuousQueryStream(t *testing.T) {
	const workers = 2
	state := testPhaseState([]string{"custom"}, workers, false)
	state.spec.prewarm = 30 * time.Millisecond
	state.spec.duration = 30 * time.Millisecond

	type queryCall struct {
		iteration uint64
		measuring bool
	}
	calls := make([][]queryCall, workers)
	done := make(chan struct{}, workers)
	var boundaryMu sync.Mutex
	var boundary time.Time

	for worker := 0; worker < workers; worker++ {
		worker := worker
		runtime := sobek.New()
		coordinator := &PhaseCoordinator{
			vu:    phaseRuntimeVU{runtime: runtime},
			state: state,
		}
		go func() {
			coordinator.Run(sobek.FunctionCall{Arguments: []sobek.Value{
				runtime.ToValue("custom"),
				runtime.ToValue(0),
				runtime.ToValue(func(uint64) {}),
				runtime.ToValue(func() {
					boundaryMu.Lock()
					boundary = time.Now()
					boundaryMu.Unlock()
				}),
				runtime.ToValue(func(iteration uint64) {
					_, measuring, coordinated := state.queryPhase("custom")
					if !coordinated {
						t.Errorf("worker %d query was not phase coordinated", worker)
					}
					calls[worker] = append(calls[worker], queryCall{
						iteration: iteration,
						measuring: measuring,
					})
					time.Sleep(4 * time.Millisecond)
				}),
			}})
			done <- struct{}{}
		}()
	}

	for worker := 0; worker < workers; worker++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("query worker did not finish")
		}
	}

	boundaryMu.Lock()
	measurementStarted := boundary
	boundaryMu.Unlock()
	if measurementStarted.IsZero() {
		t.Fatal("measurement boundary callback did not run")
	}
	for worker, workerCalls := range calls {
		if len(workerCalls) < 2 {
			t.Fatalf("worker %d query calls = %d, want prewarm and measured work", worker, len(workerCalls))
		}
		seenPrewarm := false
		seenMeasurement := false
		for _, call := range workerCalls {
			if call.measuring {
				seenMeasurement = true
			} else {
				if seenMeasurement {
					t.Fatalf("worker %d returned to prewarm after measurement", worker)
				}
				seenPrewarm = true
			}
		}
		if !seenPrewarm || !seenMeasurement {
			t.Fatalf("worker %d phases = prewarm %v, measurement %v", worker, seenPrewarm, seenMeasurement)
		}
	}

	state.mu.Lock()
	prewarmDeadline := state.active.prewarmDeadline
	measurementDeadline := state.active.deadline
	nextIteration := state.active.nextIteration
	state.mu.Unlock()
	if measurementStarted.Before(prewarmDeadline) {
		t.Fatalf("measurement started at %v before prewarm deadline %v", measurementStarted, prewarmDeadline)
	}
	if got := measurementDeadline.Sub(measurementStarted); got < state.spec.duration {
		t.Fatalf("measured window = %s, want at least %s", got, state.spec.duration)
	}
	var queryCalls int
	for _, workerCalls := range calls {
		queryCalls += len(workerCalls)
	}
	if uint64(queryCalls) != nextIteration {
		t.Fatalf("query cursor = %d after %d calls", nextIteration, queryCalls)
	}
}

func TestPhaseQueryLetsInFlightPrewarmWorkDrainPastBoundary(t *testing.T) {
	state := testPhaseState([]string{"custom"}, 1, false)
	logicalBoundary := time.Now().Add(time.Second)
	state.active.prewarming = true
	state.active.prewarmDeadline = logicalBoundary

	deadline, measuring, coordinated := state.queryPhase("custom")
	if !coordinated || measuring {
		t.Fatalf("prewarm query state = measuring %v, coordinated %v; want false, true", measuring, coordinated)
	}
	if want := logicalBoundary.Add(prewarmQueryCancelGrace); !deadline.Equal(want) {
		t.Fatalf("prewarm query cancellation deadline = %v, want %v", deadline, want)
	}
}

func TestPhaseCoordinatorLetsVUsEnterMeasurementWithoutResynchronizing(t *testing.T) {
	const workers = 2
	state := testPhaseState([]string{"custom"}, workers, false)
	state.spec.prewarm = 10 * time.Millisecond
	state.spec.duration = 100 * time.Millisecond

	firstMeasured := make([]time.Time, workers)
	done := make(chan struct{}, workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		runtime := sobek.New()
		coordinator := &PhaseCoordinator{
			vu:    phaseRuntimeVU{runtime: runtime},
			state: state,
		}
		go func() {
			prewarmCalls := 0
			coordinator.Run(sobek.FunctionCall{Arguments: []sobek.Value{
				runtime.ToValue("custom"),
				runtime.ToValue(0),
				runtime.ToValue(func(uint64) {}),
				runtime.ToValue(func() {}),
				runtime.ToValue(func(uint64) {
					_, measuring, _ := state.queryPhase("custom")
					if measuring {
						if firstMeasured[worker].IsZero() {
							firstMeasured[worker] = time.Now()
						}
						time.Sleep(time.Millisecond)
						return
					}

					prewarmCalls++
					if prewarmCalls == 1 {
						time.Sleep(time.Duration(worker+1) * 25 * time.Millisecond)
					}
				}),
			}})
			done <- struct{}{}
		}()
	}

	for range workers {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("query worker did not finish")
		}
	}

	if firstMeasured[0].IsZero() || firstMeasured[1].IsZero() {
		t.Fatalf("first measured calls = %v, want both workers measured", firstMeasured)
	}
	spread := firstMeasured[1].Sub(firstMeasured[0])
	if spread < 15*time.Millisecond {
		t.Fatalf("workers re-synchronized at measurement boundary: first-call spread = %s", spread)
	}
}

func TestPhaseStateDistributesWarmIterationsExactlyOnce(t *testing.T) {
	const workers = 4
	const iterations = 100
	state := testPhaseState([]string{"custom"}, workers, false)
	ctx := context.Background()
	for worker := 0; worker < workers; worker++ {
		if _, err := state.arriveQuery(ctx, 0, iterations); err != nil {
			t.Fatalf("worker %d arrival: %v", worker, err)
		}
	}

	claimed := make(chan uint64, iterations)
	errors := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			if err := state.waitForWarm(ctx, 0); err != nil {
				errors <- err
				return
			}
			for {
				iteration, ok, err := state.takeWarmIteration(0)
				if err != nil {
					errors <- err
					return
				}
				if !ok {
					break
				}
				claimed <- iteration
				if _, _, err := state.completeWarmIteration(0); err != nil {
					errors <- err
					return
				}
			}
			state.finishWarmWorker(0)
			errors <- nil
		}()
	}

	for worker := 0; worker < workers; worker++ {
		if err := <-errors; err != nil {
			t.Fatalf("warm worker: %v", err)
		}
	}
	close(claimed)
	seen := make([]bool, iterations)
	for iteration := range claimed {
		if iteration >= iterations || seen[iteration] {
			t.Fatalf("warm iteration %d was out of range or duplicated", iteration)
		}
		seen[iteration] = true
	}
	for iteration, completed := range seen {
		if !completed {
			t.Fatalf("warm iteration %d was not claimed", iteration)
		}
	}
	state.mu.Lock()
	ready := state.readyLocked()
	state.mu.Unlock()
	if !ready {
		t.Fatal("phase was not ready after every warm worker completed")
	}
}

func TestPhaseCollectorFinalizesEachBackendBeforeGlobalFinal(t *testing.T) {
	runtime := sobek.New()
	runtime.SetFieldNameMapper(common.FieldNameMapper{})
	state := testPhaseState([]string{"paradedb", "custom"}, 1, false)
	coordinator := &PhaseCoordinator{
		vu:    phaseRuntimeVU{runtime: runtime},
		state: state,
	}
	if err := runtime.Set("phases", coordinator); err != nil {
		t.Fatalf("expose phase coordinator: %v", err)
	}
	events := make([]string, 0, 5)
	if err := runtime.Set("collect", func(alias string) {
		events = append(events, "collect:"+alias)
		state.mu.Lock()
		state.active.measuring = true
		state.active.remaining = 1
		state.mu.Unlock()
		state.finishWorker(state.spec.positions[alias])
	}); err != nil {
		t.Fatalf("set collect callback: %v", err)
	}
	if err := runtime.Set("phaseFinal", func(alias string, hasNext bool) {
		events = append(events, fmt.Sprintf("phase-final:%s:%t", alias, hasNext))
	}); err != nil {
		t.Fatalf("set phase final callback: %v", err)
	}
	if err := runtime.Set("globalFinal", func() {
		events = append(events, "global-final")
	}); err != nil {
		t.Fatalf("set global final callback: %v", err)
	}

	if _, err := runtime.RunString(`phases.runCollector(collect, phaseFinal, globalFinal);`); err != nil {
		t.Fatalf("run phase collector: %v", err)
	}
	want := "collect:paradedb|phase-final:paradedb:true|collect:custom|phase-final:custom:false|global-final"
	if got := strings.Join(events, "|"); got != want {
		t.Fatalf("collector events = %s, want %s", got, want)
	}
}

func TestPhaseCollectorDoesNotReleaseNextBackendUntilFinalCallbackReturns(t *testing.T) {
	runtime := sobek.New()
	state := testPhaseState([]string{"paradedb", "custom"}, 1, false)
	state.active.finalizing = true
	coordinator := &PhaseCoordinator{
		vu:    phaseRuntimeVU{runtime: runtime},
		state: state,
	}

	tinArrived := make(chan error, 1)
	tinEntered := make(chan struct{})
	go func() {
		_, err := state.arriveQuery(context.Background(), 1, 0)
		close(tinEntered)
		tinArrived <- err
	}()
	finalStarted := make(chan struct{})
	releaseFinal := make(chan struct{})
	collectorDone := make(chan struct{})
	go func() {
		coordinator.RunCollector(sobek.FunctionCall{Arguments: []sobek.Value{
			runtime.ToValue(func(alias string, _ bool) {
				if alias == "custom" {
					<-tinEntered
					state.mu.Lock()
					state.active.finalizing = true
					state.signalLocked()
					state.mu.Unlock()
				}
			}),
			runtime.ToValue(func(alias string) {
				if alias == "paradedb" {
					close(finalStarted)
					<-releaseFinal
				}
			}),
			runtime.ToValue(func() {}),
		}})
		close(collectorDone)
	}()

	select {
	case <-finalStarted:
	case <-time.After(time.Second):
		t.Fatal("ParadeDB final callback did not start")
	}
	select {
	case <-tinArrived:
		t.Fatal("Custom phase arrived while ParadeDB final callback was blocked")
	case <-time.After(10 * time.Millisecond):
	}
	close(releaseFinal)
	select {
	case err := <-tinArrived:
		if err != nil {
			t.Fatalf("Custom arrival after final callback: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Custom phase remained blocked after ParadeDB final callback")
	}
	select {
	case <-collectorDone:
	case <-time.After(time.Second):
		t.Fatal("collector did not complete")
	}
}

func TestNextUpdateStartSkipsMissedSlots(t *testing.T) {
	start := time.Unix(0, 0)
	interval := 100 * time.Millisecond
	for _, test := range []struct {
		name      string
		completed time.Time
		want      time.Time
	}{
		{name: "before next slot", completed: start.Add(50 * time.Millisecond), want: start.Add(interval)},
		{name: "on next slot", completed: start.Add(interval), want: start.Add(interval)},
		{name: "just after next slot", completed: start.Add(interval + time.Nanosecond), want: start.Add(2 * interval)},
		{name: "several missed slots", completed: start.Add(350 * time.Millisecond), want: start.Add(4 * interval)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := nextUpdateStart(start, test.completed, interval); !got.Equal(test.want) {
				t.Fatalf("next update start = %s, want %s", got, test.want)
			}
		})
	}
}

func TestPhaseCoordinatorPacesUpdatesFromMeasurementStart(t *testing.T) {
	runtime := sobek.New()
	state := testPhaseState([]string{"custom"}, 1, true)
	state.spec.duration = 260 * time.Millisecond
	state.active.queryArrivals = 1
	state.active.warmWorkers = 1
	coordinator := &PhaseCoordinator{
		vu:    phaseRuntimeVU{runtime: runtime},
		state: state,
	}
	startErr := make(chan error, 1)
	go func() {
		if err := state.waitForReady(context.Background(), 0); err != nil {
			startErr <- err
			return
		}
		startErr <- startTestMeasurement(state, 0)
	}()

	var prewarmStarts []time.Time
	var starts []time.Time
	coordinator.RunUpdates(sobek.FunctionCall{Arguments: []sobek.Value{
		runtime.ToValue("custom"),
		runtime.ToValue(10),
		runtime.ToValue(func() {
			prewarmStarts = append(prewarmStarts, time.Now())
		}),
		runtime.ToValue(func() {
			starts = append(starts, time.Now())
			time.Sleep(50 * time.Millisecond)
		}),
	}})
	if err := <-startErr; err != nil {
		t.Fatalf("start measurement: %v", err)
	}
	if got, want := len(prewarmStarts), updatePrewarmCount; got != want {
		t.Fatalf("prewarm updates = %d, want %d", got, want)
	}
	if gap := prewarmStarts[1].Sub(prewarmStarts[0]); gap < 900*time.Millisecond {
		t.Fatalf("prewarm updates started only %s apart", gap)
	}
	if got, want := len(starts), 3; got != want {
		t.Fatalf("completed updates = %d, want %d", got, want)
	}
	if gap := starts[0].Sub(prewarmStarts[1]); gap < 900*time.Millisecond {
		t.Fatalf("measurement started only %s after the second prewarm update", gap)
	}
	if gap := starts[1].Sub(starts[0]); gap >= 140*time.Millisecond {
		t.Fatalf("updates started %s apart; pacing interval was added after completion", gap)
	}
}
