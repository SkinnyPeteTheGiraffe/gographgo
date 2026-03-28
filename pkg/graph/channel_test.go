package graph

import (
	"errors"
	"reflect"
	"testing"
)

func d(vals ...any) []Dynamic {
	out := make([]Dynamic, len(vals))
	for i, v := range vals {
		out[i] = Dyn(v)
	}
	return out
}

func mustDynamicSlice(t *testing.T, v Dynamic) []Dynamic {
	t.Helper()
	s, ok := v.Value().([]Dynamic)
	if !ok {
		t.Fatalf("expected []Dynamic, got %T", v.Value())
	}
	return s
}

// --- LastValue ---

func TestLastValue_BasicRoundTrip(t *testing.T) {
	c := NewLastValue()
	if c.IsAvailable() {
		t.Fatal("expected empty on creation")
	}
	if _, err := c.Get(); !errors.As(err, new(*EmptyChannelError)) {
		t.Fatalf("expected EmptyChannelError, got %v", err)
	}

	changed, err := c.Update(d(42))
	if err != nil || !changed {
		t.Fatalf("Update failed: changed=%v err=%v", changed, err)
	}
	v, err := c.Get()
	if err != nil || v.Value() != 42 {
		t.Fatalf("Get: want 42, got %v %v", v, err)
	}
}

func TestLastValue_EmptyUpdateNoOp(t *testing.T) {
	c := NewLastValue()
	changed, err := c.Update(nil)
	if err != nil || changed {
		t.Fatalf("empty update should be no-op: changed=%v err=%v", changed, err)
	}
}

func TestLastValue_MultipleWritesError(t *testing.T) {
	c := NewLastValue()
	_, err := c.Update(d(1, 2))
	if err == nil {
		t.Fatal("expected InvalidUpdateError for 2 writes")
	}
	var iue *InvalidUpdateError
	if !errors.As(err, &iue) {
		t.Fatalf("expected InvalidUpdateError, got %T: %v", err, err)
	}
}

func TestLastValue_Overwrite(t *testing.T) {
	c := NewLastValue()
	c.Update(d("first"))  //nolint:errcheck // test setup intentionally ignores update error
	c.Update(d("second")) //nolint:errcheck // test setup intentionally ignores update error
	v, _ := c.Get()
	if v.Value() != "second" {
		t.Fatalf("want 'second', got %v", v)
	}
}

func TestLastValue_CheckpointRestore(t *testing.T) {
	c := NewLastValue()
	c.Update(d("hello")) //nolint:errcheck // test setup intentionally ignores update error

	snap, ok := c.Checkpoint()
	if !ok || snap.Value() != "hello" {
		t.Fatalf("Checkpoint: want ('hello', true), got (%v, %v)", snap, ok)
	}

	c2, err := c.FromCheckpoint(snap)
	if err != nil {
		t.Fatalf("FromCheckpoint error: %v", err)
	}
	v, _ := c2.Get()
	if v.Value() != "hello" {
		t.Fatalf("restored value: want 'hello', got %v", v)
	}
}

func TestLastValue_CheckpointEmpty(t *testing.T) {
	c := NewLastValue()
	_, ok := c.Checkpoint()
	if ok {
		t.Fatal("empty channel should checkpoint as false")
	}
}

func TestLastValue_Copy(t *testing.T) {
	c := NewLastValue()
	c.Update(d(99)) //nolint:errcheck // test setup intentionally ignores update error
	c2 := c.Copy()
	// Mutating original doesn't affect copy.
	c.Update(d(100)) //nolint:errcheck // test setup intentionally ignores update error
	v, _ := c2.Get()
	if v.Value() != 99 {
		t.Fatalf("copy should be independent, got %v", v)
	}
}

func TestLastValue_ConsumeFinishNoOp(t *testing.T) {
	c := NewLastValue()
	if c.Consume() || c.Finish() {
		t.Fatal("Consume/Finish should be no-ops")
	}
}

func TestLastValueAfterFinish_RequiresFinish(t *testing.T) {
	c := NewLastValueAfterFinish()

	if _, err := c.Get(); !errors.As(err, new(*EmptyChannelError)) {
		t.Fatalf("expected EmptyChannelError before update, got %v", err)
	}

	changed, err := c.Update(d("ready"))
	if err != nil || !changed {
		t.Fatalf("Update failed: changed=%v err=%v", changed, err)
	}
	if c.IsAvailable() {
		t.Fatal("channel should not be available before Finish")
	}

	if !c.Finish() {
		t.Fatal("Finish should make channel available")
	}
	v, err := c.Get()
	if err != nil || v.Value() != "ready" {
		t.Fatalf("Get after Finish: want 'ready', got %v %v", v, err)
	}
}

func TestLastValueAfterFinish_ConsumeAndCheckpoint(t *testing.T) {
	c := NewLastValueAfterFinish()
	c.Update(d(123)) //nolint:errcheck // test setup intentionally ignores update error
	c.Finish()

	snap, ok := c.Checkpoint()
	if !ok {
		t.Fatal("expected checkpoint data")
	}

	restored, err := c.FromCheckpoint(snap)
	if err != nil {
		t.Fatalf("FromCheckpoint: %v", err)
	}
	v, err := restored.Get()
	if err != nil || v.Value() != 123 {
		t.Fatalf("restored Get: want 123, got %v %v", v, err)
	}

	if !restored.Consume() {
		t.Fatal("Consume should clear finished value")
	}
	if restored.IsAvailable() {
		t.Fatal("channel should be empty after Consume")
	}
}

// --- AnyValue ---

func TestAnyValue_BasicRoundTrip(t *testing.T) {
	c := NewAnyValue()
	if c.IsAvailable() {
		t.Fatal("expected empty on creation")
	}

	changed, err := c.Update(d("x"))
	if err != nil || !changed {
		t.Fatalf("Update failed: changed=%v err=%v", changed, err)
	}
	v, err := c.Get()
	if err != nil || v.Value() != "x" {
		t.Fatalf("Get: want 'x', got %v %v", v, err)
	}
}

func TestAnyValue_MultipleWritesTakesLast(t *testing.T) {
	c := NewAnyValue()
	changed, err := c.Update(d("a", "b", "c"))
	if err != nil || !changed {
		t.Fatalf("multi-write should succeed: %v %v", changed, err)
	}
	v, _ := c.Get()
	if v.Value() != "c" {
		t.Fatalf("want 'c' (last), got %v", v)
	}
}

func TestAnyValue_EmptyUpdateClearsValue(t *testing.T) {
	c := NewAnyValue()
	c.Update(d("set")) //nolint:errcheck // test setup intentionally ignores update error

	changed, err := c.Update(nil)
	if err != nil || !changed {
		t.Fatalf("empty update on set channel should clear: changed=%v err=%v", changed, err)
	}
	if c.IsAvailable() {
		t.Fatal("channel should be empty after empty update")
	}
}

func TestAnyValue_EmptyUpdateOnEmptyNoOp(t *testing.T) {
	c := NewAnyValue()
	changed, err := c.Update(nil)
	if err != nil || changed {
		t.Fatalf("empty update on empty channel should be no-op: changed=%v err=%v", changed, err)
	}
}

func TestAnyValue_CheckpointRestore(t *testing.T) {
	c := NewAnyValue()
	c.Update(d(7)) //nolint:errcheck // test setup intentionally ignores update error

	snap, ok := c.Checkpoint()
	if !ok || snap.Value() != 7 {
		t.Fatalf("Checkpoint: want (7, true), got (%v, %v)", snap, ok)
	}

	c2, _ := c.FromCheckpoint(snap)
	v, _ := c2.Get()
	if v.Value() != 7 {
		t.Fatalf("restored: want 7, got %v", v)
	}
}

// --- BinaryOperatorAggregate ---

func TestBinOp_Accumulate(t *testing.T) {
	add := func(a, b any) any { return a.(int) + b.(int) }
	c := NewBinaryOperatorAggregate(add)

	c.Update(d(1, 2, 3)) //nolint:errcheck // test setup intentionally ignores update error
	v, _ := c.Get()
	if v.Value() != 6 {
		t.Fatalf("want 6, got %v", v)
	}
}

func TestBinOp_EmptyUpdateNoOp(t *testing.T) {
	add := func(a, b any) any { return a.(int) + b.(int) }
	c := NewBinaryOperatorAggregate(add)
	c.Update(d(5)) //nolint:errcheck // test setup intentionally ignores update error

	changed, err := c.Update(nil)
	if err != nil || changed {
		t.Fatalf("empty update should be no-op: %v %v", changed, err)
	}
	v, _ := c.Get()
	if v.Value() != 5 {
		t.Fatalf("value unchanged: want 5, got %v", v)
	}
}

func TestBinOp_Overwrite(t *testing.T) {
	add := func(a, b any) any { return a.(int) + b.(int) }
	c := NewBinaryOperatorAggregate(add)
	c.Update(d(10)) //nolint:errcheck // test setup intentionally ignores update error

	_, err := c.Update(d(Overwrite{Value: 99}))
	if err != nil {
		t.Fatalf("Overwrite failed: %v", err)
	}
	v, _ := c.Get()
	if v.Value() != 99 {
		t.Fatalf("want 99 after Overwrite, got %v", v)
	}
}

func TestBinOp_OverwriteSuppressesLaterWritesInStep(t *testing.T) {
	add := func(a, b any) any { return a.(int) + b.(int) }
	c := NewBinaryOperatorAggregate(add)
	c.Update(d(10)) //nolint:errcheck // test setup intentionally ignores update error

	_, err := c.Update(d(Overwrite{Value: 7}, 3, 4))
	if err != nil {
		t.Fatalf("Overwrite update failed: %v", err)
	}
	v, _ := c.Get()
	if v.Value() != 7 {
		t.Fatalf("want 7 after overwrite-suppressed writes, got %v", v)
	}
}

func TestBinOp_DoubleOverwriteErrors(t *testing.T) {
	add := func(a, b any) any { return a.(int) + b.(int) }
	c := NewBinaryOperatorAggregate(add)
	_, err := c.Update(d(Overwrite{Value: 1}, Overwrite{Value: 2}))
	if err == nil {
		t.Fatal("expected error for two Overwrite values")
	}
}

func TestBinOp_DictOverwrite(t *testing.T) {
	add := func(a, b any) any { return a.(int) + b.(int) }
	c := NewBinaryOperatorAggregate(add)
	c.Update(d(10)) //nolint:errcheck // test setup intentionally ignores update error

	_, err := c.Update(d(map[string]any{"__overwrite__": 7}))
	if err != nil {
		t.Fatalf("dict overwrite failed: %v", err)
	}
	v, _ := c.Get()
	if v.Value() != 7 {
		t.Fatalf("want 7 after dict overwrite, got %v", v)
	}
}

func TestBinOp_DictOverwriteLegacyKeyStillSupported(t *testing.T) {
	add := func(a, b any) any { return a.(int) + b.(int) }
	c := NewBinaryOperatorAggregate(add)
	c.Update(d(10)) //nolint:errcheck // test setup intentionally ignores update error

	_, err := c.Update(d(map[string]any{"__OVERWRITE__": 6}))
	if err != nil {
		t.Fatalf("legacy dict overwrite failed: %v", err)
	}
	v, _ := c.Get()
	if v.Value() != 6 {
		t.Fatalf("want 6 after legacy dict overwrite, got %v", v)
	}
}

func TestBinOp_DoubleOverwriteStructAndDictErrors(t *testing.T) {
	add := func(a, b any) any { return a.(int) + b.(int) }
	c := NewBinaryOperatorAggregate(add)
	_, err := c.Update(d(Overwrite{Value: 1}, map[string]any{"__overwrite__": 2}))
	if err == nil {
		t.Fatal("expected error for two overwrite values")
	}
}

func TestBinOp_WithTypeInitializesZeroValue(t *testing.T) {
	appendInts := func(a, b any) any {
		left := a.([]int)
		right := b.([]int)
		return append(left, right...)
	}
	c := NewBinaryOperatorAggregateWithType(reflect.TypeOf([]int{}), appendInts)
	v, err := c.Get()
	if err != nil {
		t.Fatalf("expected initialized zero value, got error: %v", err)
	}
	initial, ok := v.Value().([]int)
	if !ok {
		t.Fatalf("expected []int zero value, got %T", v.Value())
	}
	if len(initial) != 0 {
		t.Fatalf("expected empty []int zero value, got %#v", initial)
	}

	c.Update(d([]int{1, 2})) //nolint:errcheck // test setup intentionally ignores update error
	v, _ = c.Get()
	if !reflect.DeepEqual(v.Value(), []int{1, 2}) {
		t.Fatalf("expected accumulated []int{1,2}, got %#v", v.Value())
	}
}

func TestBinOp_CheckpointRestore(t *testing.T) {
	add := func(a, b any) any { return a.(int) + b.(int) }
	c := NewBinaryOperatorAggregate(add)
	c.Update(d(3, 4)) //nolint:errcheck // test setup intentionally ignores update error

	snap, ok := c.Checkpoint()
	if !ok || snap.Value() != 7 {
		t.Fatalf("Checkpoint: want (7, true), got (%v, %v)", snap, ok)
	}

	c2, _ := c.FromCheckpoint(snap)
	c2.Update(d(1)) //nolint:errcheck // test setup intentionally ignores update error
	v, _ := c2.Get()
	if v.Value() != 8 {
		t.Fatalf("restored + 1: want 8, got %v", v)
	}
}

// --- Topic ---

func TestTopic_BasicAccumulate(t *testing.T) {
	c := NewTopic(false)
	c.Update(d("a", "b")) //nolint:errcheck // test setup intentionally ignores update error
	v, err := c.Get()
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	dynItems := mustDynamicSlice(t, v)
	items := make([]any, len(dynItems))
	for i := range dynItems {
		items[i] = dynItems[i].Value()
	}
	if len(items) != 2 || items[0] != "a" || items[1] != "b" {
		t.Fatalf("unexpected items: %v", items)
	}
}

func TestTopic_ClearsOnNextUpdate(t *testing.T) {
	c := NewTopic(false)
	c.Update(d("a"))      //nolint:errcheck // test setup intentionally ignores update error
	c.Update(d("b", "c")) //nolint:errcheck // test setup intentionally ignores update error
	v, _ := c.Get()
	dynItems := mustDynamicSlice(t, v)
	items := make([]any, len(dynItems))
	for i := range dynItems {
		items[i] = dynItems[i].Value()
	}
	if len(items) != 2 {
		t.Fatalf("want 2 items after second update, got %v", items)
	}
}

func TestTopic_ConsumeNoOp(t *testing.T) {
	c := NewTopic(false)
	c.Update(d("a", "b")) //nolint:errcheck // test setup intentionally ignores update error
	if c.Consume() {
		t.Fatal("Consume should be a no-op")
	}
	v, err := c.Get()
	if err != nil {
		t.Fatalf("Get after Consume should still succeed: %v", err)
	}
	dynItems := mustDynamicSlice(t, v)
	if len(dynItems) != 2 || dynItems[0].Value() != "a" || dynItems[1].Value() != "b" {
		t.Fatalf("Topic values should remain unchanged after Consume, got %v", dynItems)
	}
}

func TestTopic_AccumulateMode(t *testing.T) {
	c := NewTopic(true)
	c.Update(d("a")) //nolint:errcheck // test setup intentionally ignores update error
	c.Update(d("b")) //nolint:errcheck // test setup intentionally ignores update error
	v, _ := c.Get()
	dynItems := mustDynamicSlice(t, v)
	items := make([]any, len(dynItems))
	for i := range dynItems {
		items[i] = dynItems[i].Value()
	}
	if len(items) != 2 {
		t.Fatalf("accumulate mode: want 2 items, got %v", items)
	}
}

func TestTopic_FlattenSlice(t *testing.T) {
	c := NewTopic(false)
	c.Update(d([]Dynamic{Dyn("x"), Dyn("y")}, "z")) //nolint:errcheck // test setup intentionally ignores update error
	v, _ := c.Get()
	dynItems := mustDynamicSlice(t, v)
	items := make([]any, len(dynItems))
	for i := range dynItems {
		items[i] = dynItems[i].Value()
	}
	if len(items) != 3 {
		t.Fatalf("want 3 after flatten, got %v", items)
	}
}

func TestTopic_EmptyRaisesError(t *testing.T) {
	c := NewTopic(false)
	if _, err := c.Get(); !errors.As(err, new(*EmptyChannelError)) {
		t.Fatalf("expected EmptyChannelError, got %v", err)
	}
}

func TestTopic_CheckpointRestore(t *testing.T) {
	c := NewTopic(false)
	c.Update(d("p", "q")) //nolint:errcheck // test setup intentionally ignores update error
	snap, ok := c.Checkpoint()
	if !ok {
		t.Fatal("expected checkpoint data")
	}
	c2, _ := c.FromCheckpoint(snap)
	v, _ := c2.Get()
	dynItems := mustDynamicSlice(t, v)
	items := make([]any, len(dynItems))
	for i := range dynItems {
		items[i] = dynItems[i].Value()
	}
	if len(items) != 2 {
		t.Fatalf("restored: want 2 items, got %v", items)
	}
}

func TestTopic_CheckpointLegacyTupleRestore(t *testing.T) {
	c := NewTopic(false)
	legacy := Dyn([]any{"legacy", []any{"p", "q"}})

	c2, err := c.FromCheckpoint(legacy)
	if err != nil {
		t.Fatalf("FromCheckpoint should handle legacy tuple form: %v", err)
	}
	v, err := c2.Get()
	if err != nil {
		t.Fatalf("Get after legacy restore: %v", err)
	}
	dynItems := mustDynamicSlice(t, v)
	items := make([]any, len(dynItems))
	for i := range dynItems {
		items[i] = dynItems[i].Value()
	}
	if len(items) != 2 || items[0] != "p" || items[1] != "q" {
		t.Fatalf("unexpected restored legacy tuple values: %v", items)
	}
}

// --- EphemeralValue ---

func TestEphemeral_BasicRoundTrip(t *testing.T) {
	c := NewEphemeralValue(true)
	c.Update(d("eph")) //nolint:errcheck // test setup intentionally ignores update error
	v, _ := c.Get()
	if v.Value() != "eph" {
		t.Fatalf("want 'eph', got %v", v)
	}
}

func TestEphemeral_EmptyUpdateClears(t *testing.T) {
	c := NewEphemeralValue(true)
	c.Update(d("x")) //nolint:errcheck // test setup intentionally ignores update error

	changed, _ := c.Update(nil)
	if !changed || c.IsAvailable() {
		t.Fatal("empty update should clear value")
	}
}

func TestEphemeral_GuardEnforced(t *testing.T) {
	c := NewEphemeralValue(true)
	_, err := c.Update(d(1, 2))
	if err == nil {
		t.Fatal("guard=true should reject 2 writes")
	}
}

func TestEphemeral_GuardOff(t *testing.T) {
	c := NewEphemeralValue(false)
	_, err := c.Update(d(1, 2, 3))
	if err != nil {
		t.Fatalf("guard=false should allow multiple writes: %v", err)
	}
	v, _ := c.Get()
	if v.Value() != 3 {
		t.Fatalf("want last value 3, got %v", v)
	}
}

func TestEphemeral_CheckpointRestore(t *testing.T) {
	c := NewEphemeralValue(true)
	c.Update(d("snap")) //nolint:errcheck // test setup intentionally ignores update error
	snap, ok := c.Checkpoint()
	if !ok {
		t.Fatal("expected checkpoint data")
	}
	c2, _ := c.FromCheckpoint(snap)
	v, _ := c2.Get()
	if v.Value() != "snap" {
		t.Fatalf("restored: want 'snap', got %v", v)
	}
}

// --- NamedBarrierValue ---

func TestBarrier_NotAvailableUntilAllSeen(t *testing.T) {
	c := NewNamedBarrierValue([]string{"a", "b", "c"})
	if c.IsAvailable() {
		t.Fatal("should not be available initially")
	}

	c.Update(d("a", "b")) //nolint:errcheck // test setup intentionally ignores update error
	if c.IsAvailable() {
		t.Fatal("should not be available with only 2 of 3")
	}

	c.Update(d("c")) //nolint:errcheck // test setup intentionally ignores update error
	if !c.IsAvailable() {
		t.Fatal("should be available after all 3 seen")
	}

	v, err := c.Get()
	if err != nil || v != (Dynamic{}) {
		t.Fatalf("Get after barrier: want (nil, nil), got (%v, %v)", v, err)
	}
}

func TestBarrier_ConsumeResets(t *testing.T) {
	c := NewNamedBarrierValue([]string{"x"})
	c.Update(d("x")) //nolint:errcheck // test setup intentionally ignores update error
	if !c.Consume() {
		t.Fatal("Consume should return true when barrier satisfied")
	}
	if c.IsAvailable() {
		t.Fatal("should be reset after Consume")
	}
}

func TestBarrier_UnknownSourceErrors(t *testing.T) {
	c := NewNamedBarrierValue([]string{"a"})
	_, err := c.Update(d("b"))
	if err == nil {
		t.Fatal("expected error for unknown source")
	}
}

func TestBarrier_CheckpointRestore(t *testing.T) {
	c := NewNamedBarrierValue([]string{"a", "b"})
	c.Update(d("a")) //nolint:errcheck // test setup intentionally ignores update error
	snap, ok := c.Checkpoint()
	if !ok {
		t.Fatal("expected checkpoint data")
	}
	c2, _ := c.FromCheckpoint(snap)
	// "a" is already seen in c2; adding "b" should satisfy it.
	c2.Update(d("b")) //nolint:errcheck // test setup intentionally ignores update error
	if !c2.IsAvailable() {
		t.Fatal("barrier should be satisfied after restore + 'b'")
	}
}

func TestBarrier_AllowsNonStringComparableValues(t *testing.T) {
	c := NewNamedBarrierValue([]int{1, 2})

	c.Update(d(1)) //nolint:errcheck // test setup intentionally ignores update error
	if c.IsAvailable() {
		t.Fatal("should not be available with only one value")
	}

	c.Update(d(2)) //nolint:errcheck // test setup intentionally ignores update error
	if !c.IsAvailable() {
		t.Fatal("should be available after all expected int values are seen")
	}
}

func TestBarrier_UnexpectedTypeErrors(t *testing.T) {
	c := NewNamedBarrierValue([]int{1})
	_, err := c.Update(d("1"))
	if err == nil {
		t.Fatal("expected type mismatch error")
	}
}

// --- NamedBarrierValueAfterFinish ---

func TestBarrierAfterFinish_RequiresFinish(t *testing.T) {
	c := NewNamedBarrierValueAfterFinish([]string{"a"})
	c.Update(d("a")) //nolint:errcheck // test setup intentionally ignores update error
	if c.IsAvailable() {
		t.Fatal("should not be available before Finish()")
	}
	if !c.Finish() {
		t.Fatal("Finish should return true")
	}
	if !c.IsAvailable() {
		t.Fatal("should be available after Finish()")
	}
}

func TestBarrierAfterFinish_ConsumeResets(t *testing.T) {
	c := NewNamedBarrierValueAfterFinish([]string{"a"})
	c.Update(d("a")) //nolint:errcheck // test setup intentionally ignores update error
	c.Finish()
	if !c.Consume() {
		t.Fatal("Consume should return true")
	}
	if c.IsAvailable() {
		t.Fatal("should be reset after Consume")
	}
}

func TestBarrierAfterFinish_AllowsNonStringComparableValues(t *testing.T) {
	c := NewNamedBarrierValueAfterFinish([]int{1})
	c.Update(d(1)) //nolint:errcheck // test setup intentionally ignores update error
	if c.IsAvailable() {
		t.Fatal("should not be available before Finish()")
	}
	if !c.Finish() {
		t.Fatal("Finish should return true")
	}
	if !c.IsAvailable() {
		t.Fatal("should be available after Finish()")
	}
}

// --- UntrackedValue ---

func TestUntracked_BasicRoundTrip(t *testing.T) {
	c := NewUntrackedValue(true)
	c.Update(d("u")) //nolint:errcheck // test setup intentionally ignores update error
	v, _ := c.Get()
	if v.Value() != "u" {
		t.Fatalf("want 'u', got %v", v)
	}
}

func TestUntracked_NeverCheckpointed(t *testing.T) {
	c := NewUntrackedValue(true)
	c.Update(d("val")) //nolint:errcheck // test setup intentionally ignores update error
	_, ok := c.Checkpoint()
	if ok {
		t.Fatal("UntrackedValue should never checkpoint")
	}
}

func TestUntracked_FromCheckpointReturnsEmpty(t *testing.T) {
	c := NewUntrackedValue(true)
	c.Update(d("val")) //nolint:errcheck // test setup intentionally ignores update error
	c2, _ := c.FromCheckpoint(Dyn("anything"))
	if c2.IsAvailable() {
		t.Fatal("restored UntrackedValue should be empty")
	}
}

func TestUntracked_GuardEnforced(t *testing.T) {
	c := NewUntrackedValue(true)
	_, err := c.Update(d(1, 2))
	if err == nil {
		t.Fatal("guard=true should reject 2 writes")
	}
}

func TestChannelTypeMetadata(t *testing.T) {
	if ChannelValueType(NewLastValue()) != anyReflectType {
		t.Fatal("LastValue should expose any type metadata")
	}
	if ChannelUpdateType(NewTopic(false)) != anyReflectType {
		t.Fatal("Topic update type should default to any")
	}
	if got := ChannelValueType(NewNamedBarrierValue([]int{1})); got != reflect.TypeFor[int]() {
		t.Fatalf("NamedBarrierValue should expose int value type, got %v", got)
	}
}

func TestChannelsEqual(t *testing.T) {
	a := NewTopic(true)
	b := NewTopic(true)
	a.Update(d("x")) //nolint:errcheck // test setup intentionally ignores update error
	b.Update(d("x")) //nolint:errcheck // test setup intentionally ignores update error
	if !ChannelsEqual(a, b) {
		t.Fatal("expected equal channels with same configuration and values")
	}
	b.Update(d("y")) //nolint:errcheck // test setup intentionally ignores update error
	if ChannelsEqual(a, b) {
		t.Fatal("expected unequal channels after state diverges")
	}
}
