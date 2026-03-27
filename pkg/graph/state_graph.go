// Package graph provides the core graph framework APIs.
//
// StateGraph is a builder class for constructing state-based graphs.
// Nodes communicate by reading and writing to a shared state.
//
// Example usage:
//
//	builder := graph.NewStateGraph[MyState]()
//	builder.AddNode("node_a", nodeAFunc)
//	builder.AddEdge(graph.Start, "node_a")
//	builder.AddEdge("node_a", graph.End)
//	compiled := builder.Compile()
//	result := compiled.Invoke(ctx, initialState)
package graph

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
)

// StateGraph is a graph builder where nodes communicate by reading and
// writing to a shared state.
//
// Type parameters:
//   - State: The type representing the graph state (TypedDict, dataclass, etc.)
//   - Context: Optional context type for runtime context (use any if not needed)
//     Examples: user_id, db_conn, or any run-scoped immutable data
//   - Input: Input type for the graph (defaults to State if not specified)
//   - Output: Output type from the graph (defaults to State if not specified)
type StateGraph[State, Context, Input, Output any] struct {
	// nodes maps node names to their specifications
	nodes map[string]*NodeSpec[State]

	// edges contains direct edges between nodes
	edges []Edge

	// waitingEdges contains edges that wait for multiple sources
	waitingEdges []WaitingEdge

	// branches contains conditional branches
	branches map[string][]*BranchSpec[State]

	// entryPoint is the starting node
	entryPoint string

	// finishPoints are nodes that end execution
	finishPoints []string

	// channels maps channel names to channel implementations
	channels map[string]Channel

	// managed maps managed value names to their specifications
	managed map[string]ManagedValueSpec

	// reducers maps reducer names to reducer functions used by annotation tags.
	reducers map[string]func(a, b any) any

	// inputSchema is the optional explicit schema descriptor for graph input.
	inputSchema any

	// outputSchema is the optional explicit schema descriptor for graph output.
	outputSchema any

	// contextSchema is the optional explicit schema descriptor for runtime context.
	contextSchema any

	// interruptBefore lists nodes to interrupt before execution
	interruptBefore []string

	// interruptAfter lists nodes to interrupt after execution
	interruptAfter []string

	// compiled indicates if the graph has been compiled
	compiled bool
}

var reservedGraphIdentifiers = map[string]struct{}{
	Start:           {},
	End:             {},
	pregelInput:     {},
	pregelInterrupt: {},
	pregelResume:    {},
	pregelError:     {},
	pregelPush:      {},
	pregelTasks:     {},
}

var reservedNodeNameCharacters = []string{"|", ":"}

// NewStateGraph creates a new StateGraph builder with default type parameters.
// The State type is required; Context defaults to any, Input and Output default to State.
func NewStateGraph[State any]() *StateGraph[State, any, State, State] {
	return &StateGraph[State, any, State, State]{
		nodes:        make(map[string]*NodeSpec[State]),
		edges:        make([]Edge, 0),
		waitingEdges: make([]WaitingEdge, 0),
		branches:     make(map[string][]*BranchSpec[State]),
		channels:     make(map[string]Channel),
		managed:      make(map[string]ManagedValueSpec),
		reducers:     defaultReducerRegistry(),
	}
}

// NewStateGraphWithTypes creates a new StateGraph builder with explicit type parameters.
func NewStateGraphWithTypes[State, Context, Input, Output any]() *StateGraph[State, Context, Input, Output] {
	return &StateGraph[State, Context, Input, Output]{
		nodes:        make(map[string]*NodeSpec[State]),
		edges:        make([]Edge, 0),
		waitingEdges: make([]WaitingEdge, 0),
		branches:     make(map[string][]*BranchSpec[State]),
		channels:     make(map[string]Channel),
		managed:      make(map[string]ManagedValueSpec),
		reducers:     defaultReducerRegistry(),
	}
}

// ManagedValue is an interface for values that are managed by the graph runtime.
// Managed values are computed on-the-fly and are not stored in checkpoints.
// They provide values like "is_last_step" that are computed from the scratchpad.
type ManagedValue interface {
	Get(scratchpad *PregelScratchpad) any
}

// ManagedValueSpec is a type that implements ManagedValue.
type ManagedValueSpec = ManagedValue

// IsManagedValue returns true if the given value is a ManagedValue type.
func IsManagedValue(value any) bool {
	_, ok := value.(ManagedValue)
	return ok
}

// PregelScratchpad provides access to runtime information for managed values.
type PregelScratchpad struct {
	Store         Store
	ChannelValues map[string]any
	Config        *Config
	Step          int
	Stop          int
}

// AddNode adds a typed node to the graph.
//
// For signature inference and injected runtime parameters, use AddNodeFunc.
func (g *StateGraph[State, Context, Input, Output]) AddNode(
	name string,
	fn func(ctx context.Context, state State) (NodeResult, error),
) *StateGraph[State, Context, Input, Output] {
	if err := validateGraphIdentifier("node", name); err != nil {
		panic(err)
	}
	if fn == nil {
		panic(fmt.Sprintf("node '%s' function must not be nil", name))
	}

	if _, exists := g.nodes[name]; exists {
		panic(fmt.Sprintf("Node '%s' already exists", name))
	}

	g.nodes[name] = &NodeSpec[State]{
		Name:        name,
		Fn:          fn,
		InputSchema: stateSchemaOf[State](),
	}

	return g
}

// AddNodeFunc adds a node from a supported function signature and infers the
// node input schema from the function's first non-context parameter.
//
// Supported signatures:
//   - func(ctx context.Context, input T) (NodeResult, error)
//   - func(input T) (NodeResult, error)
//   - func(ctx context.Context, input T, config Config) (NodeResult, error)
//   - func(ctx context.Context, input T, config *Config) (NodeResult, error)
//   - func(ctx context.Context, input T, writer StreamWriter) (NodeResult, error)
//   - func(ctx context.Context, input T, store Store) (NodeResult, error)
//   - func(ctx context.Context, input T, runtime Runtime) (NodeResult, error)
//   - func(ctx context.Context, input T, runtime *Runtime) (NodeResult, error)
//   - func(ctx context.Context, input T, config, writer, store, runtime...) (NodeResult, error)
//   - func(ctx context.Context, input T) (Command, error)
//   - func(input T) (Command, error)
//   - func(ctx context.Context, input T) (*Command, error)
//   - func(input T) (*Command, error)
//
// Additional parameters are injected from context using GetConfig, GetStreamWriter,
// GetStore, and GetRuntime.
func (g *StateGraph[State, Context, Input, Output]) AddNodeFunc(name string, fn any) *StateGraph[State, Context, Input, Output] {
	wrapped, inputSchema, err := wrapNodeFunc[State](fn)
	if err != nil {
		panic(err)
	}
	g.AddNode(name, wrapped)
	g.nodes[name].InputSchema = inputSchema
	return g
}

// NodeInputSchema returns the inferred input schema descriptor for a node.
func (g *StateGraph[State, Context, Input, Output]) NodeInputSchema(name string) (any, bool) {
	node, ok := g.nodes[name]
	if !ok {
		return nil, false
	}
	return node.InputSchema, true
}

// SetNodeRetryPolicy configures one or more retry policies for a node.
func (g *StateGraph[State, Context, Input, Output]) SetNodeRetryPolicy(name string, policies ...RetryPolicy) *StateGraph[State, Context, Input, Output] {
	node, ok := g.nodes[name]
	if !ok {
		panic(fmt.Sprintf("node '%s' not found", name))
	}
	node.RetryPolicy = append([]RetryPolicy(nil), policies...)
	return g
}

// SetNodeCachePolicy configures caching behavior for a node.
func (g *StateGraph[State, Context, Input, Output]) SetNodeCachePolicy(name string, policy *CachePolicy) *StateGraph[State, Context, Input, Output] {
	node, ok := g.nodes[name]
	if !ok {
		panic(fmt.Sprintf("node '%s' not found", name))
	}
	node.CachePolicy = policy
	return g
}

// SetNodeWriters configures per-node ChannelWrite hooks.
func (g *StateGraph[State, Context, Input, Output]) SetNodeWriters(name string, writers ...ChannelWrite[State]) *StateGraph[State, Context, Input, Output] {
	node, ok := g.nodes[name]
	if !ok {
		panic(fmt.Sprintf("node '%s' not found", name))
	}
	node.Writers = append([]ChannelWrite[State](nil), writers...)
	return g
}

// SetNodeSubgraphs annotates a node with attached subgraph names.
func (g *StateGraph[State, Context, Input, Output]) SetNodeSubgraphs(name string, subgraphs ...string) *StateGraph[State, Context, Input, Output] {
	node, ok := g.nodes[name]
	if !ok {
		panic(fmt.Sprintf("node '%s' not found", name))
	}
	node.Subgraphs = append([]string(nil), subgraphs...)
	return g
}

// AddManagedValue registers a managed value available to node state injection.
func (g *StateGraph[State, Context, Input, Output]) AddManagedValue(name string, spec ManagedValueSpec) *StateGraph[State, Context, Input, Output] {
	if err := validateGraphIdentifier("managed value", name); err != nil {
		panic(err)
	}
	if spec == nil {
		panic(fmt.Sprintf("managed value '%s' must not be nil", name))
	}
	g.managed[name] = spec
	return g
}

// RegisterReducer registers a reducer function usable by `graph:"reducer=name"`
// and `langgraph:"reducer=name"` state field tags.
func (g *StateGraph[State, Context, Input, Output]) RegisterReducer(name string, reducer func(a, b any) any) *StateGraph[State, Context, Input, Output] {
	if strings.TrimSpace(name) == "" {
		panic("reducer name must not be empty")
	}
	if reducer == nil {
		panic(fmt.Sprintf("reducer '%s' must not be nil", name))
	}
	g.reducers[name] = reducer
	return g
}

// SetInputSchema sets an explicit input schema descriptor.
func (g *StateGraph[State, Context, Input, Output]) SetInputSchema(schema any) *StateGraph[State, Context, Input, Output] {
	g.inputSchema = schema
	return g
}

// SetOutputSchema sets an explicit output schema descriptor.
func (g *StateGraph[State, Context, Input, Output]) SetOutputSchema(schema any) *StateGraph[State, Context, Input, Output] {
	g.outputSchema = schema
	return g
}

// SetContextSchema sets an explicit runtime context schema descriptor.
func (g *StateGraph[State, Context, Input, Output]) SetContextSchema(schema any) *StateGraph[State, Context, Input, Output] {
	g.contextSchema = schema
	return g
}

// SetInterruptBefore configures default compile-time interrupt-before nodes.
// Compile options override these defaults when provided.
func (g *StateGraph[State, Context, Input, Output]) SetInterruptBefore(nodes ...string) *StateGraph[State, Context, Input, Output] {
	g.interruptBefore = append([]string(nil), nodes...)
	return g
}

// SetInterruptAfter configures default compile-time interrupt-after nodes.
// Compile options override these defaults when provided.
func (g *StateGraph[State, Context, Input, Output]) SetInterruptAfter(nodes ...string) *StateGraph[State, Context, Input, Output] {
	g.interruptAfter = append([]string(nil), nodes...)
	return g
}

// InputSchema returns the configured input schema descriptor.
// If not explicitly configured, it returns the zero value for `Input`.
func (g *StateGraph[State, Context, Input, Output]) InputSchema() any {
	if g.inputSchema != nil {
		return g.inputSchema
	}
	var in Input
	return in
}

// OutputSchema returns the configured output schema descriptor.
// If not explicitly configured, it returns the zero value for `Output`.
func (g *StateGraph[State, Context, Input, Output]) OutputSchema() any {
	if g.outputSchema != nil {
		return g.outputSchema
	}
	var out Output
	return out
}

// ContextSchema returns the configured runtime context schema descriptor.
// If not explicitly configured, it returns the zero value for `Context`.
func (g *StateGraph[State, Context, Input, Output]) ContextSchema() any {
	if g.contextSchema != nil {
		return g.contextSchema
	}
	var c Context
	return c
}

// GetInputJSONSchema returns a JSON schema for the configured input schema.
func (g *StateGraph[State, Context, Input, Output]) GetInputJSONSchema() map[string]any {
	return GetInputJSONSchema(g.InputSchema(), "Input")
}

// GetOutputJSONSchema returns a JSON schema for the configured output schema.
func (g *StateGraph[State, Context, Input, Output]) GetOutputJSONSchema() map[string]any {
	return GetOutputJSONSchema(g.OutputSchema(), "Output")
}

// GetContextJSONSchema returns a JSON schema for the configured context schema.
func (g *StateGraph[State, Context, Input, Output]) GetContextJSONSchema() map[string]any {
	return GetInputJSONSchema(g.ContextSchema(), "Context")
}

// AddEdge adds a directed edge from start to end.
// Use Start for the entry point and End for termination.
func (g *StateGraph[State, Context, Input, Output]) AddEdge(start, end string) *StateGraph[State, Context, Input, Output] {
	if start == End {
		panic("End cannot be a start node")
	}

	if end == Start {
		panic("Start cannot be an end node")
	}

	g.edges = append(g.edges, Edge{Source: start, Target: end})

	if start != Start {
		if node, ok := g.nodes[start]; ok {
			node.Destinations = append(node.Destinations, end)
		}
	}

	return g
}

// AddConditionalEdges adds a conditional branch from the source node.
// The path function determines which node(s) to execute next.
func (g *StateGraph[State, Context, Input, Output]) AddConditionalEdges(
	source string,
	path func(ctx context.Context, state State) (Route, error),
	pathMap map[string]string,
) *StateGraph[State, Context, Input, Output] {
	if path == nil {
		panic("conditional path must not be nil")
	}
	name := branchNameFromPath(path)
	for _, existing := range g.branches[source] {
		if existing.Name == name {
			panic(fmt.Sprintf("branch with name '%s' already exists for node '%s'", name, source))
		}
	}

	branch := &BranchSpec[State]{
		Name:    name,
		Path:    path,
		PathMap: pathMap,
	}

	g.branches[source] = append(g.branches[source], branch)

	if source != Start {
		if node, ok := g.nodes[source]; ok {
			for _, dest := range pathMap {
				node.Destinations = append(node.Destinations, dest)
			}
		}
	}

	return g
}

// AddConditionalEdgesDynamic adds a conditional branch using a flexible route
// return type and optional path map input.
//
// Supported path return values:
//   - Route, *Route
//   - string, []string
//   - Send, []Send
//   - []any with string/Send/Route entries
//
// Supported pathMap values:
//   - map[string]string
//   - []string (identity map)
//   - nil
func (g *StateGraph[State, Context, Input, Output]) AddConditionalEdgesDynamic(
	source string,
	path func(ctx context.Context, state State) (any, error),
	pathMap any,
) *StateGraph[State, Context, Input, Output] {
	if path == nil {
		panic("conditional path must not be nil")
	}
	mapped, err := coercePathMap(pathMap)
	if err != nil {
		panic(err)
	}
	wrapped := func(ctx context.Context, state State) (Route, error) {
		raw, pathErr := path(ctx, state)
		if pathErr != nil {
			return Route{}, pathErr
		}
		route, routeErr := CoerceRoute(raw)
		if routeErr != nil {
			return Route{}, routeErr
		}
		return route, nil
	}
	return g.AddConditionalEdges(source, wrapped, mapped)
}

// AddSequence adds a sequence of nodes that execute in order.
func (g *StateGraph[State, Context, Input, Output]) AddSequence(
	names []string,
	fns []func(ctx context.Context, state State) (NodeResult, error),
) *StateGraph[State, Context, Input, Output] {
	if len(names) != len(fns) {
		panic("names and fns must have the same length")
	}

	if len(names) == 0 {
		panic("Sequence requires at least one node")
	}

	for i, name := range names {
		g.AddNode(name, fns[i])
		if i > 0 {
			g.AddEdge(names[i-1], name)
		}
	}

	return g
}

// SetEntryPoint sets the starting node for the graph.
func (g *StateGraph[State, Context, Input, Output]) SetEntryPoint(name string) *StateGraph[State, Context, Input, Output] {
	g.entryPoint = name
	return g.AddEdge(Start, name)
}

// SetFinishPoint marks a node as a terminal node.
func (g *StateGraph[State, Context, Input, Output]) SetFinishPoint(name string) *StateGraph[State, Context, Input, Output] {
	g.finishPoints = append(g.finishPoints, name)
	return g.AddEdge(name, End)
}

// SetConditionalEntryPoint sets a conditional entry point.
func (g *StateGraph[State, Context, Input, Output]) SetConditionalEntryPoint(
	path func(ctx context.Context, state State) (Route, error),
	pathMap map[string]string,
) *StateGraph[State, Context, Input, Output] {
	return g.AddConditionalEdges(Start, path, pathMap)
}

// SetConditionalEntryPointDynamic sets a conditional entry point using the same
// flexible routing/path-map coercion as AddConditionalEdgesDynamic.
func (g *StateGraph[State, Context, Input, Output]) SetConditionalEntryPointDynamic(
	path func(ctx context.Context, state State) (any, error),
	pathMap any,
) *StateGraph[State, Context, Input, Output] {
	return g.AddConditionalEdgesDynamic(Start, path, pathMap)
}

// AddEdges adds directed edges from multiple source nodes to a single target.
// This is used for "join" nodes that wait for multiple sources to complete
// before executing. All sources must complete their current superstep before
// the target can proceed.
func (g *StateGraph[State, Context, Input, Output]) AddEdges(sources []string, target string) *StateGraph[State, Context, Input, Output] {
	if target == Start {
		panic("Start cannot be a target node")
	}

	if len(sources) == 0 {
		panic("AddEdges requires at least one source")
	}

	for _, source := range sources {
		if source == End {
			panic("End cannot be a source node")
		}

		if source != Start {
			if node, ok := g.nodes[source]; ok {
				node.Destinations = append(node.Destinations, target)
			}
		}
	}

	g.waitingEdges = append(g.waitingEdges, WaitingEdge{
		Sources: sources,
		Target:  target,
	})

	return g
}

// AllEdges returns all edges (both regular and waiting edges) as a combined slice.
// This is useful for operations that need to consider all graph edges.
func (g *StateGraph[State, Context, Input, Output]) AllEdges() []Edge {
	edges := make([]Edge, 0, len(g.edges)+len(g.waitingEdges))
	edges = append(edges, g.edges...)
	for _, we := range g.waitingEdges {
		for _, src := range we.Sources {
			edges = append(edges, Edge{Source: src, Target: we.Target})
		}
	}
	return edges
}

// Validate checks the graph for structural errors.
func (g *StateGraph[State, Context, Input, Output]) Validate(interrupt []string) error {
	if err := g.validateReservedIdentifiers(); err != nil {
		return err
	}
	if err := g.validateBranchSpecs(); err != nil {
		return err
	}
	if err := g.validateDeclaredDestinations(); err != nil {
		return err
	}

	sources := g.collectValidationSources()
	if err := g.validateKnownSources(sources); err != nil {
		return err
	}
	if err := validateGraphEntryPoint(sources); err != nil {
		return err
	}

	targets := g.collectValidationTargets()
	if err := g.validateKnownTargets(targets); err != nil {
		return err
	}
	if err := g.validateInterruptNodes(interrupt); err != nil {
		return err
	}

	// Validate branch ends against node destinations
	if err := g.validateBranchEnds(); err != nil {
		return err
	}

	g.compiled = true
	return nil
}

func (g *StateGraph[State, Context, Input, Output]) collectValidationSources() map[string]bool {
	out := make(map[string]bool)
	for _, edge := range g.edges {
		out[edge.Source] = true
	}
	for source := range g.branches {
		out[source] = true
	}
	for _, waiting := range g.waitingEdges {
		for _, source := range waiting.Sources {
			out[source] = true
		}
	}
	return out
}

func (g *StateGraph[State, Context, Input, Output]) collectValidationTargets() map[string]bool {
	out := make(map[string]bool)
	for _, edge := range g.edges {
		out[edge.Target] = true
	}
	for _, waiting := range g.waitingEdges {
		out[waiting.Target] = true
	}
	return out
}

func (g *StateGraph[State, Context, Input, Output]) validateKnownSources(sources map[string]bool) error {
	for source := range sources {
		if source != Start && g.nodes[source] == nil {
			return fmt.Errorf("found edge starting at unknown node '%s'", source)
		}
	}
	return nil
}

func validateGraphEntryPoint(sources map[string]bool) error {
	if sources[Start] {
		return nil
	}
	return fmt.Errorf("graph must have an entry point: add at least one edge from Start")
}

func (g *StateGraph[State, Context, Input, Output]) validateKnownTargets(targets map[string]bool) error {
	for target := range targets {
		if target != End && g.nodes[target] == nil {
			return fmt.Errorf("found edge ending at unknown node '%s'", target)
		}
	}
	return nil
}

func (g *StateGraph[State, Context, Input, Output]) validateInterruptNodes(interrupt []string) error {
	for _, node := range interrupt {
		if node == All {
			continue
		}
		if g.nodes[node] == nil {
			return fmt.Errorf("interrupt node '%s' not found", node)
		}
	}
	return nil
}

// validateBranchEnds checks that all conditional branch destinations are valid.
func (g *StateGraph[State, Context, Input, Output]) validateBranchEnds() error {
	for nodeName, branches := range g.branches {
		node, ok := g.nodes[nodeName]
		if !ok {
			continue
		}
		if err := g.validateNodeBranches(nodeName, node, branches); err != nil {
			return err
		}
	}
	return nil
}

func (g *StateGraph[State, Context, Input, Output]) validateNodeBranches(
	nodeName string,
	node *NodeSpec[State],
	branches []*BranchSpec[State],
) error {
	for _, branch := range branches {
		if err := g.validateBranchDefinition(nodeName, node, branch); err != nil {
			return err
		}
	}
	return nil
}

func (g *StateGraph[State, Context, Input, Output]) validateBranchDefinition(
	nodeName string,
	node *NodeSpec[State],
	branch *BranchSpec[State],
) error {
	if branch.Path == nil {
		return fmt.Errorf("branch '%s' from '%s' has nil path", branch.Name, nodeName)
	}
	if err := g.validateBranchPathMapTargets(nodeName, node, branch.PathMap); err != nil {
		return err
	}
	if err := g.validateBranchEndTargets(nodeName, branch); err != nil {
		return err
	}
	return nil
}

func (g *StateGraph[State, Context, Input, Output]) validateBranchPathMapTargets(
	nodeName string,
	node *NodeSpec[State],
	pathMap map[string]string,
) error {
	for routeKey, destination := range pathMap {
		if destination == End {
			continue
		}
		if _, ok := g.nodes[destination]; !ok {
			return fmt.Errorf("branch from '%s' with route key '%s' targets unknown node '%s'", nodeName, routeKey, destination)
		}
		if len(node.Destinations) == 0 {
			continue
		}
		if contains(node.Destinations, destination) {
			continue
		}
		return fmt.Errorf("branch from '%s' with route key '%s' targets '%s' which is not in node's destinations", nodeName, routeKey, destination)
	}
	return nil
}

func (g *StateGraph[State, Context, Input, Output]) validateBranchEndTargets(nodeName string, branch *BranchSpec[State]) error {
	for _, destination := range branch.Ends {
		if destination == End {
			continue
		}
		if _, ok := g.nodes[destination]; !ok {
			return fmt.Errorf("branch '%s' from '%s' targets unknown node '%s'", branch.Name, nodeName, destination)
		}
	}
	return nil
}

func (g *StateGraph[State, Context, Input, Output]) validateReservedIdentifiers() error {
	for name := range g.channels {
		if err := validateGraphIdentifier("channel", name); err != nil {
			return err
		}
	}
	for name := range g.managed {
		if err := validateGraphIdentifier("managed value", name); err != nil {
			return err
		}
	}
	for name := range g.nodes {
		if err := validateGraphIdentifier("node", name); err != nil {
			return err
		}
	}
	return nil
}

func (g *StateGraph[State, Context, Input, Output]) validateBranchSpecs() error {
	for source, branches := range g.branches {
		seen := make(map[string]struct{}, len(branches))
		for _, branch := range branches {
			if branch == nil {
				return fmt.Errorf("found nil branch at source '%s'", source)
			}
			if branch.Name == "" {
				return fmt.Errorf("found unnamed branch at source '%s'", source)
			}
			if _, exists := seen[branch.Name]; exists {
				return fmt.Errorf("duplicate branch name '%s' for node '%s'", branch.Name, source)
			}
			seen[branch.Name] = struct{}{}
		}
	}
	return nil
}

func (g *StateGraph[State, Context, Input, Output]) validateDeclaredDestinations() error {
	for nodeName, node := range g.nodes {
		for _, dest := range node.Destinations {
			if dest == End {
				continue
			}
			if dest == Start {
				return fmt.Errorf("node '%s' destination cannot target Start", nodeName)
			}
			if _, ok := g.nodes[dest]; !ok {
				return fmt.Errorf("node '%s' declares unknown destination '%s'", nodeName, dest)
			}
		}
	}
	return nil
}

func branchNameFromPath[State any](path func(ctx context.Context, state State) (Route, error)) string {
	if path == nil {
		return "condition"
	}
	rv := reflect.ValueOf(path)
	if !rv.IsValid() {
		return "condition"
	}
	if fn := runtime.FuncForPC(rv.Pointer()); fn != nil {
		name := fn.Name()
		if idx := strings.LastIndex(name, "/"); idx >= 0 && idx+1 < len(name) {
			name = name[idx+1:]
		}
		if name != "" {
			return name
		}
	}
	return "condition"
}

func validateGraphIdentifier(kind, name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%s name must not be empty", kind)
	}
	if _, reserved := reservedGraphIdentifiers[name]; reserved {
		return fmt.Errorf("%s name '%s' is reserved", kind, name)
	}
	for _, ch := range reservedNodeNameCharacters {
		if strings.Contains(name, ch) {
			return fmt.Errorf("%s name '%s' contains reserved character '%s'", kind, name, ch)
		}
	}
	return nil
}

func stateSchemaOf[T any]() any {
	var zero T
	return zero
}

func defaultReducerRegistry() map[string]func(a, b any) any {
	return map[string]func(a, b any) any{
		"append": appendReducer,
		"add_messages": func(a, b any) any {
			left, _ := a.([]Message)
			right, _ := b.([]Message)
			return AddMessages(left, right)
		},
	}
}

func appendReducer(a, b any) any {
	if a == nil {
		return appendValueToNilSlice(b)
	}
	av := reflect.ValueOf(a)
	if av.Kind() != reflect.Slice {
		return []any{a, b}
	}

	bv := reflect.ValueOf(b)
	if !bv.IsValid() {
		return a
	}
	if bv.Kind() == reflect.Slice && bv.Type().AssignableTo(av.Type()) {
		return reflect.AppendSlice(av, bv).Interface()
	}
	if bv.Type().AssignableTo(av.Type().Elem()) {
		return reflect.Append(av, bv).Interface()
	}
	if bv.Type().ConvertibleTo(av.Type().Elem()) {
		return reflect.Append(av, bv.Convert(av.Type().Elem())).Interface()
	}
	return []any{a, b}
}

func appendValueToNilSlice(value any) any {
	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return []any{}
	}
	if v.Kind() == reflect.Slice {
		copySlice := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		reflect.Copy(copySlice, v)
		return copySlice.Interface()
	}
	out := reflect.MakeSlice(reflect.SliceOf(v.Type()), 0, 1)
	out = reflect.Append(out, v)
	return out.Interface()
}

func (g *StateGraph[State, Context, Input, Output]) applyReducerAnnotations() error {
	rt := reflect.TypeOf(stateSchemaOf[State]())
	if rt == nil {
		return nil
	}
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return nil
	}

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		if !field.IsExported() {
			continue
		}
		if _, exists := g.channels[field.Name]; exists {
			continue
		}
		if _, isManaged := g.managed[field.Name]; isManaged {
			continue
		}

		reducerName := reducerNameFromTag(field)
		if reducerName == "" {
			g.channels[field.Name] = NewLastValue()
			continue
		}

		reducer, ok := g.reducers[reducerName]
		if !ok {
			return fmt.Errorf("unknown reducer '%s' on field '%s'", reducerName, field.Name)
		}
		g.channels[field.Name] = NewBinaryOperatorAggregateWithType(field.Type, reducer)
	}
	return nil
}

func reducerNameFromTag(field reflect.StructField) string {
	for _, tagKey := range []string{"graph", "langgraph"} {
		tag := strings.TrimSpace(field.Tag.Get(tagKey))
		if tag == "" {
			continue
		}
		parts := strings.Split(tag, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if !strings.HasPrefix(part, "reducer=") {
				continue
			}
			return strings.TrimSpace(strings.TrimPrefix(part, "reducer="))
		}
	}
	return ""
}

type injectedArg int

const (
	injectConfig injectedArg = iota
	injectConfigPtr
	injectWriter
	injectStore
	injectRuntime
	injectRuntimePtr
)

type wrappedNodeSignature struct {
	inputSchema any
	inputType   reflect.Type
	injected    []injectedArg
	withContext bool
}

func wrapNodeFunc[State any](fn any) (Node[State], any, error) {
	fv := reflect.ValueOf(fn)
	if !fv.IsValid() || fv.Kind() != reflect.Func {
		return nil, nil, fmt.Errorf("node function must be a function, got %T", fn)
	}
	ft := fv.Type()
	if err := validateWrappedNodeFunctionType(ft); err != nil {
		return nil, nil, err
	}

	signature, err := parseWrappedNodeSignature(ft)
	if err != nil {
		return nil, nil, err
	}

	node := func(ctx context.Context, state State) (NodeResult, error) {
		converted, err := coerceValueToType(any(state), signature.inputType)
		if err != nil {
			return NodeResult{}, err
		}
		args := buildWrappedNodeCallArgs(ctx, ft, signature, converted)
		outs := fv.Call(args)
		if !outs[1].IsNil() {
			return NodeResult{}, outs[1].Interface().(error)
		}
		return coerceWrappedNodeResult(outs[0].Interface()), nil
	}

	return node, signature.inputSchema, nil
}

func validateWrappedNodeFunctionType(ft reflect.Type) error {
	if ft.NumOut() != 2 || !ft.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return fmt.Errorf("node function must return (result, error)")
	}
	return nil
}

func parseWrappedNodeSignature(ft reflect.Type) (wrappedNodeSignature, error) {
	contextType := reflect.TypeOf((*context.Context)(nil)).Elem()

	argIndex := 0
	withContext := false
	if ft.NumIn() >= 2 && ft.In(0).Implements(contextType) {
		withContext = true
		argIndex = 1
	} else if ft.NumIn() < 1 {
		return wrappedNodeSignature{}, fmt.Errorf("unsupported node function signature")
	}
	if argIndex >= ft.NumIn() {
		return wrappedNodeSignature{}, fmt.Errorf("node function must have an input argument")
	}

	inputType := ft.In(argIndex)
	injected, err := parseInjectedNodeArgs(ft, argIndex+1)
	if err != nil {
		return wrappedNodeSignature{}, err
	}
	return wrappedNodeSignature{
		inputType:   inputType,
		inputSchema: reflect.New(inputType).Elem().Interface(),
		withContext: withContext,
		injected:    injected,
	}, nil
}

func parseInjectedNodeArgs(ft reflect.Type, start int) ([]injectedArg, error) {
	configType := reflect.TypeOf(Config{})
	configPtrType := reflect.TypeOf(&Config{})
	streamWriterType := reflect.TypeOf(StreamWriter(nil))
	storeType := reflect.TypeOf((*Store)(nil)).Elem()
	runtimeType := reflect.TypeOf(Runtime{})
	runtimePtrType := reflect.TypeOf(&Runtime{})

	out := make([]injectedArg, 0, ft.NumIn())
	for i := start; i < ft.NumIn(); i++ {
		argType := ft.In(i)
		switch argType {
		case configType:
			out = append(out, injectConfig)
		case configPtrType:
			out = append(out, injectConfigPtr)
		case streamWriterType:
			out = append(out, injectWriter)
		case storeType:
			out = append(out, injectStore)
		case runtimeType:
			out = append(out, injectRuntime)
		case runtimePtrType:
			out = append(out, injectRuntimePtr)
		default:
			return nil, fmt.Errorf("unsupported injected parameter type %s", argType)
		}
	}
	return out, nil
}

func buildWrappedNodeCallArgs(
	ctx context.Context,
	ft reflect.Type,
	signature wrappedNodeSignature,
	convertedState reflect.Value,
) []reflect.Value {
	args := make([]reflect.Value, 0, ft.NumIn())
	if signature.withContext {
		args = append(args, reflect.ValueOf(ctx))
	}
	args = append(args, convertedState)
	for _, spec := range signature.injected {
		args = append(args, wrappedNodeInjectedArgValue(ctx, spec))
	}
	return args
}

func wrappedNodeInjectedArgValue(ctx context.Context, spec injectedArg) reflect.Value {
	storeType := reflect.TypeOf((*Store)(nil)).Elem()
	runtimeType := reflect.TypeOf(Runtime{})
	runtimePtrType := reflect.TypeOf(&Runtime{})

	switch spec {
	case injectConfig:
		return reflect.ValueOf(GetConfig(ctx))
	case injectConfigPtr:
		cfg := GetConfig(ctx)
		return reflect.ValueOf(&cfg)
	case injectWriter:
		return reflect.ValueOf(GetStreamWriter(ctx))
	case injectStore:
		store := GetStore(ctx)
		if store == nil {
			return reflect.Zero(storeType)
		}
		return reflect.ValueOf(store)
	case injectRuntime:
		rt := GetRuntime(ctx)
		if rt == nil {
			return reflect.Zero(runtimeType)
		}
		return reflect.ValueOf(*rt)
	case injectRuntimePtr:
		rt := GetRuntime(ctx)
		if rt == nil {
			return reflect.Zero(runtimePtrType)
		}
		return reflect.ValueOf(rt)
	default:
		return reflect.Value{}
	}
}

func coerceWrappedNodeResult(out any) NodeResult {
	if nr, ok := out.(NodeResult); ok {
		return nr
	}
	if cmd, ok := out.(Command); ok {
		return NodeCommand(&cmd)
	}
	if cmd, ok := out.(*Command); ok {
		return NodeCommand(cmd)
	}
	return NodeState(Dyn(out))
}

func coerceValueToType(value any, target reflect.Type) (reflect.Value, error) {
	if target.Kind() == reflect.Ptr {
		return coerceValueToPointerType(value, target)
	}
	v := reflect.ValueOf(value)
	if direct, ok := coerceDirectValue(v, target); ok {
		return direct, nil
	}
	if mapStruct, ok, err := coerceMapToStructValue(v, target); ok || err != nil {
		return mapStruct, err
	}
	if structStruct, ok := coerceStructToStructValue(v, target); ok {
		return structStruct, nil
	}
	return reflect.Value{}, fmt.Errorf("cannot convert %T to %s", value, target)
}

func coerceValueToPointerType(value any, target reflect.Type) (reflect.Value, error) {
	inner, err := coerceValueToType(value, target.Elem())
	if err != nil {
		return reflect.Value{}, err
	}
	ptr := reflect.New(target.Elem())
	ptr.Elem().Set(inner)
	return ptr, nil
}

func coerceDirectValue(value reflect.Value, target reflect.Type) (reflect.Value, bool) {
	if !value.IsValid() {
		return reflect.Value{}, false
	}
	if value.Type().AssignableTo(target) {
		return value, true
	}
	if value.Type().ConvertibleTo(target) {
		return value.Convert(target), true
	}
	return reflect.Value{}, false
}

func coerceMapToStructValue(value reflect.Value, target reflect.Type) (reflect.Value, bool, error) {
	if !value.IsValid() || value.Kind() != reflect.Map || target.Kind() != reflect.Struct {
		return reflect.Value{}, false, nil
	}
	if value.Type().Key().Kind() != reflect.String {
		return reflect.Value{}, true, fmt.Errorf("cannot convert map key type %s to struct %s", value.Type().Key(), target)
	}
	out := reflect.New(target).Elem()
	for i := 0; i < target.NumField(); i++ {
		field := target.Field(i)
		if !field.IsExported() {
			continue
		}
		fieldVal, ok := mapLookupByFieldName(value, field)
		if !ok {
			continue
		}
		coerced, err := coerceValueToType(fieldVal.Interface(), field.Type)
		if err != nil {
			return reflect.Value{}, true, fmt.Errorf("field '%s': %w", field.Name, err)
		}
		out.Field(i).Set(coerced)
	}
	return out, true, nil
}

func coerceStructToStructValue(value reflect.Value, target reflect.Type) (reflect.Value, bool) {
	if !value.IsValid() || value.Kind() != reflect.Struct || target.Kind() != reflect.Struct {
		return reflect.Value{}, false
	}
	out := reflect.New(target).Elem()
	for i := 0; i < target.NumField(); i++ {
		field := target.Field(i)
		if !field.IsExported() {
			continue
		}
		source := value.FieldByName(field.Name)
		if !source.IsValid() {
			continue
		}
		if source.Type().AssignableTo(field.Type) {
			out.Field(i).Set(source)
			continue
		}
		if source.Type().ConvertibleTo(field.Type) {
			out.Field(i).Set(source.Convert(field.Type))
		}
	}
	return out, true
}

func mapLookupByFieldName(v reflect.Value, field reflect.StructField) (reflect.Value, bool) {
	keys := []string{field.Name}
	if tag := field.Tag.Get("json"); tag != "" {
		parts := strings.Split(tag, ",")
		if parts[0] != "" && parts[0] != "-" {
			keys = append(keys, parts[0])
		}
	}
	for _, key := range keys {
		mv := v.MapIndex(reflect.ValueOf(key))
		if mv.IsValid() {
			return mv, true
		}
	}
	return reflect.Value{}, false
}

func coercePathMap(pathMap any) (map[string]string, error) {
	switch v := pathMap.(type) {
	case nil:
		return nil, nil
	case map[string]string:
		copyMap := make(map[string]string, len(v))
		for k, dest := range v {
			copyMap[k] = dest
		}
		return copyMap, nil
	case []string:
		mapped := make(map[string]string, len(v))
		for _, key := range v {
			mapped[key] = key
		}
		return mapped, nil
	default:
		return nil, fmt.Errorf("unsupported pathMap type %T", pathMap)
	}
}

// CompileOptions contains optional configuration for graph compilation.
type CompileOptions struct {
	Checkpointer    checkpoint.Saver
	Store           Store
	Cache           Cache
	Context         any
	Durability      DurabilityMode
	Name            string
	InterruptBefore []string
	InterruptAfter  []string
	StepTimeout     time.Duration
	MaxConcurrency  int
	Debug           bool
}

// Store is the interface for persistent key-value stores.
// Stores enable persistence and memory that can be shared across threads,
// scoped to user IDs, assistant IDs, or other arbitrary namespaces.
type Store interface {
	Get(namespace []string, key string) (any, bool, error)
	Set(namespace []string, key string, value any) error
	Delete(namespace []string, key string) error
	List(namespace []string, prefix string) ([]string, error)
	Search(namespace []string, query string, limit int) ([]string, error)
}

// Cache is a pluggable key-value cache for node outputs.
type Cache interface {
	Get(ctx context.Context, key CacheKey) (any, bool, error)
	Set(ctx context.Context, key CacheKey, value any) error
}

// Compile creates an executable CompiledStateGraph from this builder.
func (g *StateGraph[State, Context, Input, Output]) Compile(options ...CompileOptions) (*CompiledStateGraph[State, Context, Input, Output], error) {
	var opts CompileOptions
	if len(options) > 0 {
		opts = options[0]
	}

	interruptBefore := append([]string(nil), g.interruptBefore...)
	if opts.InterruptBefore != nil {
		interruptBefore = append([]string(nil), opts.InterruptBefore...)
	}
	interruptAfter := append([]string(nil), g.interruptAfter...)
	if opts.InterruptAfter != nil {
		interruptAfter = append([]string(nil), opts.InterruptAfter...)
	}

	if err := g.applyReducerAnnotations(); err != nil {
		return nil, err
	}

	// Validate the graph. Build a fresh slice to avoid mutating the options slices.
	interrupt := make([]string, 0, len(interruptBefore)+len(interruptAfter))
	interrupt = append(interrupt, interruptBefore...)
	interrupt = append(interrupt, interruptAfter...)
	if err := g.Validate(interrupt); err != nil {
		return nil, err
	}

	name := opts.Name
	if name == "" {
		name = "LangGraph"
	}

	return &CompiledStateGraph[State, Context, Input, Output]{
		builder:         g,
		checkpointer:    opts.Checkpointer,
		store:           opts.Store,
		cache:           opts.Cache,
		contextValue:    opts.Context,
		interruptBefore: interruptBefore,
		interruptAfter:  interruptAfter,
		debug:           opts.Debug,
		name:            name,
		durability:      opts.Durability,
		stepTimeout:     opts.StepTimeout,
		maxConcurrency:  opts.MaxConcurrency,
	}, nil
}
