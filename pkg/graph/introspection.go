package graph

import (
	"fmt"
	"sort"
	"strings"
	"unicode"
)

// GraphNodeKind describes the node category in an introspection graph.
type GraphNodeKind string

const (
	// GraphNodeStart is the synthetic START node.
	GraphNodeStart GraphNodeKind = "start"

	// GraphNodeEnd is the synthetic END node.
	GraphNodeEnd GraphNodeKind = "end"

	// GraphNodeRegular is a user-defined graph node.
	GraphNodeRegular GraphNodeKind = "node"
)

// GraphEdgeKind describes an edge category in an introspection graph.
type GraphEdgeKind string

const (
	// GraphEdgeDirect is a direct edge created via AddEdge.
	GraphEdgeDirect GraphEdgeKind = "direct"

	// GraphEdgeConditional is an edge created by AddConditionalEdges.
	GraphEdgeConditional GraphEdgeKind = "conditional"

	// GraphEdgeWaiting is an edge created by AddEdges (waiting/join edge).
	GraphEdgeWaiting GraphEdgeKind = "waiting"
)

// GraphNodeInfo is an introspectable description of a graph node.
type GraphNodeInfo struct {
	// Name is the node identifier.
	Name string `json:"name"`

	// Kind identifies start/end/regular nodes.
	Kind GraphNodeKind `json:"kind"`

	// InputSchema is the inferred or explicit input schema descriptor.
	InputSchema any `json:"input_schema,omitempty"`

	// Metadata mirrors node metadata when configured.
	Metadata map[string]any `json:"metadata,omitempty"`

	// Subgraphs lists subgraph names attached to the node.
	Subgraphs []string `json:"subgraphs,omitempty"`

	// Destinations lists known possible node destinations.
	Destinations []string `json:"destinations,omitempty"`

	// RetryPolicyCount is the number of configured retry policies.
	RetryPolicyCount int `json:"retry_policy_count,omitempty"`

	// HasCachePolicy reports whether node caching is configured.
	HasCachePolicy bool `json:"has_cache_policy,omitempty"`

	// Deferred indicates if this node has deferred scheduling.
	Deferred bool `json:"deferred,omitempty"`
}

// GraphEdgeInfo is an introspectable description of a graph edge.
type GraphEdgeInfo struct {
	// Source is the source node name.
	Source string `json:"source"`

	// Target is the target node name. It may be empty for unresolved dynamic routes.
	Target string `json:"target,omitempty"`

	// Kind identifies direct/conditional/waiting edges.
	Kind GraphEdgeKind `json:"kind"`

	// Label is edge label metadata, such as conditional route key.
	Label string `json:"label,omitempty"`

	// Branch is the branch identifier for conditional edges.
	Branch string `json:"branch,omitempty"`

	// WaitFor lists all prerequisite sources for waiting edges.
	WaitFor []string `json:"wait_for,omitempty"`

	// Resolved reports whether this edge has a concrete target.
	Resolved bool `json:"resolved"`
}

// GraphInfo is a Go-native introspection structure for a compiled graph.
//
// It is independent of rendering tools and can be exported to textual
// diagram formats (for example, Mermaid) using helper methods.
type GraphInfo struct {
	// Name is the compiled graph name.
	Name string `json:"name"`

	// Nodes lists graph nodes, including synthetic START and END nodes.
	Nodes []GraphNodeInfo `json:"nodes"`

	// Edges lists graph edges across direct, conditional, and waiting topology.
	Edges []GraphEdgeInfo `json:"edges"`

	// InputSchema is the configured graph input schema descriptor.
	InputSchema any `json:"input_schema,omitempty"`

	// OutputSchema is the configured graph output schema descriptor.
	OutputSchema any `json:"output_schema,omitempty"`

	// ContextSchema is the configured graph context schema descriptor.
	ContextSchema any `json:"context_schema,omitempty"`

	// Channels lists configured channel names.
	Channels []string `json:"channels,omitempty"`

	// ManagedValues lists configured managed value keys.
	ManagedValues []string `json:"managed_values,omitempty"`

	// InterruptBefore lists nodes configured for pre-execution interrupts.
	InterruptBefore []string `json:"interrupt_before,omitempty"`

	// InterruptAfter lists nodes configured for post-execution interrupts.
	InterruptAfter []string `json:"interrupt_after,omitempty"`
}

// GraphSchemas contains JSON schema views for graph IO/context contracts.
type GraphSchemas struct {
	Input   map[string]any `json:"input"`
	Output  map[string]any `json:"output"`
	Context map[string]any `json:"context"`
}

// GraphSubgraphInfo describes a named subgraph annotation attached to a node.
type GraphSubgraphInfo struct {
	Name       string `json:"name"`
	ParentNode string `json:"parent_node"`
	Namespace  string `json:"namespace"`
}

// GetGraph returns a drawable and introspectable view of the compiled graph.
func (g *CompiledStateGraph[State, Context, Input, Output]) GetGraph() GraphInfo {
	if g == nil || g.builder == nil {
		return GraphInfo{}
	}

	b := g.builder
	info := GraphInfo{
		Name:            g.name,
		InputSchema:     b.InputSchema(),
		OutputSchema:    b.OutputSchema(),
		ContextSchema:   b.ContextSchema(),
		Channels:        sortedMapKeys(b.channels),
		ManagedValues:   sortedMapKeys(b.managed),
		InterruptBefore: append([]string(nil), g.interruptBefore...),
		InterruptAfter:  append([]string(nil), g.interruptAfter...),
	}

	info.Nodes = graphNodesFromBuilder(b)
	info.Edges = graphEdgesFromBuilder(b)
	return info
}

// Mermaid renders the graph as a Mermaid flowchart string.
func (g GraphInfo) Mermaid() string {
	var b strings.Builder
	b.WriteString("flowchart TD\n")

	nodes := append([]GraphNodeInfo(nil), g.Nodes...)
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Name < nodes[j].Name
	})

	idByName := make(map[string]string, len(nodes))
	usedIDs := map[string]int{}
	for _, node := range nodes {
		idByName[node.Name] = nextMermaidID(node.Name, usedIDs)
	}

	for _, node := range nodes {
		id := idByName[node.Name]
		label := escapeMermaidLabel(node.Name)
		switch node.Kind {
		case GraphNodeStart, GraphNodeEnd:
			b.WriteString(fmt.Sprintf("    %s((\"%s\"))\n", id, label))
		default:
			b.WriteString(fmt.Sprintf("    %s[\"%s\"]\n", id, label))
		}
	}

	edges := append([]GraphEdgeInfo(nil), g.Edges...)
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].Source != edges[j].Source {
			return edges[i].Source < edges[j].Source
		}
		if edges[i].Target != edges[j].Target {
			return edges[i].Target < edges[j].Target
		}
		if edges[i].Kind != edges[j].Kind {
			return edges[i].Kind < edges[j].Kind
		}
		if edges[i].Branch != edges[j].Branch {
			return edges[i].Branch < edges[j].Branch
		}
		if edges[i].Label != edges[j].Label {
			return edges[i].Label < edges[j].Label
		}
		return !edges[i].Resolved && edges[j].Resolved
	})

	for _, edge := range edges {
		sourceID, sourceOK := idByName[edge.Source]
		targetID, targetOK := idByName[edge.Target]
		if !sourceOK {
			continue
		}
		if !edge.Resolved || edge.Target == "" || !targetOK {
			b.WriteString(fmt.Sprintf("    %% unresolved conditional route from %s (%s)\n", sourceID, edge.Branch))
			continue
		}

		connector := "-->"
		if edge.Kind == GraphEdgeConditional {
			connector = "-.->"
		}

		label := strings.TrimSpace(edgeLabel(edge))
		if label != "" {
			b.WriteString(fmt.Sprintf("    %s %s|%s| %s\n", sourceID, connector, escapeMermaidLabel(label), targetID))
			continue
		}
		b.WriteString(fmt.Sprintf("    %s %s %s\n", sourceID, connector, targetID))
	}

	return b.String()
}

// Schemas returns JSON-schema-compatible views for graph input/output/context.
func (g GraphInfo) Schemas() GraphSchemas {
	return GraphSchemas{
		Input:   GetInputJSONSchema(g.InputSchema, "Input"),
		Output:  GetOutputJSONSchema(g.OutputSchema, "Output"),
		Context: GetInputJSONSchema(g.ContextSchema, "Context"),
	}
}

// Subgraphs returns flattened subgraph annotations across graph nodes.
func (g GraphInfo) Subgraphs() []GraphSubgraphInfo {
	out := make([]GraphSubgraphInfo, 0)
	for _, node := range g.Nodes {
		if node.Kind != GraphNodeRegular || len(node.Subgraphs) == 0 {
			continue
		}
		for _, name := range sortedUnique(node.Subgraphs) {
			out = append(out, GraphSubgraphInfo{
				Name:       name,
				ParentNode: node.Name,
				Namespace:  node.Name + "/" + name,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		if out[i].ParentNode != out[j].ParentNode {
			return out[i].ParentNode < out[j].ParentNode
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func graphNodesFromBuilder[State, Context, Input, Output any](b *StateGraph[State, Context, Input, Output]) []GraphNodeInfo {
	names := make([]string, 0, len(b.nodes)+2)
	names = append(names, START, END)
	for name := range b.nodes {
		names = append(names, name)
	}
	sort.Strings(names)

	nodes := make([]GraphNodeInfo, 0, len(names))
	for _, name := range names {
		switch name {
		case START:
			nodes = append(nodes, GraphNodeInfo{Name: START, Kind: GraphNodeStart})
			continue
		case END:
			nodes = append(nodes, GraphNodeInfo{Name: END, Kind: GraphNodeEnd})
			continue
		}

		n := b.nodes[name]
		if n == nil {
			continue
		}
		nodes = append(nodes, GraphNodeInfo{
			Name:             name,
			Kind:             GraphNodeRegular,
			InputSchema:      n.InputSchema,
			Metadata:         cloneAnyMap(n.Metadata),
			Subgraphs:        append([]string(nil), n.Subgraphs...),
			Destinations:     sortedUnique(n.Destinations),
			RetryPolicyCount: len(n.RetryPolicy),
			HasCachePolicy:   n.CachePolicy != nil,
			Deferred:         n.Defer,
		})
	}

	return nodes
}

func graphEdgesFromBuilder[State, Context, Input, Output any](b *StateGraph[State, Context, Input, Output]) []GraphEdgeInfo {
	edges := make([]GraphEdgeInfo, 0, len(b.edges)+len(b.waitingEdges))

	for _, edge := range b.edges {
		edges = append(edges, GraphEdgeInfo{
			Source:   edge.Source,
			Target:   edge.Target,
			Kind:     GraphEdgeDirect,
			Resolved: edge.Target != "",
		})
	}

	for _, we := range b.waitingEdges {
		waitFor := sortedUnique(we.Sources)
		for _, src := range waitFor {
			edges = append(edges, GraphEdgeInfo{
				Source:   src,
				Target:   we.Target,
				Kind:     GraphEdgeWaiting,
				WaitFor:  append([]string(nil), waitFor...),
				Resolved: we.Target != "",
			})
		}
	}

	branchSources := make([]string, 0, len(b.branches))
	for source := range b.branches {
		branchSources = append(branchSources, source)
	}
	sort.Strings(branchSources)

	for _, source := range branchSources {
		branches := b.branches[source]
		for _, branch := range branches {
			targets := sortedConditionalTargets(branch)
			if len(targets) == 0 {
				edges = append(edges, GraphEdgeInfo{
					Source:   source,
					Kind:     GraphEdgeConditional,
					Label:    "dynamic",
					Branch:   branch.Name,
					Resolved: false,
				})
				continue
			}
			for _, target := range targets {
				edges = append(edges, GraphEdgeInfo{
					Source:   source,
					Target:   target.target,
					Kind:     GraphEdgeConditional,
					Label:    target.label,
					Branch:   branch.Name,
					Resolved: target.target != "",
				})
			}
		}
	}

	sort.Slice(edges, func(i, j int) bool {
		if edges[i].Source != edges[j].Source {
			return edges[i].Source < edges[j].Source
		}
		if edges[i].Target != edges[j].Target {
			return edges[i].Target < edges[j].Target
		}
		if edges[i].Kind != edges[j].Kind {
			return edges[i].Kind < edges[j].Kind
		}
		if edges[i].Branch != edges[j].Branch {
			return edges[i].Branch < edges[j].Branch
		}
		if edges[i].Label != edges[j].Label {
			return edges[i].Label < edges[j].Label
		}
		return !edges[i].Resolved && edges[j].Resolved
	})

	return edges
}

type labeledTarget struct {
	target string
	label  string
}

func sortedConditionalTargets[State any](branch *BranchSpec[State]) []labeledTarget {
	if branch == nil {
		return nil
	}

	targets := make([]labeledTarget, 0, len(branch.PathMap)+len(branch.Ends))
	if len(branch.PathMap) > 0 {
		keys := make([]string, 0, len(branch.PathMap))
		for key := range branch.PathMap {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			targets = append(targets, labeledTarget{target: branch.PathMap[key], label: key})
		}
		return targets
	}

	for _, end := range sortedUnique(branch.Ends) {
		targets = append(targets, labeledTarget{target: end})
	}
	return targets
}

func sortedMapKeys[V any](m map[string]V) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedUnique(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func edgeLabel(edge GraphEdgeInfo) string {
	if edge.Label != "" {
		return edge.Label
	}
	if edge.Kind == GraphEdgeWaiting && len(edge.WaitFor) > 0 {
		return "wait: " + strings.Join(edge.WaitFor, ",")
	}
	return ""
}

func nextMermaidID(name string, used map[string]int) string {
	base := sanitizeMermaidID(name)
	if base == "" {
		base = "node"
	}
	count := used[base]
	used[base] = count + 1
	if count == 0 {
		return base
	}
	return fmt.Sprintf("%s_%d", base, count)
}

func sanitizeMermaidID(s string) string {
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	out := b.String()
	if out == "" {
		return ""
	}
	r := rune(out[0])
	if unicode.IsDigit(r) {
		return "n_" + out
	}
	return out
}

func escapeMermaidLabel(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "|", "/")
	return s
}
