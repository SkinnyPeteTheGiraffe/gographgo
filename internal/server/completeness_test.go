package server_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/internal/server"
	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/checkpoint"
	graphpkg "github.com/SkinnyPeteTheGiraffe/gographgo/pkg/graph"
)

type introspectionRunner struct{}

type blockingStreamRunner struct {
	started chan struct{}
	release chan struct{}
}

func (r blockingStreamRunner) Run(ctx context.Context, req server.RunRequest) (server.RunResult, error) {
	select {
	case <-r.started:
	default:
		close(r.started)
	}
	select {
	case <-ctx.Done():
		return server.RunResult{}, ctx.Err()
	case <-r.release:
	}
	return server.RunResult{
		Output: map[string]any{
			"echo": req.Input["message"],
		},
		State: map[string]any{
			"last_message": req.Input["message"],
		},
	}, nil
}

type richStreamRunner struct{}

func (richStreamRunner) Run(_ context.Context, req server.RunRequest) (server.RunResult, error) {
	msg, _ := req.Input["message"].(string)
	return server.RunResult{
		Output: map[string]any{
			"echo": msg,
			"messages": []any{
				map[string]any{"role": "assistant", "content": "processed:" + msg},
			},
			"custom": map[string]any{"trace_id": "trace-1"},
		},
		State: map[string]any{"last_message": msg},
	}, nil
}

func (introspectionRunner) Run(_ context.Context, req server.RunRequest) (server.RunResult, error) {
	msg := ""
	if req.Input != nil {
		if m, ok := req.Input["message"].(string); ok {
			msg = m
		}
	}
	return server.RunResult{
		Output: map[string]any{"echo": msg},
		State:  map[string]any{"last_message": msg},
	}, nil
}

func (introspectionRunner) GraphInfo(_ context.Context, graphID string) (graphpkg.GraphInfo, error) {
	g := graphpkg.NewStateGraph[map[string]any]()
	g.SetInputSchema(struct {
		Message string `json:"message"`
	}{})
	g.SetOutputSchema(struct {
		Reply string `json:"reply"`
	}{})
	g.AddNode("router", func(_ context.Context, _ map[string]any) (graphpkg.NodeResult, error) {
		return graphpkg.NoNodeResult(), nil
	})
	g.AddNode("worker", func(_ context.Context, _ map[string]any) (graphpkg.NodeResult, error) {
		return graphpkg.NoNodeResult(), nil
	})
	g.AddEdge(graphpkg.Start, "router")
	g.AddEdge("router", "worker")
	g.AddEdge("worker", graphpkg.End)
	g.SetNodeSubgraphs("router", "decision")
	compiled, err := g.Compile(graphpkg.CompileOptions{Name: graphID})
	if err != nil {
		return graphpkg.GraphInfo{}, err
	}
	return compiled.GetGraph(), nil
}

func TestServer_AssistantsEndpoints(t *testing.T) {
	t.Parallel()

	s := server.New(server.Options{Runner: introspectionRunner{}})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	assistant := postJSON[server.Assistant](
		t,
		ts.URL+"/v1/assistants",
		`{"assistant_id":"a1","graph_id":"g1","name":"alpha","metadata":{"team":"ml"}}`,
		http.StatusCreated,
	)
	if assistant.ID != "a1" {
		t.Fatalf("assistant_id = %q", assistant.ID)
	}

	got := getJSON[server.Assistant](t, ts.URL+"/v1/assistants/a1", http.StatusOK)
	if got.GraphID != "g1" {
		t.Fatalf("graph_id = %q", got.GraphID)
	}

	patched := patchJSON[server.Assistant](t, ts.URL+"/v1/assistants/a1", `{"name":"beta"}`, http.StatusOK)
	if patched.Name != "beta" {
		t.Fatalf("name = %q", patched.Name)
	}

	search := postJSON[[]server.Assistant](t, ts.URL+"/v1/assistants/search", `{"name":"beta"}`, http.StatusOK)
	if len(search) != 1 || search[0].ID != "a1" {
		t.Fatalf("search = %#v", search)
	}

	count := postJSON[map[string]int](t, ts.URL+"/v1/assistants/count", `{}`, http.StatusOK)
	if count["count"] != 1 {
		t.Fatalf("count = %#v", count)
	}

	versions := postJSON[[]server.AssistantVersion](t, ts.URL+"/v1/assistants/a1/versions", `{"limit":10}`, http.StatusOK)
	if len(versions) < 1 {
		t.Fatal("expected at least one version")
	}

	latest := postJSON[server.Assistant](t, ts.URL+"/v1/assistants/a1/latest", `{"version":1}`, http.StatusOK)
	if latest.ID != "a1" {
		t.Fatalf("latest assistant = %#v", latest)
	}

	graphInfo := getJSON[map[string]any](t, ts.URL+"/v1/assistants/a1/graph", http.StatusOK)
	if graphInfo["assistant_id"] != "a1" || graphInfo["graph_id"] != "g1" {
		t.Fatalf("assistant graph response = %#v", graphInfo)
	}
	if graphObj, ok := graphInfo["graph"].(map[string]any); !ok || graphObj["name"] != "g1" {
		t.Fatalf("graph payload = %#v", graphInfo)
	}

	schemas := getJSON[map[string]any](t, ts.URL+"/v1/assistants/a1/schemas", http.StatusOK)
	if schemas["assistant_id"] != "a1" || schemas["graph_id"] != "g1" {
		t.Fatalf("schemas response = %#v", schemas)
	}
	if input, ok := schemas["input"].(map[string]any); !ok || input["type"] != "object" {
		t.Fatalf("schemas input = %#v", schemas)
	}

	subgraphs := getJSON[map[string]any](t, ts.URL+"/v1/assistants/a1/subgraphs", http.StatusOK)
	items, ok := subgraphs["subgraphs"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("subgraphs response = %#v", subgraphs)
	}

	deleteNoBody(t, ts.URL+"/v1/assistants/a1", http.StatusNoContent)
}

func TestServer_AssistantsContractGaps(t *testing.T) {
	t.Parallel()

	s := server.New(server.Options{Checkpointer: checkpoint.NewInMemorySaver()})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	postJSON[server.Assistant](
		t,
		ts.URL+"/v1/assistants",
		`{"assistant_id":"a1","graph_id":"g1","name":"Alpha Agent","metadata":{"team":"ml","stage":"draft"}}`,
		http.StatusCreated,
	)
	postJSON[server.Assistant](
		t,
		ts.URL+"/v1/assistants",
		`{"assistant_id":"a2","graph_id":"g1","name":"beta helper","metadata":{"team":"ml","stage":"prod"}}`,
		http.StatusCreated,
	)
	postJSON[server.Assistant](
		t,
		ts.URL+"/v1/assistants",
		`{"assistant_id":"a3","graph_id":"g2","name":"Gamma","metadata":{"team":"ops","stage":"prod"}}`,
		http.StatusCreated,
	)

	searchReq, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/assistants/search", strings.NewReader(`{"graph_id":"g1","limit":1}`))
	if err != nil {
		t.Fatalf("new search request: %v", err)
	}
	searchReq.Header.Set("Content-Type", "application/json")
	searchResp, err := http.DefaultClient.Do(searchReq)
	if err != nil {
		t.Fatalf("search request: %v", err)
	}
	defer func() { _ = searchResp.Body.Close() }()
	if searchResp.StatusCode != http.StatusOK {
		t.Fatalf("search status = %d", searchResp.StatusCode)
	}
	if got := searchResp.Header.Get("X-Pagination-Next"); got != "1" {
		t.Fatalf("X-Pagination-Next = %q, want %q", got, "1")
	}
	var search []server.Assistant
	if err := json.NewDecoder(searchResp.Body).Decode(&search); err != nil {
		t.Fatalf("decode search response: %v", err)
	}
	if len(search) != 1 || search[0].ID != "a1" {
		t.Fatalf("search response = %#v", search)
	}

	count := postJSON[map[string]int](t, ts.URL+"/v1/assistants/count", `{"name":"HELP"}`, http.StatusOK)
	if count["count"] != 1 {
		t.Fatalf("count = %#v", count)
	}

	updated := patchJSON[server.Assistant](t, ts.URL+"/v1/assistants/a1", `{"metadata":{"team":"ml","stage":"prod"}}`, http.StatusOK)
	if updated.Metadata["stage"] != "prod" {
		t.Fatalf("updated metadata = %#v", updated.Metadata)
	}

	versions := postJSON[[]server.AssistantVersion](t, ts.URL+"/v1/assistants/a1/versions", `{"metadata":{"stage":"prod"},"limit":10}`, http.StatusOK)
	if len(versions) != 1 || versions[0].Version != 2 {
		t.Fatalf("versions = %#v", versions)
	}

	dupReq, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/assistants", strings.NewReader(`{"assistant_id":"a1","graph_id":"g9","if_exists":"do_nothing"}`))
	if err != nil {
		t.Fatalf("new duplicate create request: %v", err)
	}
	dupReq.Header.Set("Content-Type", "application/json")
	dupResp, err := http.DefaultClient.Do(dupReq)
	if err != nil {
		t.Fatalf("duplicate create request: %v", err)
	}
	defer func() { _ = dupResp.Body.Close() }()
	if dupResp.StatusCode != http.StatusOK {
		t.Fatalf("duplicate create status = %d", dupResp.StatusCode)
	}
	var duplicate server.Assistant
	if err := json.NewDecoder(dupResp.Body).Decode(&duplicate); err != nil {
		t.Fatalf("decode duplicate create response: %v", err)
	}
	if duplicate.ID != "a1" || duplicate.GraphID != "g1" {
		t.Fatalf("duplicate create response = %#v", duplicate)
	}

	thread1 := postJSON[server.Thread](t, ts.URL+"/v1/threads", `{"thread_id":"t-a1","metadata":{"assistant_id":"a1"}}`, http.StatusCreated)
	if thread1.ID != "t-a1" {
		t.Fatalf("thread1 = %#v", thread1)
	}
	thread2 := postJSON[server.Thread](t, ts.URL+"/v1/threads", `{"thread_id":"t-other","metadata":{"assistant_id":"a2"}}`, http.StatusCreated)
	if thread2.ID != "t-other" {
		t.Fatalf("thread2 = %#v", thread2)
	}

	deleteReq, err := http.NewRequest(http.MethodDelete, ts.URL+"/v1/assistants/a1?delete_threads=true", nil)
	if err != nil {
		t.Fatalf("new delete request: %v", err)
	}
	deleteResp, err := http.DefaultClient.Do(deleteReq)
	if err != nil {
		t.Fatalf("delete request: %v", err)
	}
	defer func() { _ = deleteResp.Body.Close() }()
	if deleteResp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d", deleteResp.StatusCode)
	}

	assertErrorStatus(t, ts.URL+"/v1/threads/t-a1", http.StatusNotFound)
	if got := getJSON[server.Thread](t, ts.URL+"/v1/threads/t-other", http.StatusOK); got.ID != "t-other" {
		t.Fatalf("remaining thread = %#v", got)
	}
	assertErrorStatus(t, ts.URL+"/v1/assistants/a1", http.StatusNotFound)

	searchReq2, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/assistants/search", strings.NewReader(`{"limit":5,"offset":1}`))
	if err != nil {
		t.Fatalf("new paged search request: %v", err)
	}
	searchReq2.Header.Set("Content-Type", "application/json")
	searchResp2, err := http.DefaultClient.Do(searchReq2)
	if err != nil {
		t.Fatalf("paged search request: %v", err)
	}
	defer func() { _ = searchResp2.Body.Close() }()
	if got := searchResp2.Header.Get("X-Pagination-Next"); got != "" {
		t.Fatalf("paged X-Pagination-Next = %q, want empty", got)
	}

	badCreate := assertJSONStatusBody(t, http.MethodPost, ts.URL+"/v1/assistants", `{"assistant_id":"a2","if_exists":"merge"}`, http.StatusBadRequest)
	if !strings.Contains(badCreate, "invalid if_exists") {
		t.Fatalf("bad create body = %q", badCreate)
	}

	badDeleteURL := ts.URL + "/v1/assistants/a2?delete_threads=notabool"
	badDeleteReq, err := http.NewRequest(http.MethodDelete, badDeleteURL, nil)
	if err != nil {
		t.Fatalf("new bad delete request: %v", err)
	}
	badDeleteResp, err := http.DefaultClient.Do(badDeleteReq)
	if err != nil {
		t.Fatalf("bad delete request: %v", err)
	}
	defer func() { _ = badDeleteResp.Body.Close() }()
	if badDeleteResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad delete status = %d", badDeleteResp.StatusCode)
	}
}

func TestServer_TopLevelRunsAndRunStreaming(t *testing.T) {
	t.Parallel()

	s := server.New(server.Options{Runner: testRunner{}})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	run := postJSON[server.Run](t, ts.URL+"/v1/runs", `{"assistant_id":"a1","input":{"message":"hello"}}`, http.StatusAccepted)
	if run.ThreadID == "" {
		t.Fatal("expected top-level run to set thread_id")
	}
	createResp, err := http.Post(ts.URL+"/v1/runs", "application/json", strings.NewReader(`{"assistant_id":"a1","input":{"message":"headers"}}`))
	if err != nil {
		t.Fatalf("top-level run request: %v", err)
	}
	defer func() { _ = createResp.Body.Close() }()
	if createResp.StatusCode != http.StatusAccepted {
		t.Fatalf("top-level run status = %d", createResp.StatusCode)
	}
	if got := createResp.Header.Get("Content-Location"); got == "" || !strings.HasPrefix(got, "/v1/threads/") || !strings.Contains(got, "/runs/") {
		t.Fatalf("top-level Content-Location = %q", got)
	}

	final := waitRun(t, ts.URL, run.ThreadID, run.ID)
	if final.Status != server.RunStatusSuccess {
		t.Fatalf("status = %s", final.Status)
	}

	join := getJSON[map[string]any](t, ts.URL+"/v1/threads/"+run.ThreadID+"/runs/"+run.ID+"/join", http.StatusOK)
	if join["last_message"] != "hello" {
		t.Fatalf("join values = %#v", join)
	}

	body := `{"assistant_id":"a1","input":{"message":"stream"},"stream_mode":["values"]}`
	streamBody := postSSE(t, ts.URL+"/v1/runs/stream", body, http.StatusOK)
	if !strings.Contains(streamBody, "event: values") {
		t.Fatalf("unexpected top-level stream body: %q", streamBody)
	}

	thread := postJSON[server.Thread](t, ts.URL+"/v1/threads", `{}`, http.StatusCreated)
	threadRunResp, err := http.Post(ts.URL+"/v1/threads/"+thread.ID+"/runs", "application/json", strings.NewReader(`{"assistant_id":"a1","input":{"message":"thread"}}`))
	if err != nil {
		t.Fatalf("thread run request: %v", err)
	}
	defer func() { _ = threadRunResp.Body.Close() }()
	if threadRunResp.StatusCode != http.StatusAccepted {
		t.Fatalf("thread run status = %d", threadRunResp.StatusCode)
	}
	if got := threadRunResp.Header.Get("Content-Location"); got == "" || !strings.HasPrefix(got, "/v1/threads/"+thread.ID+"/runs/") {
		t.Fatalf("thread run Content-Location = %q", got)
	}
	threadStream := postSSE(t, ts.URL+"/v1/threads/"+thread.ID+"/runs/stream", body, http.StatusOK)
	if !strings.Contains(threadStream, "event: values") {
		t.Fatalf("unexpected thread stream body: %q", threadStream)
	}
}

func TestServer_RunStreamEmitsIncrementalUpdatesBeforeCompletion(t *testing.T) {
	t.Parallel()

	runner := blockingStreamRunner{started: make(chan struct{}), release: make(chan struct{})}
	s := server.New(server.Options{Runner: runner, Checkpointer: checkpoint.NewInMemorySaver()})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	thread := postJSON[server.Thread](t, ts.URL+"/v1/threads", `{}`, http.StatusCreated)
	run := postJSON[server.Run](t, ts.URL+"/v1/threads/"+thread.ID+"/runs", `{"assistant_id":"a1","input":{"message":"slow"}}`, http.StatusAccepted)

	select {
	case <-runner.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runner start")
	}

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/threads/"+thread.ID+"/runs/"+run.ID+"/stream?stream_mode=updates", nil)
	if err != nil {
		t.Fatalf("new stream request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.Contains(got, "text/event-stream") {
		t.Fatalf("stream content-type = %q", got)
	}
	if got := resp.Header.Get("Location"); !strings.Contains(got, "stream_mode=updates") {
		t.Fatalf("stream location = %q", got)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)

	deadline := time.Now().Add(2 * time.Second)
	for {
		eventName, eventData := readSSEEvent(t, scanner, 2*time.Second)
		if eventName == "metadata" {
			continue
		}
		if eventName != "updates" {
			t.Fatalf("incremental event = %q, want updates", eventName)
		}
		if eventData["status"] == string(server.RunStatusRunning) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("did not receive running update before completion: %#v", eventData)
		}
	}

	close(runner.release)
	final := waitRun(t, ts.URL, thread.ID, run.ID)
	if final.Status != server.RunStatusSuccess {
		t.Fatalf("final status = %s", final.Status)
	}
}

func TestServer_RunStreamModeAwareReconnectAndSupportedModes(t *testing.T) {
	t.Parallel()

	s := server.New(server.Options{Runner: richStreamRunner{}, Checkpointer: checkpoint.NewInMemorySaver()})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	thread := postJSON[server.Thread](t, ts.URL+"/v1/threads", `{}`, http.StatusCreated)
	run := postJSON[server.Run](t, ts.URL+"/v1/threads/"+thread.ID+"/runs", `{"assistant_id":"a1","input":{"message":"hello"}}`, http.StatusAccepted)
	_ = waitRun(t, ts.URL, thread.ID, run.ID)

	streamReq, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/threads/"+thread.ID+"/runs/"+run.ID+"/stream?stream_mode=values,updates,messages,tasks,checkpoints,debug,custom,metadata", nil)
	if err != nil {
		t.Fatalf("new stream request: %v", err)
	}
	streamResp, err := http.DefaultClient.Do(streamReq)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	bodyBytes, err := io.ReadAll(streamResp.Body)
	if err != nil {
		t.Fatalf("read stream body: %v", err)
	}
	_ = streamResp.Body.Close()

	raw := string(bodyBytes)
	for _, event := range []string{"values", "updates", "messages", "tasks", "checkpoints", "debug", "custom", "metadata"} {
		if !strings.Contains(raw, "event: "+event) {
			t.Fatalf("expected %q event in stream body, got %q", event, raw)
		}
	}
	if !strings.Contains(raw, "event: end") {
		t.Fatalf("expected end event in stream body, got %q", raw)
	}

	reconnectReq, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/threads/"+thread.ID+"/runs/"+run.ID+"/stream?stream_mode=updates", nil)
	if err != nil {
		t.Fatalf("new reconnect request: %v", err)
	}
	reconnectReq.Header.Set("Last-Event-ID", "2")
	reconnectResp, err := http.DefaultClient.Do(reconnectReq)
	if err != nil {
		t.Fatalf("reconnect request: %v", err)
	}
	reconnectBytes, err := io.ReadAll(reconnectResp.Body)
	if err != nil {
		t.Fatalf("read reconnect body: %v", err)
	}
	_ = reconnectResp.Body.Close()

	reconnectRaw := string(reconnectBytes)
	if strings.Contains(reconnectRaw, "event: metadata") {
		t.Fatalf("reconnect body should skip metadata, got %q", reconnectRaw)
	}
	if !strings.Contains(reconnectRaw, "event: updates") {
		t.Fatalf("reconnect body missing resumed updates event, got %q", reconnectRaw)
	}
	if !strings.Contains(reconnectRaw, "event: end") {
		t.Fatalf("reconnect body missing end event, got %q", reconnectRaw)
	}
}

func TestServer_GlobalStoreInfoAndCrons(t *testing.T) {
	t.Parallel()

	s := server.New(server.Options{})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	info := getJSON[server.Info](t, ts.URL+"/v1/info", http.StatusOK)
	if info.Capabilities["assistants"] != true {
		t.Fatalf("capabilities = %#v", info.Capabilities)
	}
	if info.Capabilities["assistant_graph_introspection"] != false {
		t.Fatalf("assistant graph capability = %#v", info.Capabilities)
	}
	if info.Capabilities["store_ttl"] != true {
		t.Fatalf("store ttl capability = %#v", info.Capabilities)
	}
	if info.Capabilities["store_query_filter"] != true || info.Capabilities["store_refresh_ttl"] != true || info.Capabilities["store_max_depth"] != true {
		t.Fatalf("store capabilities = %#v", info.Capabilities)
	}
	if info.Capabilities["store_vector_search"] != true {
		t.Fatalf("store vector capability = %#v", info.Capabilities)
	}

	putJSONNoResp(t, ts.URL+"/v1/store/items", `{"namespace":["profiles","user"],"key":"name","value":{"first":"Ada"}}`, http.StatusNoContent)
	ttlResp, err := http.NewRequest(http.MethodPut, ts.URL+"/v1/store/items", strings.NewReader(`{"namespace":["profiles","session"],"key":"token","value":{"token":"abc","score":7,"meta":{"tier":"gold"}},"ttl":1}`))
	if err != nil {
		t.Fatalf("new ttl put request: %v", err)
	}
	ttlResp.Header.Set("Content-Type", "application/json")
	ttlHTTPResp, err := http.DefaultClient.Do(ttlResp)
	if err != nil {
		t.Fatalf("ttl put request: %v", err)
	}
	defer func() { _ = ttlHTTPResp.Body.Close() }()
	if ttlHTTPResp.StatusCode != http.StatusNoContent {
		t.Fatalf("ttl put status = %d", ttlHTTPResp.StatusCode)
	}
	item := getJSON[server.StoreItem](t, ts.URL+"/v1/store/items?namespace=profiles.user&key=name", http.StatusOK)
	if item.Key != "name" || item.Value["first"] != "Ada" {
		t.Fatalf("store item = %#v", item)
	}
	time.Sleep(500 * time.Millisecond)
	item = getJSON[server.StoreItem](t, ts.URL+"/v1/store/items?namespace=profiles.session&key=token&refresh_ttl=true", http.StatusOK)
	if item.Key != "token" {
		t.Fatalf("refreshed item = %#v", item)
	}
	time.Sleep(700 * time.Millisecond)
	item = getJSON[server.StoreItem](t, ts.URL+"/v1/store/items?namespace=profiles.session&key=token", http.StatusOK)
	if item.Key != "token" {
		t.Fatalf("refreshed item after extension = %#v", item)
	}
	time.Sleep(1200 * time.Millisecond)
	assertErrorStatus(t, ts.URL+"/v1/store/items?namespace=profiles.session&key=token&refresh_ttl=false", http.StatusNotFound)

	putJSONNoResp(t, ts.URL+"/v1/store/items", `{"namespace":["profiles","user"],"key":"bio","value":{"first":"Ada","score":9,"meta":{"tier":"gold"}},"index":{"fields":["first","score"]}}`, http.StatusNoContent)
	putJSONNoResp(t, ts.URL+"/v1/store/items", `{"namespace":["profiles","org"],"key":"team","value":{"first":"Grace","score":3,"meta":{"tier":"silver"}}}`, http.StatusNoContent)

	search := postJSON[map[string][]server.StoreItem](t, ts.URL+"/v1/store/items/search", `{"namespace_prefix":["profiles"],"query":"ada","filter":{"score":{"$gte":7},"meta":{"tier":"gold"}}}`, http.StatusOK)
	if len(search["items"]) != 1 {
		t.Fatalf("store search = %#v", search)
	}

	putJSONNoResp(t, ts.URL+"/v1/store/items", `{"namespace":["profiles","semantic"],"key":"indexed","value":{"text":"quantum retrieval memory"},"index":{"fields":["text"]}}`, http.StatusNoContent)
	putJSONNoResp(t, ts.URL+"/v1/store/items", `{"namespace":["profiles","semantic"],"key":"plain","value":{"text":"quantum retrieval memory"},"index":false}`, http.StatusNoContent)
	semantic := postJSON[map[string][]server.StoreItem](t, ts.URL+"/v1/store/items/search", `{"namespace_prefix":["profiles","semantic"],"query":"quantum retrieval"}`, http.StatusOK)
	if len(semantic["items"]) != 1 || semantic["items"][0].Key != "indexed" {
		t.Fatalf("semantic search = %#v", semantic)
	}
	refreshedSearch := postJSON[map[string][]server.StoreItem](t, ts.URL+"/v1/store/items/search", `{"namespace_prefix":["profiles"],"query":"ada","refresh_ttl":true}`, http.StatusOK)
	if len(refreshedSearch["items"]) != 2 {
		t.Fatalf("refreshed store search = %#v", refreshedSearch)
	}

	ns := postJSON[map[string][][]string](t, ts.URL+"/v1/store/namespaces", `{"prefix":["profiles"],"max_depth":1,"limit":10}`, http.StatusOK)
	if len(ns["namespaces"]) != 1 || len(ns["namespaces"][0]) != 1 || ns["namespaces"][0][0] != "profiles" {
		t.Fatalf("store namespaces = %#v", ns)
	}
	ns = postJSON[map[string][][]string](t, ts.URL+"/v1/store/namespaces", `{"suffix":["user"],"limit":10}`, http.StatusOK)
	if len(ns["namespaces"]) != 1 || strings.Join(ns["namespaces"][0], ".") != "profiles.user" {
		t.Fatalf("store suffix namespaces = %#v", ns)
	}

	deleteJSONNoResp(t, ts.URL+"/v1/store/items", `{"namespace":["profiles","user"],"key":"name"}`, http.StatusNoContent)

	thread := postJSON[server.Thread](t, ts.URL+"/v1/threads", `{}`, http.StatusCreated)
	cron := postJSON[server.Cron](t, ts.URL+"/v1/runs/crons", `{"assistant_id":"a1","schedule":"* * * * *"}`, http.StatusCreated)
	if cron.ID == "" {
		t.Fatal("expected cron id")
	}

	threadCron := postJSON[server.Cron](t, ts.URL+"/v1/threads/"+thread.ID+"/runs/crons", `{"assistant_id":"a1","schedule":"*/5 * * * *"}`, http.StatusCreated)
	if threadCron.ThreadID != thread.ID {
		t.Fatalf("thread cron thread_id = %q", threadCron.ThreadID)
	}

	updated := patchJSON[server.Cron](t, ts.URL+"/v1/runs/crons/"+cron.ID, `{"enabled":false}`, http.StatusOK)
	if updated.Enabled {
		t.Fatalf("expected cron enabled=false, got %#v", updated)
	}

	cronSearch := postJSON[[]server.Cron](t, ts.URL+"/v1/runs/crons/search", `{"assistant_id":"a1"}`, http.StatusOK)
	if len(cronSearch) < 1 {
		t.Fatalf("cron search = %#v", cronSearch)
	}

	cronCount := postJSON[map[string]int](t, ts.URL+"/v1/runs/crons/count", `{}`, http.StatusOK)
	if cronCount["count"] != 2 {
		t.Fatalf("cron count = %#v", cronCount)
	}

	deleteNoBody(t, ts.URL+"/v1/runs/crons/"+cron.ID, http.StatusNoContent)
}

func TestServer_ThreadOperationsAndAPIKeyAuth(t *testing.T) {
	t.Parallel()

	s := server.New(server.Options{APIKey: "secret", Checkpointer: checkpoint.NewInMemorySaver()})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	thread := requestJSONWithHeaders[server.Thread](
		t,
		http.MethodPost,
		ts.URL+"/v1/threads",
		`{"thread_id":"t-auth","metadata":{"owner":"ops"}}`,
		http.StatusCreated,
		map[string]string{"Authorization": "Bearer secret"},
	)
	if thread.ID != "t-auth" {
		t.Fatalf("thread_id = %q", thread.ID)
	}

	_ = requestJSONWithHeaders[server.Thread](
		t,
		http.MethodPatch,
		ts.URL+"/v1/threads/"+thread.ID,
		`{"metadata":{"owner":"platform"}}`,
		http.StatusOK,
		map[string]string{"X-API-Key": "secret"},
	)

	search := requestJSONWithHeaders[[]server.Thread](
		t,
		http.MethodPost,
		ts.URL+"/v1/threads/search",
		`{"metadata":{"owner":"platform"},"ids":["t-auth"]}`,
		http.StatusOK,
		map[string]string{"Authorization": "Bearer secret"},
	)
	if len(search) != 1 || search[0].ID != thread.ID {
		t.Fatalf("thread search = %#v", search)
	}

	count := requestJSONWithHeaders[map[string]int](
		t,
		http.MethodPost,
		ts.URL+"/v1/threads/count",
		`{"metadata":{"owner":"platform"}}`,
		http.StatusOK,
		map[string]string{"Authorization": "Bearer secret"},
	)
	if count["count"] != 1 {
		t.Fatalf("thread count = %#v", count)
	}

	state := requestJSONWithHeaders[map[string]any](
		t,
		http.MethodPost,
		ts.URL+"/v1/threads/"+thread.ID+"/state",
		`{"values":{"stage":"ready"}}`,
		http.StatusOK,
		map[string]string{"Authorization": "Bearer secret"},
	)
	checkpoint, ok := state["checkpoint"].(map[string]any)
	if !ok || checkpoint["checkpoint_id"] == "" {
		t.Fatalf("update state response = %#v", state)
	}

	history := requestJSONWithHeaders[[]server.ThreadState](
		t,
		http.MethodPost,
		ts.URL+"/v1/threads/"+thread.ID+"/history",
		`{"limit":10}`,
		http.StatusOK,
		map[string]string{"Authorization": "Bearer secret"},
	)
	if len(history) == 0 {
		t.Fatalf("expected state history, got %#v", history)
	}

	requestJSONWithHeaders[map[string]any](
		t,
		http.MethodPost,
		ts.URL+"/v1/threads/"+thread.ID+"/state",
		`{"checkpoint":{"checkpoint_id":"`+checkpoint["checkpoint_id"].(string)+`"},"values":{"owner":"ada"},"as_node":"reviewer"}`,
		http.StatusOK,
		map[string]string{"Authorization": "Bearer secret"},
	)
	latest := requestJSONWithHeaders[server.ThreadState](
		t,
		http.MethodGet,
		ts.URL+"/v1/threads/"+thread.ID+"/state",
		``,
		http.StatusOK,
		map[string]string{"Authorization": "Bearer secret"},
	)
	if latest.Values["stage"] != "ready" || latest.Values["owner"] != "ada" {
		t.Fatalf("merged state = %#v", latest)
	}
	if latest.Metadata["as_node"] != "reviewer" {
		t.Fatalf("latest metadata = %#v", latest.Metadata)
	}
	filteredHistory := requestJSONWithHeaders[[]server.ThreadState](
		t,
		http.MethodPost,
		ts.URL+"/v1/threads/"+thread.ID+"/history",
		`{"limit":10,"metadata":{"as_node":"reviewer"}}`,
		http.StatusOK,
		map[string]string{"Authorization": "Bearer secret"},
	)
	if len(filteredHistory) != 1 || filteredHistory[0].Metadata["as_node"] != "reviewer" {
		t.Fatalf("filtered history = %#v", filteredHistory)
	}

	copyResp := requestJSONWithHeaders[map[string]any](
		t,
		http.MethodPost,
		ts.URL+"/v1/threads/"+thread.ID+"/copy",
		`{}`,
		http.StatusCreated,
		map[string]string{"Authorization": "Bearer secret"},
	)
	if copyResp["thread_id"] == "" || copyResp["thread_id"] == thread.ID {
		t.Fatalf("thread copy = %#v", copyResp)
	}

	prune := requestJSONWithHeaders[map[string]int](
		t,
		http.MethodPost,
		ts.URL+"/v1/threads/prune",
		`{"thread_ids":["t-auth"],"strategy":"delete"}`,
		http.StatusOK,
		map[string]string{"Authorization": "Bearer secret"},
	)
	if prune["pruned_count"] != 1 {
		t.Fatalf("thread prune = %#v", prune)
	}

	deleteReq, err := http.NewRequest(http.MethodDelete, ts.URL+"/v1/threads/"+thread.ID, nil)
	if err != nil {
		t.Fatalf("new delete request: %v", err)
	}
	deleteReq.Header.Set("Authorization", "Bearer secret")
	deleteResp, err := http.DefaultClient.Do(deleteReq)
	if err != nil {
		t.Fatalf("delete request: %v", err)
	}
	defer func() { _ = deleteResp.Body.Close() }()
	if deleteResp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d", deleteResp.StatusCode)
	}

	unauthReq, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/info", nil)
	if err != nil {
		t.Fatalf("new unauth request: %v", err)
	}
	unauthResp, err := http.DefaultClient.Do(unauthReq)
	if err != nil {
		t.Fatalf("unauth request: %v", err)
	}
	defer func() { _ = unauthResp.Body.Close() }()
	if unauthResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthResp.StatusCode)
	}
}

func patchJSON[T any](t *testing.T, url, body string, wantStatus int) T {
	t.Helper()
	return requestJSON[T](t, http.MethodPatch, url, body, wantStatus)
}

func assertErrorStatus(t *testing.T, url string, wantStatus int) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d", resp.StatusCode, wantStatus)
	}
}

func assertJSONStatusBody(t *testing.T, method, url, body string, wantStatus int) string {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d", resp.StatusCode, wantStatus)
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	msg, _ := payload["error"].(string)
	return msg
}

func requestJSON[T any](t *testing.T, method, url, body string, wantStatus int) T {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d", resp.StatusCode, wantStatus)
	}
	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

func requestJSONWithHeaders[T any](t *testing.T, method, url, body string, wantStatus int, headers map[string]string) T {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d", resp.StatusCode, wantStatus)
	}
	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

func postSSE(t *testing.T, url, body string, wantStatus int) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d", resp.StatusCode, wantStatus)
	}
	b := new(strings.Builder)
	buf := make([]byte, 4096)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			_, _ = b.Write(buf[:n])
		}
		if readErr != nil {
			break
		}
	}
	return b.String()
}

func deleteNoBody(t *testing.T, url string, wantStatus int) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d", resp.StatusCode, wantStatus)
	}
}

func deleteJSONNoResp(t *testing.T, url, body string, wantStatus int) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d", resp.StatusCode, wantStatus)
	}
}

func readSSEEvent(t *testing.T, scanner *bufio.Scanner, timeout time.Duration) (string, map[string]any) {
	t.Helper()

	var event string
	var data strings.Builder
	deadline := time.After(timeout)
	for {
		lineCh := make(chan string, 1)
		errCh := make(chan error, 1)
		go func() {
			if scanner.Scan() {
				lineCh <- scanner.Text()
				return
			}
			errCh <- scanner.Err()
		}()

		select {
		case <-deadline:
			t.Fatal("timed out waiting for SSE event")
		case err := <-errCh:
			if err != nil {
				t.Fatalf("scan SSE event: %v", err)
			}
			t.Fatal("stream closed before next SSE event")
		case line := <-lineCh:
			if line == "" {
				if data.Len() == 0 {
					continue
				}
				payload := map[string]any{}
				if err := json.Unmarshal([]byte(data.String()), &payload); err != nil {
					t.Fatalf("decode SSE payload: %v", err)
				}
				return event, payload
			}
			if strings.HasPrefix(line, "event:") {
				event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
				continue
			}
			if strings.HasPrefix(line, "data:") {
				if data.Len() > 0 {
					data.WriteString("\n")
				}
				data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
				continue
			}
			if strings.HasPrefix(line, "id:") {
				if _, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "id:"))); err != nil {
					t.Fatalf("invalid SSE id line: %q", line)
				}
			}
		}
	}
}
