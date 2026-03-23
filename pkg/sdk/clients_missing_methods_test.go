package sdk_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SkinnyPeteTheGiraffe/gographgo/pkg/sdk"
)

func TestThreadsClientMissingMethods(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/threads/t1":
			writeJSON(t, w, map[string]any{"thread_id": "t1"})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/threads/t1":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/threads/search":
			writeJSON(t, w, []map[string]any{{"thread_id": "t1"}})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/threads/count":
			writeJSON(t, w, map[string]any{"count": 7})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/threads/t1/copy":
			writeJSON(t, w, map[string]any{"thread_id": "t2"})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/threads/prune":
			writeJSON(t, w, map[string]any{"pruned_count": 2})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/threads/t1/state":
			writeJSON(t, w, map[string]any{"checkpoint": map[string]any{"checkpoint_id": "cp1"}})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/threads/t1/history":
			writeJSON(t, w, []map[string]any{{"thread_id": "t1", "values": map[string]any{"x": 1}}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/threads/t1/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("id: evt-1\nevent: updates\ndata: {\"delta\":1}\n\n"))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New() error = %v", err)
	}

	ctx := context.Background()
	if _, err := client.Threads.Update(ctx, "t1", sdk.ThreadUpdateRequest{Metadata: map[string]any{"a": 1}}); err != nil {
		t.Fatalf("Threads.Update() error = %v", err)
	}
	if err := client.Threads.Delete(ctx, "t1"); err != nil {
		t.Fatalf("Threads.Delete() error = %v", err)
	}
	if got, err := client.Threads.Search(ctx, sdk.ThreadSearchRequest{Limit: 1}); err != nil || len(got) != 1 {
		t.Fatalf("Threads.Search() = %v, %v", got, err)
	}
	if got, err := client.Threads.Count(ctx, sdk.ThreadCountRequest{}); err != nil || got != 7 {
		t.Fatalf("Threads.Count() = %d, %v", got, err)
	}
	if got, err := client.Threads.Copy(ctx, "t1"); err != nil || got["thread_id"] != "t2" {
		t.Fatalf("Threads.Copy() = %v, %v", got, err)
	}
	if got, err := client.Threads.Prune(ctx, sdk.ThreadPruneRequest{ThreadIDs: []string{"t1"}}); err != nil || got.PrunedCount != 2 {
		t.Fatalf("Threads.Prune() = %v, %v", got, err)
	}
	if got, err := client.Threads.UpdateState(ctx, "t1", sdk.ThreadUpdateStateRequest{Values: map[string]any{"x": 1}}); err != nil || got.Checkpoint["checkpoint_id"] != "cp1" {
		t.Fatalf("Threads.UpdateState() = %v, %v", got, err)
	}
	if got, err := client.Threads.GetHistory(ctx, "t1", sdk.ThreadHistoryRequest{Limit: 1}); err != nil || len(got) != 1 {
		t.Fatalf("Threads.GetHistory() = %v, %v", got, err)
	}

	parts, errs := client.Threads.JoinStream(ctx, "t1", sdk.ThreadJoinStreamRequest{StreamMode: []string{"updates"}, LastEventID: "evt-0"})
	select {
	case part := <-parts:
		if part.Event != "updates" || part.ID != "evt-1" {
			t.Fatalf("unexpected stream part: %+v", part)
		}
	case err := <-errs:
		t.Fatalf("Threads.JoinStream() error = %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for thread stream")
	}
}

func TestClientGetInfo(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/info" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		writeJSON(t, w, map[string]any{"version": "v1", "capabilities": map[string]any{"assistants": true}})
	}))
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New() error = %v", err)
	}

	info, err := client.GetInfo(context.Background())
	if err != nil {
		t.Fatalf("GetInfo() error = %v", err)
	}
	if info.Version != "v1" || info.Capabilities["assistants"] != true {
		t.Fatalf("GetInfo() = %#v", info)
	}
}

func TestRunsClientMissingMethods(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/threads/t1/runs/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: values\ndata: {\"a\":1}\n\n"))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/runs/batch":
			writeJSON(t, w, []map[string]any{{"run_id": "r1", "thread_id": "t1", "status": "pending", "created_at": time.Now().UTC()}})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/threads/t1/runs/r1/cancel":
			if r.URL.Query().Get("wait") != "true" || r.URL.Query().Get("action") != "interrupt" {
				t.Fatalf("unexpected cancel query: %s", r.URL.RawQuery)
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/runs/cancel":
			if r.URL.Query().Get("action") != "rollback" {
				t.Fatalf("unexpected cancel_many query: %s", r.URL.RawQuery)
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/threads/t1/runs/r1/join":
			writeJSON(t, w, map[string]any{"done": true})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/threads/t1/runs/r1/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: debug\ndata: {\"ok\":true}\n\n"))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/threads/t1/runs/r1":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New() error = %v", err)
	}

	ctx := context.Background()
	streamParts, streamErrs := client.Runs.Stream(ctx, "t1", sdk.RunStreamRequest{AssistantID: "a1", StreamMode: []string{"values"}})
	select {
	case part := <-streamParts:
		if part.Event != "values" {
			t.Fatalf("Runs.Stream() event = %q", part.Event)
		}
	case err := <-streamErrs:
		t.Fatalf("Runs.Stream() error = %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for run stream")
	}

	if got, err := client.Runs.CreateBatch(ctx, []sdk.RunCreateRequest{{AssistantID: "a1"}}); err != nil || len(got) != 1 {
		t.Fatalf("Runs.CreateBatch() = %v, %v", got, err)
	}
	if err := client.Runs.Cancel(ctx, "t1", "r1", sdk.RunCancelOptions{Wait: true, Action: "interrupt"}); err != nil {
		t.Fatalf("Runs.Cancel() error = %v", err)
	}
	if err := client.Runs.CancelMany(ctx, sdk.RunCancelManyRequest{ThreadID: "t1", RunIDs: []string{"r1"}, Action: "rollback"}); err != nil {
		t.Fatalf("Runs.CancelMany() error = %v", err)
	}
	if got, err := client.Runs.Join(ctx, "t1", "r1"); err != nil || got["done"] != true {
		t.Fatalf("Runs.Join() = %v, %v", got, err)
	}
	joinParts, joinErrs := client.Runs.JoinStream(ctx, "t1", "r1", sdk.RunJoinStreamRequest{StreamMode: []string{"debug"}, CancelOnDisconnect: true, LastEventID: "e0"})
	select {
	case part := <-joinParts:
		if part.Event != "debug" {
			t.Fatalf("Runs.JoinStream() event = %q", part.Event)
		}
	case err := <-joinErrs:
		t.Fatalf("Runs.JoinStream() error = %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for join stream")
	}
	if err := client.Runs.Delete(ctx, "t1", "r1"); err != nil {
		t.Fatalf("Runs.Delete() error = %v", err)
	}
}

func TestAssistantsAndCronClientsExistAndRoute(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/assistants/a1":
			writeJSON(t, w, map[string]any{"assistant_id": "a1", "created_at": time.Now().UTC(), "updated_at": time.Now().UTC()})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/assistants/a1/graph":
			if r.URL.Query().Get("xray") != "true" {
				t.Fatalf("unexpected graph query: %q", r.URL.RawQuery)
			}
			writeJSON(t, w, map[string]any{
				"assistant_id": "a1",
				"graph_id":     "g1",
				"graph": map[string]any{
					"name":  "g1",
					"nodes": []map[string]any{{"name": "__start__"}, {"name": "router"}, {"name": "__end__"}},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/assistants/a1/schemas":
			writeJSON(t, w, map[string]any{
				"assistant_id": "a1",
				"graph_id":     "g1",
				"input":        map[string]any{"type": "object"},
				"output":       map[string]any{"type": "object"},
				"context":      map[string]any{"type": "object"},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/assistants/a1/subgraphs/router":
			if r.URL.Query().Get("recurse") != "true" {
				t.Fatalf("unexpected subgraphs query: %q", r.URL.RawQuery)
			}
			writeJSON(t, w, map[string]any{
				"assistant_id": "a1",
				"graph_id":     "g1",
				"subgraphs": []map[string]any{{
					"name":        "decision",
					"parent_node": "router",
					"namespace":   "router/decision",
				}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/assistants":
			body := readBodyMap(t, r.Body)
			if body["if_exists"] != "do_nothing" {
				t.Fatalf("unexpected create body: %#v", body)
			}
			writeJSON(t, w, map[string]any{"assistant_id": "a1", "created_at": time.Now().UTC(), "updated_at": time.Now().UTC()})
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/assistants/a1":
			writeJSON(t, w, map[string]any{"assistant_id": "a1", "created_at": time.Now().UTC(), "updated_at": time.Now().UTC()})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/assistants/a1":
			if r.URL.Query().Get("delete_threads") != "true" {
				t.Fatalf("expected delete_threads=true, got %q", r.URL.RawQuery)
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/assistants/search":
			body := readBodyMap(t, r.Body)
			if body["limit"] != float64(1) {
				t.Fatalf("unexpected search body: %#v", body)
			}
			w.Header().Set("X-Pagination-Next", "cursor-2")
			writeJSON(t, w, []map[string]any{{"assistant_id": "a1", "created_at": time.Now().UTC(), "updated_at": time.Now().UTC()}})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/assistants/count":
			writeJSON(t, w, map[string]any{"count": 4})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/assistants/a1/versions":
			body := readBodyMap(t, r.Body)
			metadata, _ := body["metadata"].(map[string]any)
			if metadata["stage"] != "prod" {
				t.Fatalf("unexpected versions body: %#v", body)
			}
			writeJSON(t, w, []map[string]any{{"version": 1, "created_at": time.Now().UTC()}})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/assistants/a1/latest":
			writeJSON(t, w, map[string]any{"assistant_id": "a1", "created_at": time.Now().UTC(), "updated_at": time.Now().UTC()})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/runs/crons":
			writeJSON(t, w, map[string]any{"cron_id": "c1", "created_at": time.Now().UTC(), "updated_at": time.Now().UTC()})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/threads/t1/runs/crons":
			writeJSON(t, w, map[string]any{"cron_id": "c1", "thread_id": "t1", "created_at": time.Now().UTC(), "updated_at": time.Now().UTC()})
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/runs/crons/c1":
			writeJSON(t, w, map[string]any{"cron_id": "c1", "created_at": time.Now().UTC(), "updated_at": time.Now().UTC()})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/runs/crons/search":
			writeJSON(t, w, []map[string]any{{"cron_id": "c1", "created_at": time.Now().UTC(), "updated_at": time.Now().UTC()}})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/runs/crons/count":
			writeJSON(t, w, map[string]any{"count": 3})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/runs/crons/c1":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New() error = %v", err)
	}

	ctx := context.Background()
	if _, err := client.Assistants.Get(ctx, "a1"); err != nil {
		t.Fatalf("Assistants.Get() error = %v", err)
	}
	if graphResp, err := client.Assistants.GetGraph(ctx, "a1", true); err != nil || graphResp["assistant_id"] != "a1" {
		t.Fatalf("Assistants.GetGraph() = %v, %v", graphResp, err)
	}
	if schemasResp, err := client.Assistants.GetSchemas(ctx, "a1"); err != nil || schemasResp["assistant_id"] != "a1" {
		t.Fatalf("Assistants.GetSchemas() = %v, %v", schemasResp, err)
	}
	if subgraphsResp, err := client.Assistants.GetSubgraphs(ctx, "a1", "router", true); err != nil || subgraphsResp["assistant_id"] != "a1" {
		t.Fatalf("Assistants.GetSubgraphs() = %v, %v", subgraphsResp, err)
	}
	if _, err := client.Assistants.Create(ctx, sdk.AssistantCreateRequest{GraphID: "g1", IfExists: "do_nothing"}); err != nil {
		t.Fatalf("Assistants.Create() error = %v", err)
	}
	if _, err := client.Assistants.Update(ctx, "a1", sdk.AssistantUpdateRequest{Name: "new"}); err != nil {
		t.Fatalf("Assistants.Update() error = %v", err)
	}
	if err := client.Assistants.Delete(ctx, "a1", true); err != nil {
		t.Fatalf("Assistants.Delete() error = %v", err)
	}
	if got, err := client.Assistants.Search(ctx, sdk.AssistantSearchRequest{Limit: 1}); err != nil || len(got) != 1 {
		t.Fatalf("Assistants.Search() = %v, %v", got, err)
	}
	if got, err := client.Assistants.SearchObject(ctx, sdk.AssistantSearchRequest{Limit: 1}); err != nil || len(got.Assistants) != 1 || got.Next != "cursor-2" {
		t.Fatalf("Assistants.SearchObject() = %#v, %v", got, err)
	}
	if got, err := client.Assistants.Count(ctx, sdk.AssistantCountRequest{}); err != nil || got != 4 {
		t.Fatalf("Assistants.Count() = %d, %v", got, err)
	}
	if got, err := client.Assistants.GetVersions(ctx, "a1", sdk.AssistantVersionRequest{Metadata: map[string]any{"stage": "prod"}, Limit: 1}); err != nil || len(got) != 1 {
		t.Fatalf("Assistants.GetVersions() = %v, %v", got, err)
	}
	if _, err := client.Assistants.SetLatest(ctx, "a1", 1); err != nil {
		t.Fatalf("Assistants.SetLatest() error = %v", err)
	}

	if _, err := client.Cron.Create(ctx, sdk.CronCreateRequest{AssistantID: "a1", Schedule: "* * * * *"}); err != nil {
		t.Fatalf("Cron.Create() error = %v", err)
	}
	if _, err := client.Cron.CreateForThread(ctx, "t1", sdk.CronCreateRequest{AssistantID: "a1", Schedule: "* * * * *"}); err != nil {
		t.Fatalf("Cron.CreateForThread() error = %v", err)
	}
	if _, err := client.Cron.Update(ctx, "c1", sdk.CronUpdateRequest{Schedule: "0 * * * *"}); err != nil {
		t.Fatalf("Cron.Update() error = %v", err)
	}
	if got, err := client.Cron.Search(ctx, sdk.CronSearchRequest{Limit: 1}); err != nil || len(got) != 1 {
		t.Fatalf("Cron.Search() = %v, %v", got, err)
	}
	if got, err := client.Cron.Count(ctx, sdk.CronCountRequest{}); err != nil || got != 3 {
		t.Fatalf("Cron.Count() = %d, %v", got, err)
	}
	if err := client.Cron.Delete(ctx, "c1"); err != nil {
		t.Fatalf("Cron.Delete() error = %v", err)
	}
}

func TestStoreClientMissingMethods(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/v1/store/items":
			body := readBodyMap(t, r.Body)
			if body["index"] == nil {
				t.Fatalf("expected put body to include index, got %#v", body)
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/store/items":
			if r.URL.Query().Get("namespace") != "profiles.user" || r.URL.Query().Get("key") != "name" {
				t.Fatalf("unexpected get query: %s", r.URL.RawQuery)
			}
			writeJSON(t, w, map[string]any{"namespace": []string{"profiles", "user"}, "key": "name", "value": map[string]any{"first": "Ada"}})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/store/items":
			body := readBodyMap(t, r.Body)
			if body["key"] != "name" {
				t.Fatalf("unexpected delete key: %v", body)
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/store/items/search":
			writeJSON(t, w, map[string]any{"items": []map[string]any{{"namespace": []string{"profiles", "user"}, "key": "name", "value": map[string]any{"first": "Ada"}, "score": 0.97}}})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/store/namespaces":
			writeJSON(t, w, map[string]any{"namespaces": [][]string{{"profiles", "user"}}})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer ts.Close()

	client, err := sdk.New(sdk.Config{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("sdk.New() error = %v", err)
	}

	ctx := context.Background()
	if err := client.Store.PutItem(ctx, sdk.StoreItemPutRequest{Namespace: []string{"profiles", "user"}, Key: "name", Value: map[string]any{"first": "Ada"}, Index: map[string]any{"fields": []string{"first"}}}); err != nil {
		t.Fatalf("Store.PutItem() error = %v", err)
	}
	if got, err := client.Store.GetItem(ctx, []string{"profiles", "user"}, "name", nil); err != nil || got.Key != "name" {
		t.Fatalf("Store.GetItem() = %v, %v", got, err)
	}
	if err := client.Store.DeleteItem(ctx, []string{"profiles", "user"}, "name"); err != nil {
		t.Fatalf("Store.DeleteItem() error = %v", err)
	}
	if got, err := client.Store.SearchItems(ctx, sdk.StoreSearchRequest{NamespacePrefix: []string{"profiles"}}); err != nil || len(got.Items) != 1 {
		t.Fatalf("Store.SearchItems() = %v, %v", got, err)
	} else if got.Items[0].Score == nil || *got.Items[0].Score <= 0 {
		t.Fatalf("Store.SearchItems() score = %#v", got.Items[0].Score)
	}
	if got, err := client.Store.ListNamespaces(ctx, sdk.StoreNamespaceListRequest{Limit: 10}); err != nil || len(got.Namespaces) != 1 {
		t.Fatalf("Store.ListNamespaces() = %v, %v", got, err)
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("json encode error: %v", err)
	}
}

func readBodyMap(t *testing.T, body io.ReadCloser) map[string]any {
	t.Helper()
	defer func() { _ = body.Close() }()
	var out map[string]any
	if err := json.NewDecoder(body).Decode(&out); err != nil {
		t.Fatalf("decode body error: %v", err)
	}
	return out
}
