package cli

import (
	"archive/zip"
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateNew(t *testing.T) {
	origGet := httpGet
	t.Cleanup(func() { httpGet = origGet })

	buf := new(bytes.Buffer)
	zw := zip.NewWriter(buf)
	fw, err := zw.Create("repo-main/README.md")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := fw.Write([]byte("hello")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	httpGet = func(_ *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(bytes.NewReader(buf.Bytes())),
		}, nil
	}

	dir := t.TempDir()
	if err := CreateNew(dir, "new-langgraph-project-python", io.Discard); err != nil {
		t.Fatalf("CreateNew() error = %v", err)
	}

	b, err := os.ReadFile(filepath.Join(dir, "README.md"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(b) != "hello" {
		t.Fatalf("unexpected file contents = %q", string(b))
	}
}

func TestCreateNewInvalidTemplate(t *testing.T) {
	err := CreateNew(t.TempDir(), "missing-template", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected template error, got %v", err)
	}
}

func TestNewTemplateRequest_RejectsNonHTTPS(t *testing.T) {
	_, err := newTemplateRequest("http://github.com/langchain-ai/deep-agent-template/archive/refs/heads/main.zip")
	if err == nil || !strings.Contains(err.Error(), "must be https") {
		t.Fatalf("expected https validation error, got %v", err)
	}
}

func TestCreateNew_RejectsOversizedArchive(t *testing.T) {
	origGet := httpGet
	t.Cleanup(func() { httpGet = origGet })

	httpGet = func(_ *http.Request) (*http.Response, error) {
		body := bytes.Repeat([]byte("a"), maxTemplateArchiveBytes+1)
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(bytes.NewReader(body))}, nil
	}

	err := CreateNew(t.TempDir(), "new-langgraph-project-python", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected size limit error, got %v", err)
	}
}

func TestCreateNew_PropagatesRequestBuildError(t *testing.T) {
	origTemplates := templates
	templates = map[string]Template{"bad": {Description: "bad", URL: ":://bad"}}
	t.Cleanup(func() { templates = origTemplates })

	err := CreateNew(t.TempDir(), "bad", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "invalid template URL") {
		t.Fatal("expected invalid URL error")
	}
}
