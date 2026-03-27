package graph

import (
	"context"
	"time"
)

// NamespaceMatchType defines how a namespace pattern is evaluated.
type NamespaceMatchType string

const (
	// NamespaceMatchPrefix matches namespaces that start with the path.
	NamespaceMatchPrefix NamespaceMatchType = "prefix"

	// NamespaceMatchSuffix matches namespaces that end with the path.
	NamespaceMatchSuffix NamespaceMatchType = "suffix"
)

// NamespaceMatchCondition filters namespaces using prefix/suffix matching.
// Use `*` in Path as a single-segment wildcard.
type NamespaceMatchCondition struct {
	MatchType NamespaceMatchType
	Path      []string
}

// StoreItem is a single persisted store record.
type StoreItem struct {
	CreatedAt time.Time
	UpdatedAt time.Time
	Value     any
	Key       string
	Namespace []string
}

// StoreSearchItem is a search result record.
// Score is reserved for ranked search implementations and is nil for
// non-ranked in-memory search.
type StoreSearchItem struct {
	Score *float64
	StoreItem
}

// StoreGetOptions controls item retrieval behavior.
type StoreGetOptions struct {
	// RefreshTTL controls whether the TTL deadline is refreshed when the item is
	// returned. Nil means refresh.
	RefreshTTL *bool
}

// StorePutOptions controls item write behavior.
type StorePutOptions struct {
	// TTL configures expiration from last read/write access.
	// Nil means no expiration.
	TTL *time.Duration

	// Index controls vector indexing behavior for semantic search.
	// Nil means use the store default (index full item text).
	// false disables indexing for this item.
	// map{"fields": []string{...}} or []string indexes specific fields.
	Index any
}

// StoreSearchRequest defines store search criteria.
type StoreSearchRequest struct {
	Filter map[string]any
	// RefreshTTL controls whether TTL deadlines are refreshed for returned items.
	// Nil means refresh.
	RefreshTTL      *bool
	Query           string
	NamespacePrefix []string
	Limit           int
	Offset          int
}

// StoreNamespaceListRequest defines namespace listing criteria.
type StoreNamespaceListRequest struct {
	MaxDepth        *int
	MatchConditions []NamespaceMatchCondition
	Limit           int
	Offset          int
}

// StoreOp is a single store batch operation.
type StoreOp interface {
	isStoreOp()
}

// StoreGetOp retrieves one item.
type StoreGetOp struct {
	RefreshTTL *bool
	Key        string
	Namespace  []string
}

// StorePutOp stores or updates one item.
// Set Value to nil to delete the key.
type StorePutOp struct {
	Value     any
	Index     any
	TTL       *time.Duration
	Key       string
	Namespace []string
}

// StoreSearchOp searches for items under a namespace prefix.
type StoreSearchOp struct {
	Filter map[string]any
	// RefreshTTL controls whether TTL deadlines are refreshed for returned items.
	// Nil means refresh.
	RefreshTTL      *bool
	Query           string
	NamespacePrefix []string
	Limit           int
	Offset          int
}

// StoreListNamespacesOp lists namespaces using optional match conditions.
type StoreListNamespacesOp struct {
	MaxDepth        *int
	MatchConditions []NamespaceMatchCondition
	Limit           int
	Offset          int
}

func (StoreGetOp) isStoreOp()            {}
func (StorePutOp) isStoreOp()            {}
func (StoreSearchOp) isStoreOp()         {}
func (StoreListNamespacesOp) isStoreOp() {}

// BatchStore defines a full-featured store surface used by runtime memory and
// standalone store consumers.
type BatchStore interface {
	Batch(ctx context.Context, ops []StoreOp) ([]any, error)
	GetItem(ctx context.Context, namespace []string, key string, opts StoreGetOptions) (*StoreItem, error)
	PutItem(ctx context.Context, namespace []string, key string, value any, opts StorePutOptions) error
	DeleteItem(ctx context.Context, namespace []string, key string) error
	SearchItems(ctx context.Context, req StoreSearchRequest) ([]StoreSearchItem, error)
	ListNamespaces(ctx context.Context, req StoreNamespaceListRequest) ([][]string, error)
}
