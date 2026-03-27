package graph

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// InMemoryStore is a process-local Store implementation for testing and
// development.
type InMemoryStore struct {
	entries map[string]map[string]storeEntry
	mu      sync.RWMutex
}

type storeEntry struct {
	value     any
	embedding map[string]float64
	createdAt time.Time
	updatedAt time.Time
	ttl       *time.Duration
	expiresAt *time.Time
}

// NewInMemoryStore creates an empty in-memory store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{entries: make(map[string]map[string]storeEntry)}
}

// Get returns a value in a namespace and whether it exists.
func (s *InMemoryStore) Get(namespace []string, key string) (value any, ok bool, err error) {
	item, err := s.GetItem(context.Background(), namespace, key, StoreGetOptions{})
	if err != nil {
		return nil, false, err
	}
	if item == nil {
		return nil, false, nil
	}
	return item.Value, true, nil
}

// Set stores a value in a namespace.
func (s *InMemoryStore) Set(namespace []string, key string, value any) error {
	return s.PutItem(context.Background(), namespace, key, value, StorePutOptions{})
}

// Delete removes a value from a namespace.
func (s *InMemoryStore) Delete(namespace []string, key string) error {
	return s.DeleteItem(context.Background(), namespace, key)
}

// Batch executes store operations in order and returns one result per operation.
// Result types by operation:
//   - StoreGetOp: *StoreItem (or nil)
//   - StorePutOp: nil
//   - StoreSearchOp: []StoreSearchItem
//   - StoreListNamespacesOp: [][]string
func (s *InMemoryStore) Batch(ctx context.Context, ops []StoreOp) ([]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return make([]any, len(ops)), nil
	}
	results := make([]any, len(ops))
	for i, op := range ops {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result, err := s.runBatchOp(ctx, op)
		if err != nil {
			return nil, err
		}
		results[i] = result
	}
	return results, nil
}

func (s *InMemoryStore) runBatchOp(ctx context.Context, op StoreOp) (any, error) {
	switch typed := op.(type) {
	case StoreGetOp:
		return s.GetItem(ctx, typed.Namespace, typed.Key, StoreGetOptions{RefreshTTL: typed.RefreshTTL})
	case StorePutOp:
		if err := s.runStorePutOp(ctx, typed); err != nil {
			return nil, err
		}
		return nil, nil
	case StoreSearchOp:
		return s.SearchItems(ctx, StoreSearchRequest(typed))
	case StoreListNamespacesOp:
		return s.ListNamespaces(ctx, StoreNamespaceListRequest(typed))
	default:
		return nil, &InvalidUpdateError{Message: "unsupported store operation"}
	}
}

func (s *InMemoryStore) runStorePutOp(ctx context.Context, op StorePutOp) error {
	if op.Value == nil {
		return s.DeleteItem(ctx, op.Namespace, op.Key)
	}
	return s.PutItem(ctx, op.Namespace, op.Key, op.Value, StorePutOptions{TTL: op.TTL, Index: op.Index})
}

// GetItem returns a store record and applies optional TTL refresh.
func (s *InMemoryStore) GetItem(ctx context.Context, namespace []string, key string, opts StoreGetOptions) (*StoreItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, nil
	}
	nsKey := flattenStoreNamespace(namespace)
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	nsEntries, ok := s.entries[nsKey]
	if !ok {
		return nil, nil
	}
	entry, ok := nsEntries[key]
	if !ok {
		return nil, nil
	}
	if isStoreEntryExpired(entry, now) {
		delete(nsEntries, key)
		if len(nsEntries) == 0 {
			delete(s.entries, nsKey)
		}
		return nil, nil
	}
	if shouldRefreshTTL(opts.RefreshTTL) && entry.ttl != nil {
		expiresAt := now.Add(*entry.ttl)
		entry.expiresAt = &expiresAt
		nsEntries[key] = entry
	}
	return entry.toItem(namespace, key), nil
}

// PutItem stores or updates a record in the given namespace.
func (s *InMemoryStore) PutItem(ctx context.Context, namespace []string, key string, value any, opts StorePutOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil {
		return nil
	}
	nsKey := flattenStoreNamespace(namespace)
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	nsEntries, ok := s.entries[nsKey]
	if !ok {
		nsEntries = make(map[string]storeEntry)
		s.entries[nsKey] = nsEntries
	}
	createdAt := now
	if existing, exists := nsEntries[key]; exists {
		createdAt = existing.createdAt
	}
	entry := storeEntry{
		value:     value,
		embedding: nil,
		createdAt: createdAt,
		updatedAt: now,
	}
	fields, indexEnabled, err := resolveStoreIndexFields(opts.Index)
	if err != nil {
		return err
	}
	if indexEnabled {
		entry.embedding = buildStoreEmbedding(value, fields)
	}
	if opts.TTL != nil && *opts.TTL > 0 {
		ttl := *opts.TTL
		expiresAt := now.Add(ttl)
		entry.ttl = &ttl
		entry.expiresAt = &expiresAt
	}
	nsEntries[key] = entry
	return nil
}

// DeleteItem removes a record from the namespace.
func (s *InMemoryStore) DeleteItem(ctx context.Context, namespace []string, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil {
		return nil
	}
	ns := flattenStoreNamespace(namespace)
	s.mu.Lock()
	defer s.mu.Unlock()
	nsEntries, ok := s.entries[ns]
	if !ok {
		return nil
	}
	delete(nsEntries, key)
	if len(nsEntries) == 0 {
		delete(s.entries, ns)
	}
	return nil
}

// List returns sorted keys in a namespace that match prefix.
func (s *InMemoryStore) List(namespace []string, prefix string) ([]string, error) {
	if s == nil {
		return nil, nil
	}
	ns := flattenStoreNamespace(namespace)
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	nsEntries, ok := s.entries[ns]
	if !ok {
		return nil, nil
	}
	keys := make([]string, 0, len(nsEntries))
	for key, entry := range nsEntries {
		if isStoreEntryExpired(entry, now) {
			delete(nsEntries, key)
			continue
		}
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	if len(nsEntries) == 0 {
		delete(s.entries, ns)
	}
	sort.Strings(keys)
	return keys, nil
}

// Search returns sorted keys in a namespace whose key contains query.
// Matching is case-insensitive. A non-positive limit means no limit.
func (s *InMemoryStore) Search(namespace []string, query string, limit int) ([]string, error) {
	if s == nil {
		return nil, nil
	}
	ns := flattenStoreNamespace(namespace)
	needle := strings.ToLower(query)
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	nsEntries, ok := s.entries[ns]
	if !ok {
		return nil, nil
	}
	keys := make([]string, 0, len(nsEntries))
	for key, entry := range nsEntries {
		if isStoreEntryExpired(entry, now) {
			delete(nsEntries, key)
			continue
		}
		if needle == "" || strings.Contains(strings.ToLower(key), needle) {
			keys = append(keys, key)
		}
	}
	if len(nsEntries) == 0 {
		delete(s.entries, ns)
	}
	sort.Strings(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	return keys, nil
}

// SearchItems returns store records matching namespace prefix, optional query,
// and optional structured filter.
func (s *InMemoryStore) SearchItems(ctx context.Context, req StoreSearchRequest) ([]StoreSearchItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, nil
	}
	now := time.Now().UTC()
	refresh := shouldRefreshTTL(req.RefreshTTL)
	s.mu.Lock()
	defer s.mu.Unlock()

	results := make([]StoreSearchItem, 0)
	queryText, queryEmbedding := prepareStoreSearchQuery(req.Query)
	for nsKey, nsEntries := range s.entries {
		ns := expandStoreNamespace(nsKey)
		if !hasNamespacePrefix(ns, req.NamespacePrefix) {
			continue
		}
		matches := collectStoreSearchMatches(nsEntries, ns, req, queryText, queryEmbedding, now, refresh)
		results = append(results, matches...)
		if len(nsEntries) == 0 {
			delete(s.entries, nsKey)
		}
	}
	sortStoreSearchItems(results, queryText != "")
	return paginateStoreSearchItems(results, req.Offset, req.Limit), nil
}

func prepareStoreSearchQuery(query string) (queryText string, queryEmbedding map[string]float64) {
	queryText = strings.TrimSpace(query)
	if queryText == "" {
		return "", nil
	}
	return queryText, buildStoreEmbedding(queryText, []string{"$"})
}

func collectStoreSearchMatches(
	nsEntries map[string]storeEntry,
	namespace []string,
	req StoreSearchRequest,
	queryText string,
	queryEmbedding map[string]float64,
	now time.Time,
	refresh bool,
) []StoreSearchItem {
	out := make([]StoreSearchItem, 0)
	for key, entry := range nsEntries {
		if isStoreEntryExpired(entry, now) {
			delete(nsEntries, key)
			continue
		}
		score, ok := matchStoreSearchEntry(req, queryText, queryEmbedding, key, entry)
		if !ok {
			continue
		}
		if refresh && entry.ttl != nil {
			expiresAt := now.Add(*entry.ttl)
			entry.expiresAt = &expiresAt
			nsEntries[key] = entry
		}
		out = append(out, StoreSearchItem{StoreItem: *entry.toItem(namespace, key), Score: score})
	}
	return out
}

func matchStoreSearchEntry(
	req StoreSearchRequest,
	queryText string,
	queryEmbedding map[string]float64,
	key string,
	entry storeEntry,
) (*float64, bool) {
	score, ok := computeStoreSearchScore(queryText, queryEmbedding, req.Query, key, entry)
	if !ok {
		return nil, false
	}
	if !matchesStoreFilter(entry.value, req.Filter) {
		return nil, false
	}
	return score, true
}

func computeStoreSearchScore(
	queryText string,
	queryEmbedding map[string]float64,
	query string,
	key string,
	entry storeEntry,
) (*float64, bool) {
	if queryText == "" {
		if !matchesStoreQuery(query, key, entry.value) {
			return nil, false
		}
		return nil, true
	}
	if len(entry.embedding) == 0 || len(queryEmbedding) == 0 {
		return nil, false
	}
	value := cosineSimilaritySparseVectors(entry.embedding, queryEmbedding)
	if value <= 0 {
		return nil, false
	}
	return ptrFloat64(value), true
}

func sortStoreSearchItems(items []StoreSearchItem, sortByScore bool) {
	sort.Slice(items, func(i, j int) bool {
		if sortByScore {
			leftScore := storeScoreValue(items[i].Score)
			rightScore := storeScoreValue(items[j].Score)
			if leftScore != rightScore {
				return leftScore > rightScore
			}
		}
		leftNS := strings.Join(items[i].Namespace, "\x00")
		rightNS := strings.Join(items[j].Namespace, "\x00")
		if leftNS != rightNS {
			return leftNS < rightNS
		}
		return items[i].Key < items[j].Key
	})
}

func paginateStoreSearchItems(items []StoreSearchItem, offset, limit int) []StoreSearchItem {
	start := offset
	if start < 0 {
		start = 0
	}
	if start >= len(items) {
		return nil
	}
	end := len(items)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	return append([]StoreSearchItem(nil), items[start:end]...)
}

// ListNamespaces lists namespaces that contain at least one non-expired item,
// with optional prefix/suffix wildcard matching and pagination.
func (s *InMemoryStore) ListNamespaces(ctx context.Context, req StoreNamespaceListRequest) ([][]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, nil
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()

	namespaceSet := make(map[string][]string)
	for nsKey, nsEntries := range s.entries {
		if pruneExpiredStoreEntries(nsEntries, now) {
			delete(s.entries, nsKey)
			continue
		}
		namespace, ok := listableNamespace(nsKey, req)
		if ok {
			namespaceSet[flattenStoreNamespace(namespace)] = namespace
		}
	}

	out := make([][]string, 0, len(namespaceSet))
	for _, ns := range namespaceSet {
		out = append(out, append([]string(nil), ns...))
	}
	sort.Slice(out, func(i, j int) bool {
		left := strings.Join(out[i], "\x00")
		right := strings.Join(out[j], "\x00")
		return left < right
	})

	start := req.Offset
	if start < 0 {
		start = 0
	}
	if start >= len(out) {
		return nil, nil
	}
	end := len(out)
	if req.Limit > 0 && start+req.Limit < end {
		end = start + req.Limit
	}
	return out[start:end], nil
}

func pruneExpiredStoreEntries(entries map[string]storeEntry, now time.Time) bool {
	for key, entry := range entries {
		if isStoreEntryExpired(entry, now) {
			delete(entries, key)
		}
	}
	return len(entries) == 0
}

func listableNamespace(nsKey string, req StoreNamespaceListRequest) ([]string, bool) {
	namespace := expandStoreNamespace(nsKey)
	if !matchesNamespaceConditions(namespace, req.MatchConditions) {
		return nil, false
	}
	if req.MaxDepth != nil && *req.MaxDepth >= 0 && len(namespace) > *req.MaxDepth {
		namespace = append([]string(nil), namespace[:*req.MaxDepth]...)
	}
	return namespace, true
}

func (e storeEntry) toItem(namespace []string, key string) *StoreItem {
	return &StoreItem{
		Namespace: append([]string(nil), namespace...),
		Key:       key,
		Value:     e.value,
		CreatedAt: e.createdAt,
		UpdatedAt: e.updatedAt,
	}
}

func isStoreEntryExpired(entry storeEntry, now time.Time) bool {
	if entry.expiresAt == nil {
		return false
	}
	return !now.Before(*entry.expiresAt)
}

func shouldRefreshTTL(refresh *bool) bool {
	if refresh == nil {
		return true
	}
	return *refresh
}

func hasNamespacePrefix(namespace, prefix []string) bool {
	if len(prefix) == 0 {
		return true
	}
	if len(namespace) < len(prefix) {
		return false
	}
	for i := range prefix {
		if namespace[i] != prefix[i] {
			return false
		}
	}
	return true
}

func matchesStoreQuery(query, key string, value any) bool {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return true
	}
	needle := strings.ToLower(trimmed)
	if strings.Contains(strings.ToLower(key), needle) {
		return true
	}
	return strings.Contains(strings.ToLower(stringifyStoreValue(value)), needle)
}

func stringifyStoreValue(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		bytes, err := json.Marshal(typed)
		if err != nil {
			return ""
		}
		return string(bytes)
	}
}

func matchesStoreFilter(value any, filter map[string]any) bool {
	if len(filter) == 0 {
		return true
	}
	itemMap, ok := value.(map[string]any)
	if !ok {
		return false
	}
	for filterKey, filterValue := range filter {
		if !compareFilterValues(itemMap[filterKey], filterValue) {
			return false
		}
	}
	return true
}

func compareFilterValues(itemValue, filterValue any) bool {
	fv, ok := filterValue.(map[string]any)
	if !ok {
		return itemValue == filterValue
	}
	if filterMapUsesOperators(fv) {
		return compareFilterOperatorMap(itemValue, fv)
	}
	return compareFilterNestedMap(itemValue, fv)
}

func filterMapUsesOperators(filter map[string]any) bool {
	for key := range filter {
		if strings.HasPrefix(key, "$") {
			return true
		}
	}
	return false
}

func compareFilterOperatorMap(itemValue any, filter map[string]any) bool {
	for op, value := range filter {
		if !applyFilterOperator(itemValue, op, value) {
			return false
		}
	}
	return true
}

func compareFilterNestedMap(itemValue any, filter map[string]any) bool {
	itemMap, ok := itemValue.(map[string]any)
	if !ok {
		return false
	}
	for key, value := range filter {
		if !compareFilterValues(itemMap[key], value) {
			return false
		}
	}
	return true
}

func applyFilterOperator(itemValue any, operator string, filterValue any) bool {
	switch operator {
	case "$eq":
		return itemValue == filterValue
	case "$ne":
		return itemValue != filterValue
	case "$gt", "$gte", "$lt", "$lte":
		left, lok := toNumeric(itemValue)
		right, rok := toNumeric(filterValue)
		if !lok || !rok {
			return false
		}
		switch operator {
		case "$gt":
			return left > right
		case "$gte":
			return left >= right
		case "$lt":
			return left < right
		case "$lte":
			return left <= right
		}
	}
	return false
}

func resolveStoreIndexFields(raw any) (fields []string, enabled bool, err error) {
	if raw == nil {
		return []string{"$"}, true, nil
	}
	switch typed := raw.(type) {
	case bool:
		if !typed {
			return nil, false, nil
		}
		return []string{"$"}, true, nil
	case []string:
		return normalizeStoreStringFields(typed), true, nil
	case map[string]any:
		return resolveStoreIndexFieldsFromMap(typed)
	default:
		return nil, false, &InvalidUpdateError{Message: "store index must be false, a fields array, or an object with fields"}
	}
}

func resolveStoreIndexFieldsFromMap(raw map[string]any) (fields []string, enabled bool, err error) {
	fieldsRaw, hasFields := raw["fields"]
	if !hasFields || fieldsRaw == nil {
		return []string{"$"}, true, nil
	}
	stringFields, err := parseStoreIndexFields(fieldsRaw)
	if err != nil {
		return nil, false, err
	}
	return normalizeStoreStringFields(stringFields), true, nil
}

func parseStoreIndexFields(raw any) ([]string, error) {
	if stringFields, ok := raw.([]string); ok {
		return append([]string(nil), stringFields...), nil
	}
	fields, ok := raw.([]any)
	if !ok {
		return nil, &InvalidUpdateError{Message: "store index fields must be an array of strings"}
	}
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		text, ok := field.(string)
		if !ok {
			return nil, &InvalidUpdateError{Message: "store index fields must be an array of strings"}
		}
		if strings.TrimSpace(text) != "" {
			out = append(out, text)
		}
	}
	return out, nil
}

func normalizeStoreStringFields(fields []string) []string {
	if len(fields) == 0 {
		return []string{"$"}
	}
	return append([]string(nil), fields...)
}

func buildStoreEmbedding(value any, fields []string) map[string]float64 {
	textParts := make([]string, 0)
	for _, field := range fields {
		texts := extractStoreTextsAtPath(value, strings.TrimSpace(field))
		textParts = append(textParts, texts...)
	}
	if len(textParts) == 0 {
		textParts = append(textParts, stringifyStoreValue(value))
	}
	tokens := make(map[string]float64)
	for _, text := range textParts {
		for _, token := range tokenizeStoreText(text) {
			tokens[token]++
		}
	}
	if len(tokens) == 0 {
		return nil
	}
	norm := 0.0
	for _, value := range tokens {
		norm += value * value
	}
	if norm == 0 {
		return nil
	}
	scale := 1.0 / math.Sqrt(norm)
	for token, value := range tokens {
		tokens[token] = value * scale
	}
	return tokens
}

func tokenizeStoreText(text string) []string {
	lowered := strings.ToLower(strings.TrimSpace(text))
	if lowered == "" {
		return nil
	}
	b := strings.Builder{}
	b.Grow(len(lowered))
	for _, r := range lowered {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		} else {
			b.WriteByte(' ')
		}
	}
	words := strings.Fields(b.String())
	if len(words) == 0 {
		return nil
	}
	tokens := make([]string, 0)
	for _, word := range words {
		runes := []rune(word)
		if len(runes) < 3 {
			tokens = append(tokens, word)
			continue
		}
		for i := 0; i <= len(runes)-3; i++ {
			tokens = append(tokens, string(runes[i:i+3]))
		}
	}
	return tokens
}

func cosineSimilaritySparseVectors(left, right map[string]float64) float64 {
	if len(left) == 0 || len(right) == 0 {
		return 0
	}
	dot := 0.0
	if len(left) > len(right) {
		left, right = right, left
	}
	for token, value := range left {
		dot += value * right[token]
	}
	return dot
}

func storeScoreValue(score *float64) float64 {
	if score == nil {
		return -1
	}
	return *score
}

func ptrFloat64(value float64) *float64 {
	v := value
	return &v
}

type storePathToken struct {
	field    string
	index    int
	wildcard bool
}

func extractStoreTextsAtPath(value any, path string) []string {
	if path == "" || path == "$" {
		text := strings.TrimSpace(stringifyStoreValue(value))
		if text == "" {
			return nil
		}
		return []string{text}
	}
	tokens, ok := parseStorePath(path)
	if !ok {
		return nil
	}
	values := extractStorePathValues(value, tokens)
	texts := make([]string, 0, len(values))
	for _, candidate := range values {
		text := strings.TrimSpace(stringifyStoreValue(candidate))
		if text != "" {
			texts = append(texts, text)
		}
	}
	return texts
}

func parseStorePath(path string) ([]storePathToken, bool) {
	out := make([]storePathToken, 0)
	for i := 0; i < len(path); {
		switch path[i] {
		case '.':
			i++
		case '[':
			end := strings.IndexByte(path[i:], ']')
			if end <= 0 {
				return nil, false
			}
			end += i
			part := path[i+1 : end]
			if part == "*" {
				out = append(out, storePathToken{wildcard: true})
			} else {
				index, err := strconv.Atoi(part)
				if err != nil {
					return nil, false
				}
				out = append(out, storePathToken{index: index})
			}
			i = end + 1
		default:
			start := i
			for i < len(path) && path[i] != '.' && path[i] != '[' {
				i++
			}
			field := strings.TrimSpace(path[start:i])
			if field == "" {
				return nil, false
			}
			out = append(out, storePathToken{field: field})
		}
	}
	return out, len(out) > 0
}

func extractStorePathValues(value any, tokens []storePathToken) []any {
	if len(tokens) == 0 {
		return []any{value}
	}
	tok := tokens[0]
	rest := tokens[1:]
	if tok.field != "" {
		return extractStoreFieldPathValues(value, tok.field, rest)
	}
	if tok.wildcard {
		return extractStoreWildcardPathValues(value, rest)
	}
	return extractStoreIndexPathValues(value, tok.index, rest)
}

func extractStoreFieldPathValues(value any, field string, rest []storePathToken) []any {
	asMap, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	next, exists := asMap[field]
	if !exists {
		return nil
	}
	return extractStorePathValues(next, rest)
}

func extractStoreWildcardPathValues(value any, rest []storePathToken) []any {
	switch typed := value.(type) {
	case []any:
		return extractStoreWildcardSliceValues(typed, rest)
	case map[string]any:
		return extractStoreWildcardMapValues(typed, rest)
	default:
		return nil
	}
}

func extractStoreWildcardSliceValues(values []any, rest []storePathToken) []any {
	out := make([]any, 0)
	for _, item := range values {
		out = append(out, extractStorePathValues(item, rest)...)
	}
	return out
}

func extractStoreWildcardMapValues(values map[string]any, rest []storePathToken) []any {
	out := make([]any, 0)
	for _, item := range values {
		out = append(out, extractStorePathValues(item, rest)...)
	}
	return out
}

func extractStoreIndexPathValues(value any, index int, rest []storePathToken) []any {
	asList, ok := value.([]any)
	if !ok {
		return nil
	}
	resolved := index
	if resolved < 0 {
		resolved = len(asList) + resolved
	}
	if resolved < 0 || resolved >= len(asList) {
		return nil
	}
	return extractStorePathValues(asList[resolved], rest)
}

func toNumeric(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}

func matchesNamespaceConditions(namespace []string, conditions []NamespaceMatchCondition) bool {
	if len(conditions) == 0 {
		return true
	}
	for _, condition := range conditions {
		if !matchesNamespaceCondition(namespace, condition) {
			return false
		}
	}
	return true
}

func matchesNamespaceCondition(namespace []string, condition NamespaceMatchCondition) bool {
	if len(namespace) < len(condition.Path) {
		return false
	}
	switch condition.MatchType {
	case NamespaceMatchPrefix:
		for i := range condition.Path {
			if condition.Path[i] != "*" && namespace[i] != condition.Path[i] {
				return false
			}
		}
		return true
	case NamespaceMatchSuffix:
		offset := len(namespace) - len(condition.Path)
		for i := range condition.Path {
			if condition.Path[i] != "*" && namespace[offset+i] != condition.Path[i] {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func flattenStoreNamespace(namespace []string) string {
	if len(namespace) == 0 {
		return ""
	}
	var b strings.Builder
	for _, part := range namespace {
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
		b.WriteByte('|')
	}
	return b.String()
}

func expandStoreNamespace(encoded string) []string {
	if encoded == "" {
		return nil
	}
	out := make([]string, 0)
	i := 0
	for i < len(encoded) {
		sep := strings.IndexByte(encoded[i:], ':')
		if sep < 0 {
			return nil
		}
		sep += i
		lengthText := encoded[i:sep]
		segmentLength, err := strconv.Atoi(lengthText)
		if err != nil || segmentLength < 0 {
			return nil
		}
		start := sep + 1
		end := start + segmentLength
		if end > len(encoded) {
			return nil
		}
		out = append(out, encoded[start:end])
		i = end
		if i < len(encoded) {
			if encoded[i] != '|' {
				return nil
			}
			i++
		}
	}
	return out
}
