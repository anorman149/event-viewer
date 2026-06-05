# Phase 8 — Metadata Search: Requirements

## Goal

Events are queryable through a composable, type-safe boolean query language. Results are paginated using OpenSearch `search_after` (no PIT). Each layer owns a complete, self-contained model with no cross-boundary type leakage: `libs/event-api` defines the external API search model; `libs/opensearch-lib` is a thin, self-contained execution abstraction that accepts pre-built OpenSearch SDK types; `apps/event-read` owns all translation between the two.

---

## Scope

### In scope

| Area | Detail |
|---|---|
| `libs/event-api` — `search` package | `FieldType`, `SearchField`, sealed `Expression` hierarchy, `ConditionExpr`, `BooleanExpr`, `SortDirection`, `Sort`, `SearchPage`, `AggregationType`, `AggregationRequest`, `CursorPageable`, `SearchRequest`, `AggregationBucket`, `AggregationResult`, `SearchResponse<T>` |
| `libs/opensearch-lib` — ORM utilities | `@FieldName` annotation, `FieldNameMapper` reflection cache |
| `libs/opensearch-lib` — search I/O types | `OsSearchRequest` (holds pre-built SDK types), `OsCursorPageable`, `OsAggregationBucket`, `OsAggregationResult`, `OsSearchResponse<T>` |
| `libs/opensearch-lib` — `OsDocumentClient.search()` | Alias resolution, assemble + execute the OpenSearch `SearchRequest`, map hits to `T`, build `OsCursorPageable` from last hit's sort values, return `OsSearchResponse<T>` |
| `apps/event-read` — `search/` infrastructure | `OsQueryBuilder` (Expression → SDK `Query`), `OsSortBuilder` (Sort → `List<SortOptions>`), `OsAggregationBuilder` (aggregations → `Map<String, Aggregation>`), `OsSearchResponseTranslator` (`OsSearchResponse<EventDocument>` → `SearchResponse<EventSearchResult>`) |
| `apps/event-read` — service + REST | `EventSearchResult` (external projection), `EventSearchService`, `EventSearchController`, `GlobalExceptionHandler`, `SecurityConfig` |
| Tests | Unit tests per module; itest against Docker Compose OpenSearch |

### Out of scope

- Raw payload retrieval (`GET /search/v1/events/{id}/payload`) — Phase 11
- BFF aggregation layer — later phases
- Any changes to the write path (`apps/event-ingest`) beyond what Phase 7 already provides

---

## Design Decisions

### 1. Sealed `Expression` hierarchy in `libs/event-api`

`Expression` is a `sealed interface` permitting exactly `ConditionExpr` and `BooleanExpr` (both `final`). `OsQueryBuilder` in `apps/event-read` uses an exhaustive `switch` over this sealed type to build the OpenSearch SDK `Query`.

### 2. `Sort` record groups sort field and direction

`CursorPageable` uses a `Sort` record (`field: SearchField`, `direction: SortDirection`) rather than two flat fields. `OsSortBuilder` translates this into `List<SortOptions>` (OpenSearch SDK), always appending a secondary sort on `_id` for stable ordering.

### 3. Strict abstraction wall — no cross-library type leakage

`libs/event-api` has zero dependency on `libs/opensearch-lib`. `libs/opensearch-lib` has zero dependency on `libs/event-api`. `apps/event-read` is the only module that imports both and owns all translation.

```
libs/event-api          (no deps on other libs in this project)
      ↑
apps/event-read         depends on both; all translation lives here
      ↓
libs/opensearch-lib     (no deps on event-api)
      ↓
OpenSearch Java Client
```

### 4. `OsSearchRequest` holds pre-built OpenSearch SDK types

`apps/event-read` is responsible for constructing all OpenSearch SDK query components. `libs/opensearch-lib` has no knowledge of the event-api search model. `OsSearchRequest` is a simple holder:

```java
// libs/opensearch-lib
public record OsSearchRequest(
    Query query,                        // org.opensearch.client.opensearch._types.query_dsl.Query
    List<SortOptions> sort,             // includes secondary _id sort
    Map<String, Aggregation> aggregations,
    int size,
    List<Object> searchAfter            // null on first page
) {}
```

### 5. `libs/opensearch-lib` owns the `OsCursorPageable` — fully encapsulated

`OsDocumentClient.search()` extracts sort values from the last hit and constructs `OsCursorPageable` internally. The cursor carries everything needed to make the next identical search: `sort`, `size`, `pageNumber` (incremented), and `searchAfter`. No raw sort arrays leak out of the library.

```java
// libs/opensearch-lib
public record OsCursorPageable(
    List<SortOptions> sort,
    int size,
    int pageNumber,
    List<Object> searchAfter
) {}
```

`apps/event-read` maps `OsCursorPageable` → `CursorPageable` in `OsSearchResponseTranslator` by resolving the primary `SortOptions` field name back to the matching `SearchField`.

### 6. `EventSearchResult` omits all S3 internals

`EventSearchResult` exposes `eventId`, `schemaType`, and `timestamp` only. `s3Key`, `batchOffset`, and `batchLength` are internal storage fields and must not appear in external API responses.

### 7. `nextPage` is null only when `hits` is empty

`OsSearchResponse.nextPage` (and `SearchResponse.nextPage`) is null exclusively when `hits` is empty. Standard `search_after` contract — no special-casing at exact page boundaries.

### 8. Shared fixture for itests

All itest classes share a single `@BeforeAll` fixture indexing a fixed, diverse set of `EventDocument` records once per test run. No per-test index/delete cycles.

### 9. POST for the search endpoint

`POST /search/v1/events` is used because the `Expression` tree can be arbitrarily large and complex.

---

## `libs/opensearch-lib` Search Types

Minimal set — no parallel expression/sort/aggregation model:

| Type | Purpose |
|---|---|
| `OsSearchRequest` | Input to `search()`: holds pre-built `Query`, `List<SortOptions>`, `Map<String, Aggregation>`, `size`, `searchAfter` |
| `OsCursorPageable` | Cursor returned by `search()`: `sort: List<SortOptions>`, `size: int`, `pageNumber: int`, `searchAfter: List<Object>` |
| `OsAggregationBucket` | `key: Object`, `docCount: long`, `subAggregations: Map<String, OsAggregationResult>` |
| `OsAggregationResult` | `name: String`, `buckets: List<OsAggregationBucket>` |
| `OsSearchResponse<T>` | `hits: List<T>`, `totalHits: long`, `nextPage: OsCursorPageable` (null when hits empty), `aggregations: Map<String, OsAggregationResult>` |

---

## `apps/event-read` SDK Translation — `search/` Infrastructure Package

| Component | Responsibility |
|---|---|
| `OsQueryBuilder` | Walks sealed `Expression` tree (exhaustive `switch`) → OpenSearch SDK `Query`; `null` expression → match-all |
| `OsSortBuilder` | Translates `Sort` → `List<SortOptions>`; always appends secondary `_id` `SortOptions` for stable ordering |
| `OsAggregationBuilder` | Translates `List<AggregationRequest>` → `Map<String, Aggregation>`; `TERMS` → terms agg, `DATE_HISTOGRAM` → date_histogram agg with interval, `VALUE_COUNT` → value_count agg |
| `OsSearchResponseTranslator` | Maps `OsSearchResponse<EventDocument>` → `SearchResponse<EventSearchResult>`; projects hits to `EventSearchResult`; maps `OsCursorPageable` → `CursorPageable` (resolves primary sort field name to `SearchField`); maps aggregation results |

---

## Key Constraints

- **No PIT:** Cursor-based pagination uses `search_after` only.
- **Read alias routing:** `OsDocumentClient.search()` resolves the read alias via `OsSchemaRegistry` from the document class.
- **Secondary sort on `_id`:** `OsSortBuilder` always appends a secondary `_id` sort. `OsCursorPageable.sort` carries it forward so each next-page request is consistent.
- **Validation chain:** `@Validated` on `SearchRequest` + `@Valid` on nested objects; `GlobalExceptionHandler` maps validation exceptions → HTTP 400 `ErrorResponse`.
- **Aggregation field guard:** `AggregationRequest` rejects any `field` whose `SearchField.allowedAggregation` is null at the Bean Validation layer before the request reaches the service.
- **`@FieldName` ORM:** `FieldNameMapper` uses a `ConcurrentHashMap` cache; resolves OpenSearch field names to `java.lang.reflect.Field` for hit deserialization inside `OsDocumentClient.search()`.
- **Metrics on `OsDocumentClient.search()`:** `@Timed(histogram=true)`; `DistributionSummary` for hits per response; `Counter` for failures.

---

## Context

Phase 7 indexed `EventDocument` records into OpenSearch with write/read aliases and ILM configured. Phase 8 activates the `search()` stub from Phase 6 and builds the full read path. `apps/event-read` owns all SDK translation; `libs/opensearch-lib` stays focused on what only it can do — alias resolution, execution, hit mapping, and cursor construction.
