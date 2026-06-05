# Phase 8 — Metadata Search: Plan

## Task Group 1: Search Query Language — `libs/event-api`

1. Add `FieldType` enum (`KEYWORD`, `INTEGER`, `DATE`, `TEXT`, `BOOLEAN`) to the `search` package.

2. Add `SearchField` enum — each value carries `fieldName: String`, `fieldType: FieldType`, and `allowedAggregation: AggregationType` (nullable). Values: `EVENT_ID` (KEYWORD, null), `SCHEMA_TYPE` (KEYWORD, TERMS), `TIMESTAMP` (DATE, DATE_HISTOGRAM), `RULE_RESULT_STATUS` (KEYWORD, null). `S3_KEY` intentionally excluded.

3. Add `AggregationType` enum (`TERMS`, `DATE_HISTOGRAM`, `VALUE_COUNT`). Add `SortDirection` enum (`ASC`, `DESC`). Add `Sort` record — `field: @NotNull SearchField`, `direction: @NotNull SortDirection`.

4. Add sealed `Expression` interface (`permits ConditionExpr, BooleanExpr`). Add final `ConditionExpr implements Expression` with static factories: `eq`, `in`, `between`, `exists`, `notExists`. Add final `BooleanExpr implements Expression` with `must`, `should`, `mustNot` lists and static factory/builder methods.

5. Add `SearchPage` record — `page: int` (`@Min(0)`, default `0`), `size: int` (`@Min(1)`, `@Max(1000)`, default `20`).

6. Add `AggregationRequest` — `name: String` (`@NotBlank`), `type: AggregationType` (`@NotNull`), `field: SearchField` (`@NotNull`), `interval: String` (required only for `DATE_HISTOGRAM`). Add a custom constraint (or `@AssertTrue`) that rejects any `field` whose `allowedAggregation` is null.

7. Add `CursorPageable` — `page: @Valid SearchPage`, `sort: @NotNull @Valid Sort`, `searchAfter: List<Object>` (nullable on first page).

8. Add `SearchRequest` annotated `@Validated` — `expression: Expression` (nullable = match-all), `cursorPageable: @NotNull @Valid CursorPageable`, `aggregations: @Valid List<AggregationRequest>` (empty = no aggregations).

9. Add `AggregationBucket` — `key: Object`, `docCount: long`, `subAggregations: Map<String, AggregationResult>`. Add `AggregationResult` — `name: String`, `buckets: List<AggregationBucket>`.

10. Add `SearchResponse<T>` — `hits: List<T>`, `totalHits: long`, `nextPage: CursorPageable` (null when `hits` is empty), `aggregations: Map<String, AggregationResult>`.

---

## Task Group 2: ORM Utilities — `libs/opensearch-lib`

11. Add `@FieldName` annotation — `@Target(FIELD)`, `@Retention(RUNTIME)`, single attribute `value: String`. Applied to document fields where the OpenSearch field name differs from the Java field name.

12. Add `FieldNameMapper` — `ConcurrentHashMap<Class<?>, Map<String, Field>>` populated once per class on first access by scanning all declared fields (using `@FieldName.value` if present, otherwise Java field name). Public API: `getField(Class<?> docClass, String opensearchFieldName): Field` (throws `IllegalArgumentException` if not found); `getValue(Object document, String opensearchFieldName): Object` convenience wrapper.

---

## Task Group 3: Search I/O Types — `libs/opensearch-lib`

13. Add `OsSearchRequest` record — `query: Query` (OpenSearch SDK `Query`), `sort: List<SortOptions>` (SDK type, includes secondary `_id` sort), `aggregations: Map<String, Aggregation>` (SDK type), `size: int`, `searchAfter: List<Object>` (null on first page).

14. Add `OsCursorPageable` record — `sort: List<SortOptions>`, `size: int`, `pageNumber: int`, `searchAfter: List<Object>`.

15. Add `OsAggregationBucket` — `key: Object`, `docCount: long`, `subAggregations: Map<String, OsAggregationResult>`. Add `OsAggregationResult` — `name: String`, `buckets: List<OsAggregationBucket>`. Add `OsSearchResponse<T>` — `hits: List<T>`, `totalHits: long`, `nextPage: OsCursorPageable` (null when hits empty), `aggregations: Map<String, OsAggregationResult>`.

---

## Task Group 4: `OsDocumentClient.search()` — `libs/opensearch-lib`

16. Implement `OsDocumentClient.search(Class<T> docClass, OsSearchRequest request): OsSearchResponse<T>`:

    **a. Alias resolution** — call `OsSchemaRegistry.getMetadata(docClass)`; extract `readAlias` as the target index.

    **b. Request assembly** — construct the OpenSearch Java Client `SearchRequest` using the pre-built components from `OsSearchRequest`: set `index` to the read alias, `query` from `request.query()`, `sort` from `request.sort()`, `size` from `request.size()`, `aggregations` from `request.aggregations()`, and append `search_after` only when `request.searchAfter()` is non-null.

    **c. Execution** — execute the `SearchRequest` via the OpenSearch Java Client.

    **d. Hit mapping** — deserialize each hit's `_source` JSON node to `T` via `ObjectMapper`; use `FieldNameMapper` to resolve `@FieldName`-annotated fields correctly.

    **e. Cursor building** — if `hits` is non-empty, take the last `Hit<T>` in the response hits list and read its `sort()` values (the per-hit sort field values OpenSearch returns alongside each document); convert those `FieldValue` items to a plain `List<Object>` — this list is the `searchAfter` token for the next page. Construct `OsCursorPageable` carrying `request.sort()`, `request.size()`, `pageNumber` incremented from the inbound cursor (or `1` if `request.searchAfter()` was null), and this extracted `searchAfter` list. Set `nextPage` to null when hits is empty.

    **f. Aggregation result mapping** — iterate response aggregations; extract bucket `key`, `docCount`, and nested sub-aggregations recursively; build `Map<String, OsAggregationResult>`.

    **g. Response assembly** — return `OsSearchResponse<T>` with `hits`, `totalHits` (from `hits.total().value()`), `nextPage`, and `aggregations`.

    **h. Observability** — `@Timed(value = "opensearch.document.client.search", histogram = true)`; `DistributionSummary` (`opensearch.search.hits`) for hits per response; `Counter` (`opensearch.search.failures`) incremented on exception before rethrowing.

---

## Task Group 5: SDK Translation — `apps/event-read` `search/` Infrastructure Package

17. Add `OsQueryBuilder` — translates `Expression` → OpenSearch SDK `Query` via exhaustive `switch` over the sealed hierarchy:
    - `BooleanExpr` → `BoolQuery` with `must` / `should` / `mustNot` clauses (each list recursed)
    - `ConditionExpr` dispatched by factory method type: `eq` → `TermQuery`; `in` → `TermsQuery`; `between` → `RangeQuery` with `gte`/`lte`; `exists` → `ExistsQuery`; `notExists` → `BoolQuery` wrapping a `must_not ExistsQuery`
    - Resolves `SearchField.fieldName` for all leaf nodes
    - Null `expression` → `MatchAllQuery`

18. Add `OsSortBuilder` — translates `Sort` → `List<SortOptions>`: primary sort on `sort.field().fieldName()` + `sort.direction()`; always appends secondary sort on `_id` (direction matching primary) for stable `search_after` ordering.

19. Add `OsAggregationBuilder` — translates `List<AggregationRequest>` → `Map<String, Aggregation>`:
    - `TERMS` → `TermsAggregation` on `field.fieldName()`
    - `DATE_HISTOGRAM` → `DateHistogramAggregation` on `field.fieldName()` with `interval`
    - `VALUE_COUNT` → `ValueCountAggregation` on `field.fieldName()`

20. Add `OsSearchResponseTranslator` — translates `OsSearchResponse<EventDocument>` → `SearchResponse<EventSearchResult>`:
    - Maps each `EventDocument` hit → `EventSearchResult` (project `eventId`, `schemaType`, `timestamp`)
    - Maps `OsCursorPageable` → `CursorPageable`: resolve the primary `SortOptions` field name to the matching `SearchField` (lookup by `SearchField.fieldName()`); reconstruct `Sort`, `SearchPage` (from `pageNumber` + `size`), and pass `searchAfter` through
    - Maps `Map<String, OsAggregationResult>` → `Map<String, AggregationResult>` with bucket key, docCount, sub-aggregations
    - Propagates null `nextPage` when `osResponse.nextPage()` is null

---

## Task Group 6: Event-Read Service — `apps/event-read`

21. Add `EventSearchResult` record — `eventId`, `schemaType`, `timestamp` only. No `s3Key`, `batchOffset`, or `batchLength`.

22. Add `EventSearchService` — injects `OsDocumentClient`, `OsQueryBuilder`, `OsSortBuilder`, `OsAggregationBuilder`, and `OsSearchResponseTranslator`; builds `OsSearchRequest` from the incoming `SearchRequest`; calls `osDocumentClient.search(EventDocument.class, osRequest)`; translates the response; returns `SearchResponse<EventSearchResult>`; annotated `@Timed(value = "event.read.service.search", histogram = true)`.

23. Add `EventSearchController` — `POST /search/v1/events`; accepts `@Validated @RequestBody SearchRequest`; returns `ResponseEntity<SearchResponse<EventSearchResult>>`; annotated `@Timed(value = "event.read.controller.search", histogram = true)`.

24. Add `GlobalExceptionHandler` in `apps/event-read` — handles `ConstraintViolationException` and `MethodArgumentNotValidException` → HTTP 400 `ErrorResponse`; follows the platform convention from `Rules.md`.

25. Add `SecurityConfig` in `apps/event-read` — explicit `SecurityFilterChain`; `/actuator/health` public; all other paths require authentication via Spring OAuth2 Resource Server JWT.

---

## Task Group 7: Tests

26. Unit tests in `libs/event-api` (`src/test/`):
    - `ConditionExpr` and `BooleanExpr` factory methods produce correct field/value state.
    - `Sort` record rejects null `field` or `direction`.

27. Unit tests in `libs/opensearch-lib` (`src/test/`):
    - `OsDocumentClient.search()` — verify `SearchRequest` is assembled correctly from `OsSearchRequest` fields (query set, sort set, aggregations set, `search_after` present only when non-null, index set to read alias).
    - `FieldNameMapper` — cache populated once on first call; `@FieldName` override respected; missing field throws `IllegalArgumentException`.

28. Unit tests in `apps/event-read` (`src/test/`):
    - `OsQueryBuilder` — verify SDK `Query` shape for each `ConditionExpr` type (`eq`, `in`, `between`, `exists`, `notExists`), nested `BooleanExpr` (must/should/mustNot), and null expression (match-all).
    - `OsSortBuilder` — verify primary sort field + direction; verify secondary `_id` sort always appended.
    - `OsAggregationBuilder` — verify `TERMS`, `DATE_HISTOGRAM` (with interval), and `VALUE_COUNT` aggregation DSL.
    - `OsSearchResponseTranslator` — verify `EventDocument` → `EventSearchResult` projection drops S3 fields; verify `OsCursorPageable` → `CursorPageable` mapping; verify null `nextPage` passes through; verify aggregation bucket mapping.

29. Integration tests in `apps/event-read` (`src/itest/`):
    - `BaseTest` abstract class with `@SpringBootTest`, `@LocalServerPort`, `TestRestTemplate`, `ObjectMapper`, `authHeaders()`, `baseUrl()`.
    - `@BeforeAll` shared fixture: index a fixed set of `EventDocument` records covering multiple `schemaType` values, `timestamp` ranges, and `ruleResults` statuses; refresh the index before tests run.
    - Verify `eq`, `in`, `between`, `exists`, `notExists` — each returns the correct document subset.
    - Verify nested `BooleanExpr` (`must` + `mustNot`) returns the correct intersection.
    - Verify null `expression` (match-all) returns all fixture documents.
    - Verify `search_after` cursor advances page-by-page; `nextPage` is null on the page where `hits` is empty.
    - Verify `TERMS` aggregation on `SCHEMA_TYPE` returns correct bucket keys and counts.
    - Verify `DATE_HISTOGRAM` aggregation on `TIMESTAMP` with `"1h"` interval returns non-empty buckets.
    - Verify `@FieldName`-annotated fields resolve correctly into `EventSearchResult`.
    - Verify `EventSearchResult` does not expose `s3Key`, `batchOffset`, or `batchLength`.
    - Verify HTTP 400 for null `cursorPageable` and for aggregation on a non-aggregatable field.
    - Verify HTTP 401 for a request with no `Authorization` header.
