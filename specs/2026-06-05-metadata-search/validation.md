# Phase 8 — Metadata Search: Validation

## Definition of Done

Phase 8 is complete and can be merged when every item below passes without exception.

---

## 1. Build

- [ ] `./gradlew build` passes with no compilation errors on `libs/event-api`, `libs/opensearch-lib`, and `apps/event-read`.
- [ ] `libs/opensearch-lib` has no compile-time dependency on `libs/event-api` — verify with `./gradlew :libs:opensearch-lib:dependencies | grep event-api` returns nothing.
- [ ] `libs/event-api` has no compile-time dependency on `libs/opensearch-lib` — verify similarly.
- [ ] No raw-type or unchecked-cast warnings in the sealed `Expression` hierarchy or `OsSearchResponse<T>`.

---

## 2. Unit Tests

Run: `./gradlew :libs:event-api:test :libs:opensearch-lib:test :apps:event-read:test`

**`libs/event-api`**
- [ ] `ConditionExpr` and `BooleanExpr` factory methods produce correct field/value state for each condition type.
- [ ] `Sort` record rejects null `field` or null `direction` at construction.

**`libs/opensearch-lib` — `OsDocumentClient.search()`**
- [ ] `SearchRequest` is assembled with `query`, `sort`, `aggregations` from `OsSearchRequest` fields.
- [ ] `search_after` is appended to the request when `OsSearchRequest.searchAfter` is non-null and absent when null.
- [ ] Target index is set to the read alias resolved from `OsSchemaRegistry`.

**`libs/opensearch-lib` — `FieldNameMapper`**
- [ ] Cache is populated on first call and reused on subsequent calls for the same class.
- [ ] `@FieldName("schema_type")` resolves to the correct `Field` regardless of Java field name.
- [ ] `getField()` throws `IllegalArgumentException` for an unmapped field name.

**`apps/event-read` — `OsQueryBuilder`**
- [ ] `eq` → SDK `TermQuery` on the correct `SearchField.fieldName`.
- [ ] `in` → SDK `TermsQuery`.
- [ ] `between` → SDK `RangeQuery` with `gte`/`lte`.
- [ ] `exists` → SDK `ExistsQuery`.
- [ ] `notExists` → SDK `BoolQuery` wrapping `must_not ExistsQuery`.
- [ ] Nested `BooleanExpr` maps correctly to SDK `BoolQuery` `must` / `should` / `mustNot` clauses.
- [ ] Null `expression` produces a `MatchAllQuery`.

**`apps/event-read` — `OsSortBuilder`**
- [ ] Primary sort field name and direction are correct.
- [ ] Secondary `_id` sort is always appended as the second element.

**`apps/event-read` — `OsAggregationBuilder`**
- [ ] `TERMS` → SDK `TermsAggregation` on the correct field name.
- [ ] `DATE_HISTOGRAM` → SDK `DateHistogramAggregation` with the specified `interval`.
- [ ] `VALUE_COUNT` → SDK `ValueCountAggregation` on the correct field name.

**`apps/event-read` — `OsSearchResponseTranslator`**
- [ ] `EventDocument` → `EventSearchResult` projection includes `eventId`, `schemaType`, `timestamp` and excludes `s3Key`, `batchOffset`, `batchLength`.
- [ ] `OsCursorPageable` → `CursorPageable` mapping reconstructs `Sort` (primary field resolved to `SearchField`), `SearchPage` (page + size), and `searchAfter` correctly.
- [ ] Null `OsSearchResponse.nextPage` produces null `SearchResponse.nextPage`.
- [ ] Aggregation buckets (key, docCount, nested sub-aggregations) map correctly.

---

## 3. Integration Tests

Run: `./gradlew :apps:event-read:itest` (Docker Compose managed by root build)

- [ ] Shared `@BeforeAll` fixture indexes successfully — no index failures, read alias resolves correctly.
- [ ] `eq` on `SCHEMA_TYPE` returns only documents with the target schema type.
- [ ] `in` on `SCHEMA_TYPE` returns documents matching any specified value.
- [ ] `between` on `TIMESTAMP` returns only documents within the specified range.
- [ ] `exists` on `RULE_RESULT_STATUS` returns only documents that have this field populated.
- [ ] `notExists` on `RULE_RESULT_STATUS` returns only documents where this field is absent.
- [ ] Nested `BooleanExpr` (`must` + `mustNot`) returns the correct intersection of fixture documents.
- [ ] Null `expression` (match-all) returns all fixture documents across pages.
- [ ] `search_after` pagination with page size 2 visits every fixture document exactly once; `nextPage` is null on the page where `hits` is empty.
- [ ] `TERMS` aggregation on `SCHEMA_TYPE`: bucket keys match the distinct schema types in the fixture; doc counts sum to total fixture size.
- [ ] `DATE_HISTOGRAM` aggregation on `TIMESTAMP` with `"1h"` interval returns at least one non-empty bucket.
- [ ] `@FieldName`-annotated fields on `EventDocument` are correctly projected into `EventSearchResult`.
- [ ] Response `EventSearchResult` objects contain no `s3Key`, `batchOffset`, or `batchLength` fields.
- [ ] HTTP 400 returned when `cursorPageable` is null.
- [ ] HTTP 400 returned when `AggregationRequest.field` has a null `allowedAggregation` (e.g., `EVENT_ID`).
- [ ] HTTP 401 returned for a request with no `Authorization` header.

---

## 4. API Contract Smoke Test

Manual or scripted against the local Docker Compose stack:

- [ ] `POST /search/v1/events` with a valid JWT and a simple `eq` expression returns HTTP 200 with a non-empty `hits` list and correct `totalHits`.
- [ ] Response `nextPage` is non-null when there are more pages and null when `hits` is empty.
- [ ] Response `EventSearchResult` objects contain no `s3Key`, `batchOffset`, or `batchLength` fields.
- [ ] Invalid request body (missing `cursorPageable`) returns HTTP 400 with an `ErrorResponse` identifying the field.

---

## 5. Platform Rules Compliance

- [ ] All public `@Service` methods in `apps/event-read` carry `@Timed(histogram=true)` with dot-notation metric names.
- [ ] All public `@RestController` methods carry `@Timed(histogram=true)`.
- [ ] `OsDocumentClient.search()` carries `@Timed(histogram=true)` with a dot-notation metric name.
- [ ] `GlobalExceptionHandler` is the sole location of `@ExceptionHandler` methods in `apps/event-read`.
- [ ] `SecurityConfig` explicitly declares public and protected paths.
- [ ] `apps/event-read/application.yml` includes the standard log pattern and tracing block.
- [ ] No test uses Actuator or Prometheus endpoints as a correctness oracle.
- [ ] No test constructs request/response objects from raw JSON strings.
- [ ] Controller tests (`*IT`) live in `src/itest/` and extend `BaseTest`.
