# Coverage Sweep: Deterministic Deep Assertions

## Mission

Find uncovered code paths in unit tests and cover them with deep, deterministic assertions.
Fix bugs as you go — especially anything that breaks **Cypher compatibility** or **Neo4j DDL adherence**.
Those are highest priority.

## Current State

- All tests pass (`go test ./...` — 67 packages, 0 failures)
- Any test failure you introduce is a regression — evaluate whether:
  1. The original test was non-deterministic or shallow (fix the test)
  2. The asserted behavior was wrong (fix the code)
  3. Your change broke something (revert your change)

## Rules

### Test Quality
- **Deep deterministic assertions only.** No fake tests, no `assert.NotNil` when you can assert the actual value.
- Every test must assert specific values, specific error messages, specific lengths, specific field contents.
- No random/time-dependent assertions. If a test depends on timing, mock it.
- If you find a bug, fix it in the same commit. Don't skip it.

### Cypher/Neo4j Compatibility — HIGHEST PRIORITY
- Any `regexp` that matches Cypher patterns with fixed arity (fixed number of labels, properties, 
  relationship types, or arguments) where the Cypher DDL allows n-arity: **replace with keyword scanning**.
- Use the same scanning helper style as `pkg/cypher/keyword_scan.go` and the new helpers in 
  `pkg/storage/constraint_contracts.go` (`ccSkipSpaces`, `ccScanIdent`, `ccMatchKeywordAt`, etc.)
- When you find regex patterns in the cypher/storage/server packages that parse Cypher syntax,
  check if they handle multi-label nodes `(:A:B:C)`, composite properties, variable-length paths, etc.
- Neo4j allows n-arity everywhere: multiple labels, multiple properties in constraints, multiple 
  relationship types in patterns. Our parsers must too.

### What to Cover
Run `go test ./... -coverprofile=coverage.out` and `go tool cover -func=coverage.out` to find gaps.

**Priority order for coverage work:**
1. `pkg/storage` (76.9%) — biggest core gap, pure logic functions
2. `pkg/multidb` (83.3%)
3. `pkg/textchunk` (83.3%)
4. `pkg/storage/lifecycle` (84.1%)
5. `pkg/server` (84.2%)
6. `pkg/cypher` (85.8%) — Cypher compatibility tests are highest value here
7. `pkg/graphql/resolvers` (85.5%)
8. `pkg/search` (86.1%)
9. `apoc/coll` (65.9%), `apoc/date` (60.3%), `apoc/refactor` (21.5%)

Skip auto-generated files (antlr, graphql/generated).

### Scanning Helper Pattern

When replacing regex in any package, write local helpers if the package can't import `pkg/cypher`.
Follow this pattern (from `pkg/storage/constraint_contracts.go`):

```go
func ccSkipSpaces(s string, i int) int        // advance past whitespace
func ccScanIdent(s string, i int) (string, int) // read [A-Za-z_][A-Za-z0-9_]*
func ccMatchKeywordAt(s string, i int, kw string) int // case-insensitive keyword with word boundary
func ccScanComparator(s string, i int) (string, int)  // read <=, >=, <>, !=, =, <, >
func ccExpectByte(s string, i int, b byte) int        // check exact byte, return next pos or -1
func ccScanLabels(s string, i int) ([]string, int)    // read zero or more :Label sequences
```

### Process
1. Run coverage, find the biggest gaps in hand-written code
2. Read the uncovered functions
3. Write tests with deep assertions
4. If tests reveal bugs, fix the bugs
5. If you find regex parsing Cypher with fixed arity, refactor to keyword scanning
6. Run `go test ./...` frequently — never leave tests broken
7. Work in batches by package, verify each batch passes before moving on
