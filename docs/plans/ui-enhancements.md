1. Define the end-state user experience and interaction model.
2. Define the backend graph APIs and payload contracts to support it.
3. Map the frontend architecture, shared state, and repo file changes for a single greenfield delivery.

**End-State Product**
Build this as a full graph investigation workspace inside the existing browser, not as a visualization add-on. The browser becomes a three-mode workspace: `Cypher`, `Semantic Search`, and `Graph`. The `Graph` mode is the primary visual surface for exploring live graph structure, semantic similarity, and temporal state.

The user flow should be:
- Run a Cypher query and click `Open in Graph`.
- Run semantic search and click `Explore Graph`.
- Open a node from any result table and expand its neighborhood visually.
- Switch between `Live` and `As Of` views with a timestamp control.
- Overlay semantic similarity links on top of structural edges.
- Focus on paths, clusters, selected labels, or changed topology over time.

This should feel cleaner than Neo4j Browser because the graph is not the whole app. The graph canvas is only one pane in a structured workspace:
- Top toolbar: mode controls, filters, layout, depth, as-of time, overlay toggles.
- Main canvas: graph viewport.
- Right inspector: node, edge, path, and cluster details.
- Bottom tray: query, legend, diagnostics, and graph stats.
- Left command surface remains the existing query/search entry model.

**Visual Direction**
Match the existing NornicDB UI language already visible in the browser and `Bifrost` styling:
- Use the Norse palette already defined in ui/src/index.css and ui/Bifrost/Bifrost.css.
- Keep a dark, high-contrast canvas with restrained accents.
- Avoid Neo4j Browser’s dense labels, loud colors, and default edge noise.
- Show labels progressively:
  - node label text only for selected, hovered, or zoom-relevant nodes
  - edge labels only on hover, focus, or path mode
- Use glow sparingly:
  - green for active selection
  - frost blue for semantic similarity
  - gold for pinned or anchor nodes
  - red/orange for changed or deleted historical elements in diff mode

The graph should feel like a precision instrument, not a whiteboard.

**Core End-State Features**
The full single-phase feature set should include all of these from day one:

- Structural graph exploration
  - Expand/collapse node neighborhoods by hop depth
  - Filter by labels, relationship types, degree, and database
  - Path finding between two nodes
  - Schema graph and data graph views
  - Query result graph view

- Semantic overlays
  - “Find similar” directly on the graph
  - Vector-nearest nodes displayed as a separate edge class
  - Hybrid relevance scoring in inspector
  - Cluster by embedding proximity
  - Search-to-graph transition without losing semantic ranking context

- Temporal graph exploration
  - `As Of` timestamp selector
  - Compare `Now` vs `As Of`
  - Visual diff:
    - added nodes
    - removed nodes
    - changed edges
    - changed properties
  - Time-aware inspector and graph metadata

- Investigation tooling
  - Pin nodes
  - Focus mode
  - Subgraph save/share
  - Path breadcrumbs
  - Mini-map
  - Keyboard shortcuts
  - Export image / export graph JSON
  - Open selected nodes in table/detail view

**Recommended Frontend Stack**
Use:
- `cytoscape`
- `cytoscape-cose-bilkent` or `elkjs`
- optional `cytoscape-dagre` for deterministic hierarchical layout

Layout set:
- `Force`: default for neighborhood exploration
- `Hierarchy`: for lineage, ownership, or dependency flows
- `Radial`: for “center node + neighbors”
- `Time Slice`: temporal diff arrangement
- `Semantic Cluster`: grouping by similarity/embedding cluster

**Backend API Design**
Add graph-native endpoints alongside the existing search and similarity routes already exposed in pkg/server/server_router.go.

New endpoints:

- `POST /nornicdb/graph/neighborhood`
  - seed node or nodes
  - structural neighborhood expansion
- `POST /nornicdb/graph/query`
  - normalize graph-shaped Cypher results into graph payload
- `POST /nornicdb/graph/path`
  - shortest path or constrained path
- `POST /nornicdb/graph/similar`
  - vector-nearest related nodes as graph overlay payload
- `POST /nornicdb/graph/temporal`
  - graph as of timestamp
- `POST /nornicdb/graph/diff`
  - compare two timestamps or current vs prior snapshot
- `GET /nornicdb/graph/schema`
  - labels, relationship types, display hints, counts
- `POST /nornicdb/graph/expand`
  - expand selected nodes incrementally without recomputing the full graph
- `POST /nornicdb/graph/clusters`
  - semantic or topology-based grouping

Canonical payload:

```ts
type GraphPayload = {
  nodes: Array<{
    id: string
    labels: string[]
    properties: Record<string, unknown>
    created_at?: string
    updated_at?: string
    score?: number
    vector_score?: number
    rrf_score?: number
    degree?: number
    centrality?: number
    cluster_id?: string
    status?: "live" | "added" | "removed" | "changed"
  }>
  edges: Array<{
    id: string
    source: string
    target: string
    type: string
    properties?: Record<string, unknown>
    weight?: number
    semantic?: boolean
    status?: "live" | "added" | "removed" | "changed"
  }>
  meta: {
    database: string
    seed_ids?: string[]
    generated_from?: "query" | "search" | "node" | "schema" | "diff"
    truncated?: boolean
    depth?: number
    as_of?: string
    compare_to?: string
    layout_hint?: "force" | "radial" | "hierarchy" | "semantic"
    node_count: number
    edge_count: number
  }
}
```

Request examples:

```json
{
  "database": "nornic",
  "seed_ids": ["123"],
  "depth": 2,
  "relationship_types": ["KNOWS", "RELATES_TO"],
  "labels": ["Memory", "Task"],
  "limit": 200,
  "as_of": "2026-03-15T12:00:00Z"
}
```

```json
{
  "database": "nornic",
  "node_id": "123",
  "similarity_limit": 12,
  "include_structural_neighbors": true,
  "as_of": "2026-03-15T12:00:00Z"
}
```

```json
{
  "database": "nornic",
  "from": "123",
  "to": "456",
  "max_depth": 5,
  "relationship_types": ["DEPENDS_ON", "RELATES_TO"],
  "as_of": "2026-03-15T12:00:00Z"
}
```

**Nornic-Specific Leverage**
This should not stop at Neo4j-style visualization.

Use Nornic-native features directly:
- Managed embeddings:
  - render semantic neighbors and clusters
  - show vector score in node inspector
  - color-code semantic edges separately
- Canonical graph ledger:
  - `asOf` reads in all graph endpoints
  - temporal diff between two graph states
  - inspect relationship evolution over time
- Unified search backing:
  - graph mode can start from semantic search results from `/nornicdb/search`
  - “find similar” graph overlay can reuse `/nornicdb/similar`
- Future: path explanation from graph plus vector context for Heimdall/Bifrost

In other words, Neo4j Browser parity is the floor. Nornic’s temporal and semantic capabilities are the point.

**Frontend Architecture**
The browser page in ui/src/pages/Browser.tsx already gives you the right shell. Extend it into a graph-native workspace.

Add browser state:
- `activeTab: "query" | "search" | "graph"`
- `graphData`
- `graphLoading`
- `graphError`
- `graphSelection`
- `graphFilters`
- `graphLayout`
- `graphOverlays`
- `graphTimeMode`
- `graphViewport`
- `graphHistory`

Extend the Zustand store in ui/src/store/appStore.ts:
- `loadGraphNeighborhood`
- `loadGraphFromQuery`
- `loadGraphFromSearch`
- `expandGraphNode`
- `collapseGraphNode`
- `loadGraphPath`
- `toggleSemanticOverlay`
- `setGraphAsOf`
- `loadGraphDiff`
- `pinGraphNode`
- `focusGraphNode`
- `clearGraph`

Extend the API client in ui/src/utils/api.ts:
- `getGraphNeighborhood`
- `getGraphFromQuery`
- `getGraphPath`
- `getGraphSchema`
- `getGraphSimilar`
- `getGraphTemporal`
- `getGraphDiff`

**Frontend Components**
Add these new components under `ui/src/components/browser/graph/`:

- `GraphPanel.tsx`
  - main composition component
- `GraphCanvas.tsx`
  - Cytoscape mount and interaction handling
- `GraphToolbar.tsx`
  - layouts, filters, depth, time, overlays
- `GraphInspector.tsx`
  - node/edge/path details
- `GraphLegend.tsx`
  - visual key
- `GraphMinimap.tsx`
  - viewport map
- `GraphFiltersPanel.tsx`
  - labels, relationships, semantic toggles
- `GraphDiffBadge.tsx`
  - added/removed/changed status chips
- `GraphEmptyState.tsx`
  - guided empty state
- `GraphLoadingState.tsx`
  - skeleton and loading animation

Shared utilities:
- `ui/src/utils/graphStyles.ts`
- `ui/src/utils/graphTransform.ts`
- `ui/src/utils/graphLayout.ts`
- `ui/src/utils/graphColors.ts`

The right panel should either:
- evolve `NodeDetailsPanel` into a more general inspector, or
- keep `NodeDetailsPanel` and wrap it inside `GraphInspector`

I would choose the second first, then consolidate later.

**Interaction Model**
The graph should support:
- click node:
  - select node
  - populate inspector
- double click node:
  - expand neighborhood
- shift click:
  - multi-select
- right click:
  - pin, expand, path from here, path to here, hide, isolate
- hover node:
  - lightweight preview
- drag:
  - manual reposition
- keyboard:
  - `f` focus selected
  - `e` expand
  - `c` collapse
  - `p` pin
  - `d` show diff controls
  - `/` focus graph search

**Graph Styling Rules**
Node visual encoding:
- size by degree or score
- fill by primary label
- ring color by state:
  - live: subtle rune border
  - selected: green glow
  - semantic match: frost ring
  - pinned: gold ring
  - changed in diff: amber or red

Edge visual encoding:
- structural edges: thin neutral lines
- semantic edges: dotted frost blue
- historical removed edges: red dashed
- newly added edges: green glow
- edge label only on hover or focus

This keeps the canvas readable while still expressive.

**Backend Implementation Shape**
Add graph handlers near the existing search handlers in:
- pkg/server/server_nornicdb.go
- pkg/server/server_router.go

Add service-layer graph composition in new files:
- `pkg/server/server_graph.go`
- `pkg/nornicdb/graph_services.go`
- `pkg/nornicdb/graph_payloads.go`

Likely responsibilities:
- server handler parses request and auth/database context
- graph service resolves nodes/edges from:
  - Cypher query results
  - structural neighborhood traversal
  - vector similarity search
  - temporal ledger reads
- graph payload formatter produces UI-ready graph payload

You already have useful building blocks:
- semantic search service in `pkg/search`
- similar/vector operations already wired to `/nornicdb/similar`
- neighbor traversal evidence in `pkg/nornicdb/db.go`
- APOC/path/subgraph and schema visualization support in `pkg/cypher`

That means the backend work is mostly composition and normalization, not invention from scratch.

**File-by-File End-State Plan**
Frontend changes:
- ui/src/pages/Browser.tsx
  - add graph tab, graph panel mount, shared selection wiring
- ui/src/store/appStore.ts
  - graph workspace state/actions
- ui/src/utils/api.ts
  - graph endpoints and graph types
- ui/src/components/browser/NodeDetailsPanel.tsx
  - reuse inside graph inspector or extend to edge/path detail support
- ui/src/components/browser/QueryPanel.tsx
  - add `Visualize` action for query results
- ui/src/components/browser/QueryResultsTable.tsx
  - add graph-open actions for rows or result sets
- ui/src/components/browser/SearchPanel.tsx
  - add `Explore Graph` and semantic-overlay actions
- `ui/src/components/browser/graph/*`
  - new graph workspace components
- ui/src/index.css
  - graph-specific utility classes
- ui/Bifrost/Bifrost.css
  - borrow token vocabulary, not necessarily direct edits unless you centralize tokens
- ui/package.json
  - add `cytoscape`, `elkjs`, optional layout plugins

Backend changes:
- pkg/server/server_router.go
  - register graph endpoints
- pkg/server/server_nornicdb.go
  - add graph handlers or delegate to `server_graph.go`
- new `pkg/server/server_graph.go`
  - HTTP graph endpoint implementation
- pkg/nornicdb/db.go
  - expose any missing traversal helpers if needed
- new `pkg/nornicdb/graph_services.go`
  - graph assembly logic
- new `pkg/nornicdb/graph_payloads.go`
  - DTOs and normalization
- possibly `pkg/search/*`
  - if semantic graph overlays need search-specific ranking normalization
- possibly `pkg/temporal/*`
  - if `asOf` graph assembly needs convenience service wrappers

Tests:
- add server tests for new graph endpoints under `pkg/server`
- add UI component tests if present in current setup, otherwise at least integration smoke coverage
- add payload consistency tests for temporal diff output

**Single-Phase Delivery Definition**
If you truly want one delivery phase, define done as:
- graph tab exists in browser
- query results can open in graph
- semantic search results can open in graph
- node neighborhoods can expand live
- similarity overlay works
- as-of selector works
- temporal diff works
- inspector is integrated
- layouts and filters work
- export works
- styling is fully native to NornicDB

That is a large feature, but it is coherent. It should be treated as one product surface with one design system, one graph payload contract, and one shared browser state model, not as separate mini-features.