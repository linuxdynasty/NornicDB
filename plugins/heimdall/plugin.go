// Package heimdall provides the Heimdall SLM Management plugin.
//
// Heimdall is the all-seeing guardian of the SLM subsystem, named after the
// Norse god who watches over Bifröst. Like its namesake, Heimdall monitors
// all activity, maintains vigilance over system health, and controls access
// to the cognitive capabilities of NornicDB.
//
// # Plugin Type
//
// This is an SLM plugin (Type() returns "slm"), which means it provides
// subsystem management capabilities that the SLM can use.
//
// # Actions Provided
//
//   - heimdall.heimdall.status - Get SLM status (Heimdall's vigilant watch)
//   - heimdall.heimdall.health - Check SLM health (Heimdall's keen sight)
//   - heimdall.heimdall.config - Get/set SLM configuration
//   - heimdall.heimdall.metrics - Get SLM metrics (Heimdall's awareness)
//   - heimdall.heimdall.events - Get recent events (Heimdall's memory)
//
// # Example Usage
//
// User: "What's the status of the SLM?"
// SLM maps to: heimdall.heimdall.status
// Result: Returns current model, memory usage, request counts
//
// # Building as Plugin
//
// To build as a standalone .so plugin:
//
//	go build -buildmode=plugin -o heimdall.so ./plugins/heimdall
//
// # Built-in Registration
//
// This plugin is also registered as a built-in plugin, so no .so file is needed.
package heimdall

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/heimdall"
)

// Plugin is the exported plugin variable.
// For .so plugins, export as: var Plugin heimdall.HeimdallPlugin = &WatcherPlugin{}
var Plugin heimdall.HeimdallPlugin = &WatcherPlugin{}

// WatcherPlugin implements heimdall.HeimdallPlugin for SLM management.
// The Watcher is Heimdall's core guardian - the all-seeing eye of the system.
//
// This plugin also demonstrates autonomous action invocation:
// - Implements DatabaseEventHook to monitor database events
// - Accumulates events and triggers analysis when thresholds are exceeded
// - Uses HeimdallInvoker to autonomously invoke SLM actions
type WatcherPlugin struct {
	mu       sync.RWMutex
	ctx      heimdall.SubsystemContext
	status   heimdall.SubsystemStatus
	events   []heimdall.SubsystemEvent
	config   map[string]interface{}
	started  time.Time
	requests int64
	errors   int64
}

// === Identity Methods ===

func (p *WatcherPlugin) Name() string {
	return "watcher"
}

func (p *WatcherPlugin) Version() string {
	return "1.0.0"
}

func (p *WatcherPlugin) Type() string {
	return heimdall.PluginTypeHeimdall // Must return "heimdall"
}

func (p *WatcherPlugin) Description() string {
	return "Watcher - Heimdall's core guardian, the all-seeing eye of NornicDB's SLM subsystem"
}

// === Lifecycle Methods ===

func (p *WatcherPlugin) Initialize(ctx heimdall.SubsystemContext) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ctx = ctx
	p.status = heimdall.StatusReady
	p.events = make([]heimdall.SubsystemEvent, 0, 100)
	p.config = map[string]interface{}{
		"max_tokens":  ctx.Config.MaxTokens,
		"temperature": ctx.Config.Temperature,
		"model":       ctx.Config.Model,
	}

	p.addEvent("info", "Heimdall awakens - SLM guardian initialized", nil)
	return nil
}

func (p *WatcherPlugin) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.status = heimdall.StatusRunning
	p.started = time.Now()
	p.addEvent("info", "Heimdall stands watch - SLM guardian active", nil)
	return nil
}

func (p *WatcherPlugin) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.status = heimdall.StatusStopped
	p.addEvent("info", "Heimdall rests - SLM guardian paused", nil)
	return nil
}

func (p *WatcherPlugin) Shutdown() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.status = heimdall.StatusUninitialized
	p.addEvent("info", "Heimdall departs - SLM guardian shutdown", nil)
	return nil
}

// === State & Health Methods ===

func (p *WatcherPlugin) Status() heimdall.SubsystemStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.status
}

func (p *WatcherPlugin) Health() heimdall.SubsystemHealth {
	p.mu.RLock()
	defer p.mu.RUnlock()

	healthy := p.status == heimdall.StatusRunning || p.status == heimdall.StatusReady

	return heimdall.SubsystemHealth{
		Status:    p.status,
		Healthy:   healthy,
		Message:   fmt.Sprintf("Heimdall reports: SLM is %s", p.status),
		LastCheck: time.Now(),
		Details: map[string]interface{}{
			"uptime_seconds": time.Since(p.started).Seconds(),
			"requests":       p.requests,
			"errors":         p.errors,
		},
	}
}

func (p *WatcherPlugin) Metrics() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return map[string]interface{}{
		"status":         string(p.status),
		"uptime_seconds": time.Since(p.started).Seconds(),
		"requests":       p.requests,
		"errors":         p.errors,
		"error_rate":     float64(p.errors) / float64(max(p.requests, 1)),
		"memory_mb":      memStats.Alloc / 1024 / 1024,
		"goroutines":     runtime.NumGoroutine(),
	}
}

// === Configuration Methods ===

func (p *WatcherPlugin) Config() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Return copy
	result := make(map[string]interface{})
	for k, v := range p.config {
		result[k] = v
	}
	return result
}

func (p *WatcherPlugin) Configure(settings map[string]interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Validate and apply settings
	for key, value := range settings {
		switch key {
		case "max_tokens":
			if v, ok := value.(int); ok && v > 0 && v <= 4096 {
				p.config[key] = v
			} else {
				return fmt.Errorf("invalid max_tokens: must be 1-4096")
			}
		case "temperature":
			if v, ok := value.(float64); ok && v >= 0 && v <= 2 {
				p.config[key] = v
			} else {
				return fmt.Errorf("invalid temperature: must be 0-2")
			}
		default:
			return fmt.Errorf("unknown config key: %s", key)
		}
	}

	p.addEvent("info", "Heimdall configuration updated", settings)
	return nil
}

func (p *WatcherPlugin) ConfigSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"max_tokens": map[string]interface{}{
				"type":        "integer",
				"description": "Maximum tokens to generate",
				"minimum":     1,
				"maximum":     4096,
				"default":     512,
			},
			"temperature": map[string]interface{}{
				"type":        "number",
				"description": "Generation temperature (0=deterministic, 2=creative)",
				"minimum":     0,
				"maximum":     2,
				"default":     0.1,
			},
		},
	}
}

// === Actions ===

// autocompleteSuggestInputSchema is JSON Schema for the autocomplete_suggest action (query param).
var autocompleteSuggestInputSchema = json.RawMessage([]byte(`{"type":"object","properties":{"query":{"type":"string","description":"Partial Cypher query to complete"}},"required":["query"]}`))

// discoverInputSchema is JSON Schema for the discover action (semantic search in the graph).
var discoverInputSchema = json.RawMessage([]byte(`{"type":"object","properties":{"query":{"type":"string","description":"Natural language or keyword search query"},"limit":{"type":"integer","description":"Max results (default 10)"},"depth":{"type":"integer","description":"Traversal depth for relationships from matched nodes (default 1)"}},"required":["query"]}`))

// queryInputSchema is JSON Schema for the query action (read-only Cypher).
var queryInputSchema = json.RawMessage([]byte(`{"type":"object","properties":{"cypher":{"type":"string","description":"Cypher query to execute"},"params":{"type":"object","description":"Optional query parameters"},"database":{"type":"string","description":"Logical database name (optional)"}},"required":["cypher"]}`))

func (p *WatcherPlugin) Actions() map[string]heimdall.ActionFunc {
	return map[string]heimdall.ActionFunc{
		"help": {
			Description: "List all available SLM actions",
			Category:    "system",
			Handler:     p.actionHelp,
		},
		"status": {
			Description: "Get SLM system status (plugins, actions, database, runtime)",
			Category:    "system",
			Handler:     p.actionStatus,
		},
		"autocomplete_suggest": {
			Description: "Generate Cypher query autocomplete suggestions based on database schema",
			Category:    "query",
			InputSchema: autocompleteSuggestInputSchema,
			Handler:     p.actionAutocompleteSuggest,
		},
		"discover": {
			Description: "Semantic search in the knowledge graph. Use for conceptual search (e.g. 'pharmacy', 'my orders', 'prescription status'). Params: query (required), limit (optional), depth (optional).",
			Category:    "search",
			InputSchema: discoverInputSchema,
			Handler:     p.actionDiscover,
		},
		"query": {
			Description: "Execute a read-only Cypher query. Use when the user asks for explicit Cypher, MATCH/RETURN, or nodes by label. Params: cypher (required), params (optional), database (optional).",
			Category:    "database",
			InputSchema: queryInputSchema,
			Handler:     p.actionQuery,
		},
		"db_stats": {
			Description: "Get database statistics: node/edge counts, labels, k-means clusters, embeddings, feature flags",
			Category:    "database",
			Handler:     p.actionDBStats,
		},
	}
}

// Action Handlers

// actionHelp returns the action catalog from the package (all registered actions by category).
func (p *WatcherPlugin) actionHelp(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	catalog := heimdall.ActionCatalog()
	p.addEvent("info", "Help: listed actions", nil)
	return &heimdall.ActionResult{
		Success: true,
		Message: "Available actions by category",
		Data:    map[string]interface{}{"catalog": catalog},
	}, nil
}

// actionAutocompleteSuggest returns database schema (labels, relationship types, properties) for Cypher autocomplete.
func (p *WatcherPlugin) actionAutocompleteSuggest(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	query, _ := ctx.Params["query"].(string)
	if query == "" {
		return &heimdall.ActionResult{
			Success: false,
			Message: "query parameter required",
		}, nil
	}

	var labels, properties, relTypes []string
	if ctx.Database != nil {
		dbName, _ := ctx.Params["database"].(string)
		if dbName == "" {
			dbName, _ = ctx.Params["db"].(string)
		}
		labelResults, err := ctx.Database.Query(ctx.Context, dbName, "CALL db.labels() YIELD label RETURN label", nil)
		if err == nil {
			for _, row := range labelResults {
				if label, ok := row["label"].(string); ok {
					labels = append(labels, label)
				}
			}
		}
		relResults, err := ctx.Database.Query(ctx.Context, dbName, "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType", nil)
		if err == nil {
			for _, row := range relResults {
				if relType, ok := row["relationshipType"].(string); ok {
					relTypes = append(relTypes, relType)
				}
			}
		}
		propResults, err := ctx.Database.Query(ctx.Context, dbName, `
			MATCH (n) WITH n, keys(n) as props UNWIND props as prop RETURN DISTINCT prop LIMIT 50
		`, nil)
		if err == nil {
			for _, row := range propResults {
				if prop, ok := row["prop"].(string); ok {
					properties = append(properties, prop)
				}
			}
		}
	}
	schemaInfo := map[string]interface{}{
		"labels":     labels,
		"properties": properties,
		"relTypes":   relTypes,
	}
	return &heimdall.ActionResult{
		Success: true,
		Message: "Autocomplete suggestions",
		Data: map[string]interface{}{
			"query":      query,
			"schema":     schemaInfo,
			"suggestion": "",
		},
	}, nil
}

// actionDiscover runs semantic search on the graph and returns formatted results for the LLM.
func (p *WatcherPlugin) actionDiscover(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	query, _ := ctx.Params["query"].(string)
	if query == "" {
		return &heimdall.ActionResult{
			Success: false,
			Message: "query parameter required",
		}, nil
	}
	limit := 10
	if l, ok := ctx.Params["limit"].(float64); ok && l > 0 && l <= 50 {
		limit = int(l)
	}
	depth := 1 // relationship hop from matched nodes (1 = direct neighbors)
	if d, ok := ctx.Params["depth"].(float64); ok && d >= 0 && d <= 5 {
		depth = int(d)
	}
	dbName := getDatabaseParam(ctx.Params)
	if dbName == "" && ctx.Database != nil {
		dbName = ctx.Database.DefaultDatabaseName()
	}

	if ctx.Database == nil {
		return &heimdall.ActionResult{Success: false, Message: "database not available"}, nil
	}

	discoverCtx, cancel := context.WithTimeout(ctx.Context, 15*time.Second)
	defer cancel()
	result, err := ctx.Database.Discover(discoverCtx, dbName, query, nil, limit, depth)
	if err != nil {
		log.Printf("[Watcher] discover failed: %v", err)
		return &heimdall.ActionResult{
			Success: false,
			Message: fmt.Sprintf("Search failed: %v", err),
		}, nil
	}
	formatted := formatDiscoverResult(result)
	// Put formatted context in Message so the agentic loop sends readable context to the LLM
	msg := formatted
	if len(msg) > 8000 {
		msg = msg[:8000] + "\n... (truncated)"
	}
	if msg == "" {
		msg = fmt.Sprintf("Found %d result(s) (%s search); no formatted summary.", result.Total, result.Method)
	}
	p.addEvent("info", "Discover: semantic search executed", map[string]interface{}{
		"query": query, "total": result.Total, "method": result.Method,
	})
	return &heimdall.ActionResult{
		Success: true,
		Message: msg,
		Data:    map[string]interface{}{"results": result.Results, "total": result.Total},
	}, nil
}

// actionQuery executes a read-only Cypher query and returns rows for the LLM.
func (p *WatcherPlugin) actionQuery(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	cypher, _ := ctx.Params["cypher"].(string)
	if cypher == "" {
		return &heimdall.ActionResult{
			Success: false,
			Message: "cypher parameter required",
		}, nil
	}
	if len(cypher) > 10000 {
		return &heimdall.ActionResult{
			Success: false,
			Message: "query too long (max 10000 characters)",
		}, nil
	}
	dbName := getDatabaseParam(ctx.Params)
	if dbName == "" && ctx.Database != nil {
		dbName = ctx.Database.DefaultDatabaseName()
	}
	var params map[string]interface{}
	if p, ok := ctx.Params["params"].(map[string]interface{}); ok {
		params = p
	}

	if ctx.Database == nil {
		return &heimdall.ActionResult{Success: false, Message: "database not available"}, nil
	}

	rows, err := ctx.Database.Query(ctx.Context, dbName, cypher, params)
	if err != nil {
		log.Printf("[Watcher] query failed: %v", err)
		return &heimdall.ActionResult{
			Success: false,
			Message: fmt.Sprintf("Query failed: %v", err),
		}, nil
	}
	p.addEvent("info", "Query: Cypher executed", map[string]interface{}{
		"database": dbName, "rows": len(rows),
	})
	return &heimdall.ActionResult{
		Success: true,
		Message: fmt.Sprintf("Query returned %d row(s)", len(rows)),
		Data:    map[string]interface{}{"rows": rows},
	}, nil
}

// actionHello is a simple test action to verify Heimdall is working.
// Prompt examples that should trigger this:
//   - "say hello"
//   - "test the system"
//   - "hello world"
//   - "run a test action"
func (p *WatcherPlugin) actionHello(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	name := "World"
	if n, ok := ctx.Params["name"].(string); ok && n != "" {
		name = n
	}

	greeting := fmt.Sprintf("Hello, %s! 👋 Heimdall is operational and ready to serve.", name)
	p.addEvent("info", greeting, nil)

	return &heimdall.ActionResult{
		Success: true,
		Message: greeting,
		Data: map[string]interface{}{
			"greeting":  greeting,
			"timestamp": time.Now().Format(time.RFC3339),
			"model":     p.config["model"],
			"status":    string(p.status),
		},
	}, nil
}

func (p *WatcherPlugin) actionStatus(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	health := p.Health()
	pluginMetrics := p.Metrics()

	// Collect comprehensive status
	status := map[string]interface{}{
		"heimdall": map[string]interface{}{
			"health":  health,
			"metrics": pluginMetrics,
			"config":  p.Config(),
		},
	}

	// Add database stats if available
	if ctx.Database != nil {
		dbName := getDatabaseParam(ctx.Params)
		dbStats, err := ctx.Database.Stats(dbName)
		if err == nil {
			status["database"] = map[string]interface{}{
				"database":      coalesceString(dbName, ctx.Database.DefaultDatabaseName()),
				"nodes":         dbStats.NodeCount,
				"relationships": dbStats.RelationshipCount,
				"labels":        dbStats.LabelCounts,
			}
		} else {
			status["database"] = map[string]interface{}{
				"database": coalesceString(dbName, ctx.Database.DefaultDatabaseName()),
				"error":    err.Error(),
			}
		}
	}

	// Add runtime metrics if available
	if ctx.Metrics != nil {
		runtimeMetrics := ctx.Metrics.Runtime()
		status["runtime"] = map[string]interface{}{
			"goroutines": runtimeMetrics.GoroutineCount,
			"memory_mb":  runtimeMetrics.MemoryAllocMB,
			"gc_cycles":  runtimeMetrics.NumGC,
		}
	} else {
		// Fallback to direct runtime stats
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		status["runtime"] = map[string]interface{}{
			"goroutines": runtime.NumGoroutine(),
			"memory_mb":  m.Alloc / 1024 / 1024,
			"gc_cycles":  m.NumGC,
		}
	}

	return &heimdall.ActionResult{
		Success: true,
		Message: fmt.Sprintf("NornicDB Status: %s, Uptime: %.0fs, Goroutines: %d",
			health.Status, pluginMetrics["uptime_seconds"], runtime.NumGoroutine()),
		Data: status,
	}, nil
}

func (p *WatcherPlugin) actionHealth(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	health := p.Health()

	return &heimdall.ActionResult{
		Success: health.Healthy,
		Message: health.Message,
		Data: map[string]interface{}{
			"health": health,
		},
	}, nil
}

func (p *WatcherPlugin) actionConfig(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	return &heimdall.ActionResult{
		Success: true,
		Message: "Current SLM configuration",
		Data: map[string]interface{}{
			"config": p.Config(),
			"schema": p.ConfigSchema(),
		},
	}, nil
}

func (p *WatcherPlugin) actionSetConfig(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	if err := p.Configure(ctx.Params); err != nil {
		p.mu.Lock()
		p.errors++
		p.mu.Unlock()
		return &heimdall.ActionResult{
			Success: false,
			Message: fmt.Sprintf("Configuration error: %v", err),
		}, nil
	}

	return &heimdall.ActionResult{
		Success: true,
		Message: "Configuration updated successfully",
		Data: map[string]interface{}{
			"config": p.Config(),
		},
	}, nil
}

func (p *WatcherPlugin) actionMetrics(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	// Collect comprehensive metrics
	metrics := map[string]interface{}{
		"heimdall": p.Metrics(),
	}

	// Add runtime metrics
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	metrics["runtime"] = map[string]interface{}{
		"goroutines":      runtime.NumGoroutine(),
		"memory_alloc_mb": m.Alloc / 1024 / 1024,
		"memory_sys_mb":   m.Sys / 1024 / 1024,
		"heap_alloc_mb":   m.HeapAlloc / 1024 / 1024,
		"heap_inuse_mb":   m.HeapInuse / 1024 / 1024,
		"stack_inuse_mb":  m.StackInuse / 1024 / 1024,
		"gc_cycles":       m.NumGC,
		"gc_pause_ns":     m.PauseTotalNs,
	}

	// Add database stats if available
	if ctx.Database != nil {
		dbName := getDatabaseParam(ctx.Params)
		dbStats, err := ctx.Database.Stats(dbName)
		if err == nil {
			metrics["database"] = map[string]interface{}{
				"database":      coalesceString(dbName, ctx.Database.DefaultDatabaseName()),
				"nodes":         dbStats.NodeCount,
				"relationships": dbStats.RelationshipCount,
				"labels":        dbStats.LabelCounts,
			}
		} else {
			metrics["database"] = map[string]interface{}{
				"database": coalesceString(dbName, ctx.Database.DefaultDatabaseName()),
				"error":    err.Error(),
			}
		}
	}

	// Add metrics reader data if available
	if ctx.Metrics != nil {
		runtimeFromReader := ctx.Metrics.Runtime()
		metrics["runtime_reader"] = runtimeFromReader
	}

	return &heimdall.ActionResult{
		Success: true,
		Message: fmt.Sprintf("NornicDB Metrics: %d goroutines, %d MB memory, %d GC cycles",
			runtime.NumGoroutine(), m.Alloc/1024/1024, m.NumGC),
		Data: metrics,
	}, nil
}

func (p *WatcherPlugin) actionEvents(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	limit := 10
	if l, ok := ctx.Params["limit"].(int); ok && l > 0 {
		limit = l
	}

	events := p.RecentEvents(limit)

	return &heimdall.ActionResult{
		Success: true,
		Message: fmt.Sprintf("Heimdall recalls %d events", len(events)),
		Data: map[string]interface{}{
			"events": events,
		},
	}, nil
}

// actionBroadcast demonstrates using Bifrost to broadcast messages to all connected clients.
func (p *WatcherPlugin) actionBroadcast(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	msg, ok := ctx.Params["message"].(string)
	if !ok || msg == "" {
		return &heimdall.ActionResult{
			Success: false,
			Message: "Missing required parameter: message",
		}, nil
	}

	// Use Bifrost to broadcast the message
	if ctx.Bifrost != nil {
		if err := ctx.Bifrost.Broadcast(fmt.Sprintf("📢 Heimdall announces: %s", msg)); err != nil {
			p.mu.Lock()
			p.errors++
			p.mu.Unlock()
			return &heimdall.ActionResult{
				Success: false,
				Message: fmt.Sprintf("Failed to broadcast via Bifrost: %v", err),
			}, nil
		}
	}

	p.addEvent("info", fmt.Sprintf("Broadcast sent: %s", msg), nil)

	return &heimdall.ActionResult{
		Success: true,
		Message: fmt.Sprintf("Message broadcast to %d connected clients", ctx.Bifrost.ConnectionCount()),
		Data: map[string]interface{}{
			"message":     msg,
			"connections": ctx.Bifrost.ConnectionCount(),
		},
	}, nil
}

// actionNotify demonstrates using Bifrost to send typed notifications.
func (p *WatcherPlugin) actionNotify(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	notifType, _ := ctx.Params["type"].(string)
	title, _ := ctx.Params["title"].(string)
	message, _ := ctx.Params["message"].(string)

	if notifType == "" {
		notifType = "info"
	}
	if title == "" {
		title = "Heimdall"
	}
	if message == "" {
		return &heimdall.ActionResult{
			Success: false,
			Message: "Missing required parameter: message",
		}, nil
	}

	// Use Bifrost to send notification
	if ctx.Bifrost != nil {
		if err := ctx.Bifrost.SendNotification(notifType, title, message); err != nil {
			p.mu.Lock()
			p.errors++
			p.mu.Unlock()
			return &heimdall.ActionResult{
				Success: false,
				Message: fmt.Sprintf("Failed to send notification via Bifrost: %v", err),
			}, nil
		}
	}

	p.addEvent(notifType, fmt.Sprintf("Notification sent: [%s] %s - %s", notifType, title, message), nil)

	return &heimdall.ActionResult{
		Success: true,
		Message: fmt.Sprintf("Notification sent: %s", message),
		Data: map[string]interface{}{
			"type":    notifType,
			"title":   title,
			"message": message,
		},
	}, nil
}

// actionDBStats returns comprehensive database statistics.
func (p *WatcherPlugin) actionDBStats(ctx heimdall.ActionContext) (*heimdall.ActionResult, error) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	stats := map[string]interface{}{}
	var msgBuilder strings.Builder

	// Get database stats if available
	if ctx.Database != nil {
		dbName := getDatabaseParam(ctx.Params)
		dbStats, err := ctx.Database.Stats(dbName)
		if err != nil {
			stats["database"] = map[string]interface{}{
				"database": coalesceString(dbName, ctx.Database.DefaultDatabaseName()),
				"error":    err.Error(),
			}
			msgBuilder.WriteString(fmt.Sprintf("DATABASE: error reading stats for %s: %v\n\n",
				coalesceString(dbName, ctx.Database.DefaultDatabaseName()), err))
		} else {
			stats["database"] = map[string]interface{}{
				"database":      coalesceString(dbName, ctx.Database.DefaultDatabaseName()),
				"nodes":         dbStats.NodeCount,
				"relationships": dbStats.RelationshipCount,
				"labels":        dbStats.LabelCounts,
			}

			msgBuilder.WriteString(fmt.Sprintf("DATABASE (%s): %d nodes, %d relationships\n\n",
				coalesceString(dbName, ctx.Database.DefaultDatabaseName()),
				dbStats.NodeCount, dbStats.RelationshipCount))

			// Add cluster/search stats if available
			if dbStats.ClusterStats != nil {
				stats["clustering"] = map[string]interface{}{
					"embedding_count":    dbStats.ClusterStats.EmbeddingCount,
					"num_clusters":       dbStats.ClusterStats.NumClusters,
					"is_clustered":       dbStats.ClusterStats.IsClustered,
					"avg_cluster_size":   dbStats.ClusterStats.AvgClusterSize,
					"cluster_iterations": dbStats.ClusterStats.Iterations,
				}

				msgBuilder.WriteString("CLUSTERING:\n")
				msgBuilder.WriteString(fmt.Sprintf("  • Embeddings: %d\n", dbStats.ClusterStats.EmbeddingCount))
				msgBuilder.WriteString(fmt.Sprintf("  • K-Means Clusters: %d\n", dbStats.ClusterStats.NumClusters))
				if dbStats.ClusterStats.IsClustered {
					msgBuilder.WriteString(fmt.Sprintf("  • Clustered: Yes (avg size: %.1f, iterations: %d)\n",
						dbStats.ClusterStats.AvgClusterSize, dbStats.ClusterStats.Iterations))
				} else {
					msgBuilder.WriteString("  • Clustered: No\n")
				}
				msgBuilder.WriteString("\n")
			}

			// Add feature flags if available
			if dbStats.FeatureFlags != nil {
				stats["feature_flags"] = map[string]interface{}{
					"heimdall_enabled":           dbStats.FeatureFlags.HeimdallEnabled,
					"heimdall_anomaly_detection": dbStats.FeatureFlags.HeimdallAnomalyDetection,
					"heimdall_runtime_diagnosis": dbStats.FeatureFlags.HeimdallRuntimeDiagnosis,
					"heimdall_memory_curation":   dbStats.FeatureFlags.HeimdallMemoryCuration,
					"clustering_enabled":         dbStats.FeatureFlags.ClusteringEnabled,
					"topology_enabled":           dbStats.FeatureFlags.TopologyEnabled,
					"kalman_enabled":             dbStats.FeatureFlags.KalmanEnabled,
					"async_writes_enabled":       dbStats.FeatureFlags.AsyncWritesEnabled,
				}

				msgBuilder.WriteString("FEATURE FLAGS:\n")
				msgBuilder.WriteString(fmt.Sprintf("  • Heimdall AI: %s\n", boolToStatus(dbStats.FeatureFlags.HeimdallEnabled)))
				msgBuilder.WriteString(fmt.Sprintf("  • Anomaly Detection: %s\n", boolToStatus(dbStats.FeatureFlags.HeimdallAnomalyDetection)))
				msgBuilder.WriteString(fmt.Sprintf("  • Runtime Diagnosis: %s\n", boolToStatus(dbStats.FeatureFlags.HeimdallRuntimeDiagnosis)))
				msgBuilder.WriteString(fmt.Sprintf("  • Memory Curation: %s\n", boolToStatus(dbStats.FeatureFlags.HeimdallMemoryCuration)))
				msgBuilder.WriteString(fmt.Sprintf("  • K-Means Clustering: %s\n", boolToStatus(dbStats.FeatureFlags.ClusteringEnabled)))
				msgBuilder.WriteString(fmt.Sprintf("  • Topology Prediction: %s\n", boolToStatus(dbStats.FeatureFlags.TopologyEnabled)))
				msgBuilder.WriteString(fmt.Sprintf("  • Kalman Filtering: %s\n", boolToStatus(dbStats.FeatureFlags.KalmanEnabled)))
				msgBuilder.WriteString(fmt.Sprintf("  • Async Writes: %s\n", boolToStatus(dbStats.FeatureFlags.AsyncWritesEnabled)))
				msgBuilder.WriteString("\n")
			}
		}
	}

	// Get runtime stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	stats["runtime"] = map[string]interface{}{
		"goroutines":      runtime.NumGoroutine(),
		"memory_alloc_mb": m.Alloc / 1024 / 1024,
		"heap_objects":    m.HeapObjects,
		"gc_cycles":       m.NumGC,
	}

	msgBuilder.WriteString("RUNTIME:\n")
	msgBuilder.WriteString(fmt.Sprintf("  • Goroutines: %d\n", runtime.NumGoroutine()))
	msgBuilder.WriteString(fmt.Sprintf("  • Memory: %d MB\n", m.Alloc/1024/1024))
	msgBuilder.WriteString(fmt.Sprintf("  • GC Cycles: %d\n", m.NumGC))

	// Get metrics if available
	if ctx.Metrics != nil {
		runtimeMetrics := ctx.Metrics.Runtime()
		stats["metrics"] = runtimeMetrics
	}

	return &heimdall.ActionResult{
		Success: true,
		Message: msgBuilder.String(),
		Data:    stats,
	}, nil
}

// boolToStatus converts a boolean to a user-friendly status string.
func boolToStatus(b bool) string {
	if b {
		return "✅ Enabled"
	}
	return "❌ Disabled"
}

func getDatabaseParam(params map[string]interface{}) string {
	if params == nil {
		return ""
	}
	if v, ok := params["database"].(string); ok {
		return strings.TrimSpace(v)
	}
	if v, ok := params["db"].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

func coalesceString(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// === Data Access Methods ===

func (p *WatcherPlugin) Summary() string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return fmt.Sprintf("Heimdall watches: Status=%s, Model=%s, Uptime=%.0fs, Requests=%d, Errors=%d",
		p.status,
		p.config["model"],
		time.Since(p.started).Seconds(),
		p.requests,
		p.errors,
	)
}

func (p *WatcherPlugin) RecentEvents(limit int) []heimdall.SubsystemEvent {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if limit <= 0 || limit > len(p.events) {
		limit = len(p.events)
	}

	// Return most recent events
	start := len(p.events) - limit
	if start < 0 {
		start = 0
	}

	result := make([]heimdall.SubsystemEvent, limit)
	copy(result, p.events[start:])
	return result
}

// === Request Lifecycle Hooks ===

// PrePrompt is called before the prompt is sent to Heimdall.
// The ActionPrompt is immutable (already set). We can add context here.
//
// === GRAPH-RAG IMPLEMENTATION ===
// This hook automatically enriches every user query with relevant knowledge
// from the graph database using semantic search with neighbor traversal.
//
// Flow:
// 1. User asks "when is welcome season?"
// 2. PrePrompt runs semantic search on the query
// 3. Related nodes (neighbors) are retrieved
// 4. Context is injected into AdditionalInstructions
// 5. Heimdall now has the knowledge to answer
func (p *WatcherPlugin) PrePrompt(ctx *heimdall.PromptContext) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	msgPreview := ctx.UserMessage
	if len(msgPreview) > 50 {
		msgPreview = msgPreview[:50] + "..."
	}
	log.Printf("[Watcher] PrePrompt: request=%s user_msg=%q", ctx.RequestID, msgPreview)

	// === GRAPH-RAG: Semantic search to enrich context ===
	ctx.NotifyProgress("Graph-RAG", "Searching knowledge graph...")
	ragContext := p.performGraphRAG(ctx)
	if ragContext != "" {
		ctx.AdditionalInstructions += ragContext
		ctx.NotifyInfo("Graph-RAG", "Found relevant knowledge from graph")
	}

	// Add watcher-specific context to help Heimdall understand the system
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Store current metrics in PluginData for later phases
	if ctx.PluginData == nil {
		ctx.PluginData = make(map[string]interface{})
	}
	ctx.PluginData["watcher_preprompt_time"] = time.Now()
	ctx.PluginData["watcher_goroutines"] = runtime.NumGoroutine()
	ctx.PluginData["watcher_memory_mb"] = m.Alloc / 1024 / 1024

	// Add status examples to help with natural language → action mapping
	// NOTE: Domain-specific examples (welcome season, war rooms) go in domain plugins
	ctx.Examples = append(ctx.Examples,
		heimdall.PromptExample{
			UserSays:   "check the system",
			ActionJSON: `{"action": "heimdall_watcher_status", "params": {}}`,
		},
		heimdall.PromptExample{
			UserSays:   "get status",
			ActionJSON: `{"action": "heimdall_watcher_status", "params": {}}`,
		},
		// Database stats queries -> db_stats (node/edge counts, labels, etc.)
		heimdall.PromptExample{
			UserSays:   "how many nodes are there",
			ActionJSON: `{"action": "heimdall_watcher_db_stats", "params": {}}`,
		},
		heimdall.PromptExample{
			UserSays:   "how many nodes",
			ActionJSON: `{"action": "heimdall_watcher_db_stats", "params": {}}`,
		},
		heimdall.PromptExample{
			UserSays:   "what is the database status",
			ActionJSON: `{"action": "heimdall_watcher_db_stats", "params": {}}`,
		},
		heimdall.PromptExample{
			UserSays:   "show database info",
			ActionJSON: `{"action": "heimdall_watcher_db_stats", "params": {}}`,
		},
		heimdall.PromptExample{
			UserSays:   "database statistics",
			ActionJSON: `{"action": "heimdall_watcher_db_stats", "params": {}}`,
		},
		heimdall.PromptExample{
			UserSays:   "how many relationships",
			ActionJSON: `{"action": "heimdall_watcher_db_stats", "params": {}}`,
		},
		heimdall.PromptExample{
			UserSays:   "show node labels",
			ActionJSON: `{"action": "heimdall_watcher_db_stats", "params": {}}`,
		},
		// K-means clustering and feature flag queries -> db_stats
		heimdall.PromptExample{
			UserSays:   "how many k-means clusters",
			ActionJSON: `{"action": "heimdall_watcher_db_stats", "params": {}}`,
		},
		heimdall.PromptExample{
			UserSays:   "show clustering stats",
			ActionJSON: `{"action": "heimdall_watcher_db_stats", "params": {}}`,
		},
		heimdall.PromptExample{
			UserSays:   "how many embeddings",
			ActionJSON: `{"action": "heimdall_watcher_db_stats", "params": {}}`,
		},
		heimdall.PromptExample{
			UserSays:   "what features are enabled",
			ActionJSON: `{"action": "heimdall_watcher_db_stats", "params": {}}`,
		},
		heimdall.PromptExample{
			UserSays:   "show feature flags",
			ActionJSON: `{"action": "heimdall_watcher_db_stats", "params": {}}`,
		},
		// Generic semantic search examples
		heimdall.PromptExample{
			UserSays:   "search for X",
			ActionJSON: `{"action": "heimdall_watcher_discover", "params": {"query": "X"}}`,
		},
		heimdall.PromptExample{
			UserSays:   "find information about Y",
			ActionJSON: `{"action": "heimdall_watcher_discover", "params": {"query": "Y"}}`,
		},
	)

	p.addEvent("info", "PrePrompt hook executed with Graph-RAG", map[string]interface{}{
		"request_id":   ctx.RequestID,
		"user_msg":     ctx.UserMessage[:min(50, len(ctx.UserMessage))],
		"has_history":  len(ctx.Messages) > 0,
		"rag_enriched": ragContext != "",
	})

	return nil
}

// performGraphRAG performs semantic search on the user's message and returns
// formatted context to inject into the prompt. This is the core of Graph-RAG.
func (p *WatcherPlugin) performGraphRAG(ctx *heimdall.PromptContext) string {
	// Skip if no database access or message is too short
	if p.ctx.Database == nil || len(ctx.UserMessage) < 5 {
		return ""
	}

	// Skip for system commands (status, metrics, etc.)
	lowered := strings.ToLower(ctx.UserMessage)
	skipPhrases := []string{"status", "metrics", "health", "config", "hello", "hi", "help"}
	for _, phrase := range skipPhrases {
		if strings.HasPrefix(lowered, phrase) {
			return ""
		}
	}

	// Perform semantic search with depth=1 for relationship traversal from matched nodes
	result, err := p.ctx.Database.Discover(
		context.Background(),
		"", // default database
		ctx.UserMessage,
		nil, // all node types
		5,   // limit to top 5 results
		1,   // depth 1: include direct relationship neighbors
	)
	if err != nil {
		log.Printf("[Watcher] Graph-RAG search failed: %v", err)
		return ""
	}

	if result.Total == 0 {
		return ""
	}
	return formatDiscoverResult(result)
}

// internalDiscoverPropertyKeys are property names to omit from Graph-RAG context (embedding/managed internals).
var internalDiscoverPropertyKeys = map[string]bool{
	"embedding":            true,
	"embedding_model":      true,
	"embedding_dimensions": true,
	"has_embedding":        true,
	"embedded_at":          true,
	"chunk_count":          true,
	"orderLevelLookupKey":  true,
	"fetchedAt":            true,
	"id":                   true,
	"created_at":           true,
	"updated_at":           true,
}

// formatDiscoverResult formats DiscoverResult as context text for the LLM (PrePrompt or action result).
// Excludes internal/embedding properties; otherwise general-purpose (no domain-specific ordering or emphasis).
func formatDiscoverResult(result *heimdall.DiscoverResult) string {
	if result == nil || len(result.Results) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("\n\n=== KNOWLEDGE FROM GRAPH DATABASE ===\n")
	sb.WriteString("The following information was found in the knowledge graph and may help answer the user's question:\n\n")

	for i, r := range result.Results {
		title := r.Title
		if title == "" {
			title = r.ID
		}
		sb.WriteString(fmt.Sprintf("### %d. [%s] %s\n", i+1, r.Type, title))

		if r.ContentPreview != "" {
			sb.WriteString(fmt.Sprintf("Content: %s\n", r.ContentPreview))
		}

		if len(r.Properties) > 0 {
			sb.WriteString("Properties:\n")
			for k, v := range r.Properties {
				if internalDiscoverPropertyKeys[k] {
					continue
				}
				valStr := fmt.Sprint(v)
				if len(valStr) > 300 {
					valStr = valStr[:300] + "..."
				}
				sb.WriteString(fmt.Sprintf("  - %s: %s\n", k, valStr))
			}
		}

		if len(r.Related) > 0 {
			sb.WriteString("Related:\n")
			shown := 0
			for _, rel := range r.Related {
				if shown >= 3 {
					sb.WriteString(fmt.Sprintf("  ... and %d more related items\n", len(r.Related)-3))
					break
				}
				relTitle := rel.Title
				if relTitle == "" {
					relTitle = rel.ID
				}
				direction := "→"
				if rel.Direction == "incoming" {
					direction = "←"
				}
				sb.WriteString(fmt.Sprintf("  %s [%s] %s (via %s)\n", direction, rel.Type, relTitle, rel.Relationship))
				shown++
			}
		}
		sb.WriteString("\n")
	}

	sb.WriteString("=== END KNOWLEDGE ===\n\n")
	sb.WriteString("The above lists nodes (with Properties) and their Related nodes (relationship links). Use this information to answer the user in one short sentence. Do not call any action when this knowledge answers the question.\n")

	return sb.String()
}

// PreExecute is called after Heimdall responds, before action execution.
// We can fetch additional data or modify params here.
//
// This demonstrates:
// - Async validation with callback
// - Sending notifications before action runs
// - Cancelling with ctx.Cancel() method
// - Modifying params before execution
func (p *WatcherPlugin) PreExecute(ctx *heimdall.PreExecuteContext, done func(heimdall.PreExecuteResult)) {
	p.mu.Lock()
	p.requests++
	p.mu.Unlock()

	log.Printf("[Watcher] PreExecute: request=%s action=%s params=%v", ctx.RequestID, ctx.Action, ctx.Params)

	// === EXAMPLE: Send notification that we're about to execute ===
	ctx.NotifyInfo("Watcher", fmt.Sprintf("Executing action: %s", ctx.Action))

	// Log the action being executed
	p.addEvent("info", fmt.Sprintf("PreExecute: %s", ctx.Action), map[string]interface{}{
		"request_id": ctx.RequestID,
		"action":     ctx.Action,
		"params":     ctx.Params,
	})

	// For certain actions, we might want to fetch additional context
	// This is async so we don't block the response
	go func() {
		// === EXAMPLE: Validation for query actions ===
		if ctx.Action == "heimdall_watcher_query" {
			if cypher, ok := ctx.Params["cypher"].(string); ok {
				// Basic safety check
				if len(cypher) > 10000 {
					// Send warning notification (async)
					ctx.NotifyWarning("Query Validation", "Query too long, aborting")

					done(heimdall.PreExecuteResult{
						Continue:     false,
						AbortMessage: "Query too long (max 10000 chars)",
					})
					return
				}

				// === EXAMPLE: Notify about query analysis ===
				ctx.NotifyProgress("Query Analysis", "Validating Cypher query...")
			}
		}

		// === EXAMPLE: Cancel via context method (alternative to callback) ===
		// This is another way to cancel - useful when you want to
		// cancel from deep in nested code
		// if someCondition {
		//     ctx.Cancel("Validation failed", "PreExecute:watcher")
		//     done(heimdall.PreExecuteResult{Continue: false})
		//     return
		// }

		// Default: continue with execution
		done(heimdall.PreExecuteResult{
			Continue: true,
		})
	}()
}

// PostExecute is called after action execution completes.
// We log metrics and update state here.
//
// This demonstrates:
// - Logging execution metrics
// - Sending completion notifications to UI
// - Tracking error counts
// - Accessing execution timing from context
func (p *WatcherPlugin) PostExecute(ctx *heimdall.PostExecuteContext) {
	p.mu.Lock()
	defer p.mu.Unlock()

	log.Printf("[Watcher] PostExecute: request=%s action=%s duration=%v", ctx.RequestID, ctx.Action, ctx.Duration)

	// === EXAMPLE: Check if request was cancelled in earlier phase ===
	if ctx.WasCancelled && ctx.CancellationInfo != nil {
		p.addEvent("warning", fmt.Sprintf("Request was cancelled: %s", ctx.CancellationInfo.Reason), map[string]interface{}{
			"request_id":   ctx.RequestID,
			"cancelled_by": ctx.CancellationInfo.CancelledBy,
			"phase":        ctx.CancellationInfo.Phase,
		})
		return
	}

	// Log execution metrics
	executionTime := float64(ctx.Duration.Microseconds()) / 1000
	p.addEvent("info", fmt.Sprintf("PostExecute: %s (%.2fms)", ctx.Action, executionTime), map[string]interface{}{
		"request_id": ctx.RequestID,
		"action":     ctx.Action,
		"duration":   ctx.Duration.String(),
		"success":    ctx.Result != nil && ctx.Result.Success,
	})

	// Track errors
	if ctx.Result != nil && !ctx.Result.Success {
		p.errors++
	}

	// === Send completion notification inline ===
	// PostExecute notifications are queued and sent after the action result
	if ctx.Result != nil && ctx.Result.Success {
		ctx.NotifySuccess("Watcher", fmt.Sprintf("Action completed in %.2fms", executionTime))
	} else if ctx.Result != nil {
		ctx.NotifyError("Watcher", fmt.Sprintf("Action failed: %s", ctx.Result.Message))
	}
}

// Synthesize implements SynthesisHook to transform action results into conversational prose.
// This is the reference implementation showing how plugins can provide rich, domain-specific
// response formatting using LLM-based synthesis.
//
// The Watcher plugin demonstrates:
//   - Using the Heimdall invoker to call the LLM for prose synthesis
//   - Falling back gracefully if synthesis fails
//   - Detecting pre-formatted messages to avoid double-processing
//
// Domain-specific plugins (like Welcome Season) can override this with their own synthesis logic.
func (p *WatcherPlugin) Synthesize(ctx *heimdall.SynthesisContext, done func(response string)) {
	// Skip synthesis for failed actions
	if ctx.Result == nil || !ctx.Result.Success {
		done("") // Let default handler format errors
		return
	}

	// If there's no structured data, just return the message
	if len(ctx.Result.Data) == 0 {
		done(ctx.Result.Message)
		return
	}

	// Check if Heimdall invoker is available for LLM synthesis
	if p.ctx.Heimdall == nil {
		log.Printf("[Watcher] Heimdall invoker not available, using pre-formatted message")
		// Fall back to the action's formatted message if it exists
		if ctx.Result.Message != "" {
			done(ctx.Result.Message)
			return
		}
		done("") // Fall back to default JSON formatting
		return
	}

	// Build synthesis prompt for the LLM
	// This transforms structured data into conversational prose
	synthesisPrompt := p.buildSynthesisPrompt(ctx.UserQuestion, ctx.Result.Message, ctx.Result.Data)

	log.Printf("[Watcher] Generating LLM synthesis for user question: %s", ctx.UserQuestion)

	// Call the LLM via Heimdall invoker using raw prompt (no action routing)
	result, err := p.ctx.Heimdall.SendRawPrompt(synthesisPrompt)
	if err != nil {
		log.Printf("[Watcher] LLM synthesis failed: %v, falling back to formatted message", err)
		if ctx.Result.Message != "" {
			done(ctx.Result.Message)
			return
		}
		done("")
		return
	}

	if result == nil || !result.Success || result.Message == "" {
		log.Printf("[Watcher] LLM returned empty/unsuccessful result, using formatted message")
		if ctx.Result.Message != "" {
			done(ctx.Result.Message)
			return
		}
		done("")
		return
	}

	// Clean up the response
	response := strings.TrimSpace(result.Message)

	// If the model output JSON (it tried to route an action), fall back
	if strings.HasPrefix(response, "{") || strings.HasPrefix(response, "[") {
		log.Printf("[Watcher] LLM returned JSON instead of prose, using formatted message")
		if ctx.Result.Message != "" {
			done(ctx.Result.Message)
			return
		}
		done("")
		return
	}

	// Quality check: if response is too short relative to the data,
	// the LLM probably failed to synthesize properly - use formatted message
	if len(response) < 50 && ctx.Result.Message != "" && len(ctx.Result.Message) > len(response)*2 {
		log.Printf("[Watcher] LLM response too short (%d chars), using formatted message (%d chars)",
			len(response), len(ctx.Result.Message))
		done(ctx.Result.Message)
		return
	}

	log.Printf("[Watcher] LLM synthesis successful (%d chars)", len(response))
	done(response)
}

// buildSynthesisPrompt creates a prompt that instructs the LLM to generate
// a conversational answer from structured data.
func (p *WatcherPlugin) buildSynthesisPrompt(userQuestion, actionMessage string, data map[string]interface{}) string {
	// Format data as readable JSON
	dataJSON, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		dataJSON = []byte(fmt.Sprintf("%v", data))
	}

	// Build the prompt with clear structure
	var prompt strings.Builder
	prompt.WriteString("You are Heimdall, the AI assistant for NornicDB. Answer the user's question using the data provided.\n\n")
	prompt.WriteString(fmt.Sprintf("USER QUESTION: %s\n\n", userQuestion))

	// Include the formatted summary if available
	if actionMessage != "" {
		prompt.WriteString("FORMATTED DATA:\n")
		prompt.WriteString(actionMessage)
		prompt.WriteString("\n\n")
	}

	// Include raw JSON data for reference
	prompt.WriteString("RAW DATA (JSON):\n")
	prompt.WriteString(string(dataJSON))
	prompt.WriteString("\n\n")

	prompt.WriteString(`INSTRUCTIONS:
1. Answer the user's question directly and completely
2. Include ALL relevant data points from above - don't omit information
3. Format your response with clear sections if there are multiple categories
4. Use bullet points (•) for lists
5. For feature flags, list each flag with its status (enabled/disabled)
6. For statistics, include the actual numbers
7. Be helpful and informative, not brief
8. Do NOT output JSON - write natural language
9. Do NOT suggest commands - just present the data

YOUR RESPONSE:`)

	return prompt.String()
}

// === Internal Helpers ===

func (p *WatcherPlugin) addEvent(eventType, message string, data map[string]interface{}) {
	event := heimdall.SubsystemEvent{
		Time:    time.Now(),
		Type:    eventType,
		Message: message,
		Data:    data,
	}

	p.events = append(p.events, event)

	// Keep only last 100 events
	if len(p.events) > 100 {
		p.events = p.events[len(p.events)-100:]
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
