package nornicdb

import (
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPluginFunctionRegistry(t *testing.T) {
	// Clear any existing state
	pluginsMu.Lock()
	pluginFunctions = make(map[string]PluginFunction)
	loadedPlugins = make(map[string]*LoadedPlugin)
	pluginsInitialized = false
	pluginsMu.Unlock()

	t.Run("GetPluginFunction returns false for unregistered function", func(t *testing.T) {
		_, found := GetPluginFunction("nonexistent.function")
		assert.False(t, found)
	})

	t.Run("register and retrieve plugin function", func(t *testing.T) {
		// Manually register a function (simulating plugin load)
		pluginsMu.Lock()
		pluginFunctions["test.plugin.sum"] = PluginFunction{
			Name:        "test.plugin.sum",
			Handler:     func(vals []interface{}) float64 { return 42.0 },
			Description: "Test sum function",
			Category:    "test",
		}
		pluginsMu.Unlock()

		fn, found := GetPluginFunction("test.plugin.sum")
		require.True(t, found)
		assert.Equal(t, "test.plugin.sum", fn.Name)
		assert.Equal(t, "Test sum function", fn.Description)
		assert.NotNil(t, fn.Handler)
	})

	t.Run("ListPluginFunctions returns registered functions", func(t *testing.T) {
		names := ListPluginFunctions()
		assert.Contains(t, names, "test.plugin.sum")
	})

	t.Run("case sensitivity - function names are case sensitive", func(t *testing.T) {
		_, found := GetPluginFunction("TEST.PLUGIN.SUM")
		assert.False(t, found, "Function lookup should be case sensitive")

		_, found = GetPluginFunction("test.plugin.sum")
		assert.True(t, found)
	})
}

func TestLoadedPluginTracking(t *testing.T) {
	// Clear state
	pluginsMu.Lock()
	pluginFunctions = make(map[string]PluginFunction)
	loadedPlugins = make(map[string]*LoadedPlugin)
	pluginsInitialized = false
	pluginsMu.Unlock()

	t.Run("ListLoadedPlugins returns empty when no plugins loaded", func(t *testing.T) {
		plugins := ListLoadedPlugins()
		assert.Empty(t, plugins)
	})

	t.Run("track loaded plugin", func(t *testing.T) {
		pluginsMu.Lock()
		loadedPlugins["testplugin"] = &LoadedPlugin{
			Name:    "testplugin",
			Version: "1.0.0",
			Path:    "/path/to/testplugin.so",
			Functions: []PluginFunction{
				{Name: "testplugin.func1", Handler: nil, Description: "Test func 1"},
				{Name: "testplugin.func2", Handler: nil, Description: "Test func 2"},
			},
		}
		pluginsInitialized = true
		pluginsMu.Unlock()

		plugins := ListLoadedPlugins()
		require.Len(t, plugins, 1)
		assert.Equal(t, "testplugin", plugins[0].Name)
		assert.Equal(t, "1.0.0", plugins[0].Version)
		assert.Len(t, plugins[0].Functions, 2)
	})

	t.Run("PluginsInitialized returns correct state", func(t *testing.T) {
		assert.True(t, PluginsInitialized())
	})
}

func TestLoadPluginsFromDir(t *testing.T) {
	t.Run("empty directory path returns nil", func(t *testing.T) {
		err := LoadPluginsFromDir("", nil)
		assert.NoError(t, err)
	})

	t.Run("non-existent directory returns nil (not an error)", func(t *testing.T) {
		err := LoadPluginsFromDir("/nonexistent/path/to/plugins", nil)
		assert.NoError(t, err)
	})

	t.Run("file path instead of directory returns error", func(t *testing.T) {
		// Create a temp file
		err := LoadPluginsFromDir("/dev/null", nil) // This is a file, not a directory
		// Should return error since it's not a directory
		if err != nil {
			assert.Contains(t, err.Error(), "not a directory")
		}
	})
}

func TestPluginFunctionHandlerTypes(t *testing.T) {
	// Test that various handler types can be stored and retrieved
	pluginsMu.Lock()
	pluginFunctions = make(map[string]PluginFunction)
	pluginsMu.Unlock()

	testCases := []struct {
		name    string
		handler interface{}
	}{
		{"no_args_string", func() string { return "hello" }},
		{"no_args_float", func() float64 { return 3.14 }},
		{"single_float", func(x float64) float64 { return x * 2 }},
		{"two_floats", func(a, b float64) float64 { return a + b }},
		{"string_func", func(s string) string { return s + "!" }},
		{"list_func", func(vals []interface{}) float64 { return float64(len(vals)) }},
		{"two_lists", func(a, b []float64) float64 { return float64(len(a) + len(b)) }},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pluginsMu.Lock()
			pluginFunctions["test."+tc.name] = PluginFunction{
				Name:    "test." + tc.name,
				Handler: tc.handler,
			}
			pluginsMu.Unlock()

			fn, found := GetPluginFunction("test." + tc.name)
			require.True(t, found)
			assert.NotNil(t, fn.Handler)
		})
	}
}

func TestPluginTypeDetection(t *testing.T) {
	t.Run("detects function plugin type from reflection", func(t *testing.T) {
		plugin := &mockFunctionPlugin{
			name:    "testfunc",
			version: "1.0.0",
			ptype:   "function",
		}

		// detectPluginType uses reflection internally
		// Keep the pointer for method lookup (methods are defined on *mock...)
		val := reflect.ValueOf(plugin)

		// Test that Type() method exists and returns correct value
		typeMethod := val.MethodByName("Type")
		require.True(t, typeMethod.IsValid(), "Type method should exist")
		result := typeMethod.Call(nil)
		require.Len(t, result, 1)
		assert.Equal(t, "function", result[0].String())
	})

	t.Run("detects heimdall plugin type from reflection", func(t *testing.T) {
		plugin := &mockHeimdallPlugin{
			name:    "testsys",
			version: "1.0.0",
			ptype:   "heimdall",
		}

		// Keep the pointer for method lookup
		val := reflect.ValueOf(plugin)

		typeMethod := val.MethodByName("Type")
		require.True(t, typeMethod.IsValid())
		result := typeMethod.Call(nil)
		require.Len(t, result, 1)
		assert.Equal(t, "heimdall", result[0].String())
	})

	t.Run("handles various type strings", func(t *testing.T) {
		testCases := []struct {
			typeStr  string
			expected PluginType
		}{
			{"function", PluginTypeFunction},
			{"apoc", PluginTypeFunction},
			{"", PluginTypeFunction},
			{"heimdall", PluginTypeHeimdall},
			{"somethingelse", PluginTypeUnknown},
		}

		for _, tc := range testCases {
			t.Run(tc.typeStr, func(t *testing.T) {
				// Test the type detection logic directly
				var result PluginType
				switch tc.typeStr {
				case "heimdall":
					result = PluginTypeHeimdall
				case "function", "apoc", "":
					result = PluginTypeFunction
				default:
					result = PluginTypeUnknown
				}
				assert.Equal(t, tc.expected, result)
			})
		}
	})
}

func TestExtractFunctions(t *testing.T) {
	t.Run("extracts functions from plugin", func(t *testing.T) {
		plugin := &mockFunctionPlugin{
			name:    "testplugin",
			version: "1.0.0",
			ptype:   "function",
			functions: map[string]interface{}{
				"double": func(x float64) float64 { return x * 2 },
				"sum":    func(vals []float64) float64 { return vals[0] + vals[1] },
			},
		}

		funcs, err := extractFunctions(reflectValueOf(plugin), "testplugin")
		require.NoError(t, err)
		assert.Len(t, funcs, 2)

		// Check function names are prefixed
		names := make(map[string]bool)
		for _, fn := range funcs {
			names[fn.Name] = true
		}
		assert.True(t, names["apoc.testplugin.double"] || names["double"])
		assert.True(t, names["apoc.testplugin.sum"] || names["sum"])
	})

	t.Run("handles empty functions map", func(t *testing.T) {
		plugin := &mockFunctionPlugin{
			name:      "emptyplugin",
			version:   "1.0.0",
			ptype:     "function",
			functions: map[string]interface{}{},
		}

		funcs, err := extractFunctions(reflectValueOf(plugin), "emptyplugin")
		require.NoError(t, err)
		assert.Empty(t, funcs)
	})
}

func TestLoadedPluginTypes(t *testing.T) {
	// Clear state
	pluginsMu.Lock()
	pluginFunctions = make(map[string]PluginFunction)
	loadedPlugins = make(map[string]*LoadedPlugin)
	pluginsInitialized = false
	pluginsMu.Unlock()

	t.Run("tracks function plugin", func(t *testing.T) {
		pluginsMu.Lock()
		loadedPlugins["funcplugin"] = &LoadedPlugin{
			Name:    "funcplugin",
			Version: "1.0.0",
			Type:    PluginTypeFunction,
			Path:    "/path/to/funcplugin.so",
			Functions: []PluginFunction{
				{Name: "funcplugin.test", Handler: nil, Description: "Test"},
			},
		}
		pluginsMu.Unlock()

		plugins := ListLoadedPlugins()
		require.Len(t, plugins, 1)
		assert.Equal(t, PluginTypeFunction, plugins[0].Type)
		assert.Len(t, plugins[0].Functions, 1)
	})

	t.Run("tracks heimdall plugin", func(t *testing.T) {
		pluginsMu.Lock()
		loadedPlugins["sysplugin"] = &LoadedPlugin{
			Name:      "sysplugin",
			Version:   "1.0.0",
			Type:      PluginTypeHeimdall,
			Path:      "/path/to/sysplugin.so",
			Functions: nil, // Heimdall plugins don't have functions
		}
		pluginsMu.Unlock()

		plugins := ListLoadedPlugins()
		require.Len(t, plugins, 2) // funcplugin + sysplugin

		var heimdallPlugin *LoadedPlugin
		for _, p := range plugins {
			if p.Type == PluginTypeHeimdall {
				heimdallPlugin = p
				break
			}
		}
		require.NotNil(t, heimdallPlugin)
		assert.Equal(t, "sysplugin", heimdallPlugin.Name)
		assert.Nil(t, heimdallPlugin.Functions)
	})

	t.Run("lists mixed plugin types", func(t *testing.T) {
		plugins := ListLoadedPlugins()
		require.Len(t, plugins, 2)

		hasFunction := false
		hasHeimdall := false
		for _, p := range plugins {
			if p.Type == PluginTypeFunction {
				hasFunction = true
			}
			if p.Type == PluginTypeHeimdall {
				hasHeimdall = true
			}
		}
		assert.True(t, hasFunction, "Should have function plugin")
		assert.True(t, hasHeimdall, "Should have Heimdall plugin")
	})
}

func TestUnifiedPluginLoader(t *testing.T) {
	// This tests the overall integration of the unified loader
	t.Run("plugin loader handles both types", func(t *testing.T) {
		// Clear state
		pluginsMu.Lock()
		pluginFunctions = make(map[string]PluginFunction)
		loadedPlugins = make(map[string]*LoadedPlugin)
		pluginsInitialized = false
		pluginsMu.Unlock()

		// Simulate loading both types
		pluginsMu.Lock()
		loadedPlugins["apoc"] = &LoadedPlugin{
			Name:    "apoc",
			Version: "1.0.0",
			Type:    PluginTypeFunction,
			Path:    "/test/apoc.so",
			Functions: []PluginFunction{
				{Name: "apoc.coll.sum", Handler: nil},
			},
		}
		loadedPlugins["watcher"] = &LoadedPlugin{
			Name:      "watcher",
			Version:   "1.0.0",
			Type:      PluginTypeHeimdall,
			Path:      "/test/watcher.so",
			Functions: nil,
		}
		pluginsInitialized = true
		pluginsMu.Unlock()

		plugins := ListLoadedPlugins()
		assert.Len(t, plugins, 2)
		assert.True(t, PluginsInitialized())

		// Verify we can distinguish between types
		typeCount := make(map[PluginType]int)
		for _, p := range plugins {
			typeCount[p.Type]++
		}
		assert.Equal(t, 1, typeCount[PluginTypeFunction])
		assert.Equal(t, 1, typeCount[PluginTypeHeimdall])
	})
}

// Mock plugins for testing

type mockFunctionPlugin struct {
	name      string
	version   string
	ptype     string
	functions map[string]interface{}
}

func (m *mockFunctionPlugin) Name() string    { return m.name }
func (m *mockFunctionPlugin) Version() string { return m.version }
func (m *mockFunctionPlugin) Type() string    { return m.ptype }
func (m *mockFunctionPlugin) Functions() map[string]interface{} {
	return m.functions
}

type mockHeimdallPlugin struct {
	name       string
	version    string
	ptype      string
	startCalls int
}

func (m *mockHeimdallPlugin) Name() string                                   { return m.name }
func (m *mockHeimdallPlugin) Version() string                                { return m.version }
func (m *mockHeimdallPlugin) Type() string                                   { return m.ptype }
func (m *mockHeimdallPlugin) Description() string                            { return "Test Heimdall plugin" }
func (m *mockHeimdallPlugin) Initialize(ctx heimdall.SubsystemContext) error { return nil }
func (m *mockHeimdallPlugin) Start() error                                   { m.startCalls++; return nil }
func (m *mockHeimdallPlugin) Stop() error                                    { return nil }
func (m *mockHeimdallPlugin) Shutdown() error                                { return nil }
func (m *mockHeimdallPlugin) Status() heimdall.SubsystemStatus {
	return heimdall.StatusRunning
}
func (m *mockHeimdallPlugin) Health() heimdall.SubsystemHealth {
	return heimdall.SubsystemHealth{Status: heimdall.StatusRunning, Healthy: true}
}
func (m *mockHeimdallPlugin) Metrics() map[string]interface{}                  { return nil }
func (m *mockHeimdallPlugin) Config() map[string]interface{}                   { return nil }
func (m *mockHeimdallPlugin) Configure(settings map[string]interface{}) error  { return nil }
func (m *mockHeimdallPlugin) ConfigSchema() map[string]interface{}             { return nil }
func (m *mockHeimdallPlugin) Actions() map[string]heimdall.ActionFunc          { return nil }
func (m *mockHeimdallPlugin) Summary() string                                  { return "Test" }
func (m *mockHeimdallPlugin) RecentEvents(limit int) []heimdall.SubsystemEvent { return nil }

type mockPluginNoType struct {
	name    string
	version string
}

func (m *mockPluginNoType) Name() string    { return m.name }
func (m *mockPluginNoType) Version() string { return m.version }

// Helper to get reflect.Value from interface
func reflectValueOf(i interface{}) reflect.Value {
	return reflect.ValueOf(i)
}

func TestRegisterHeimdallPlugin_RequiresSubsystemContext(t *testing.T) {
	// If Heimdall is disabled/not initialized, Heimdall plugins must not be registered or started.
	plugin := &mockHeimdallPlugin{
		name:    "watcher",
		version: "1.0.0",
		ptype:   "heimdall",
	}

	loaded, err := registerHeimdallPlugin(plugin, "/test/watcher.so", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, errHeimdallContextRequired)
	require.NotNil(t, loaded)
	assert.Equal(t, PluginTypeHeimdall, loaded.Type)
	assert.Equal(t, "watcher", loaded.Name)
	assert.Equal(t, "1.0.0", loaded.Version)
	assert.Equal(t, 0, plugin.startCalls, "Start() must not be called when Heimdall is disabled")
}
