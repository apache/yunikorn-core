package configs

// Plugins specifies multiple plugins in scheduling cycles.
type PluginsConfig struct {
	Plugins []*PluginConfig
}

// Plugin specifies a plugin name and its arguments when applicable.
type PluginConfig struct {
	// Name defines the name of plugin
	Name string
	// Args defines the arguments passed to the plugins at the time of initialization. Args can have arbitrary structure.
	Args interface{}
}
