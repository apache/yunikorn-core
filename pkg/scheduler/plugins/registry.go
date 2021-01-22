package plugins

import (
	"fmt"

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/plugins/defaults"
)

// PluginFactory is a function that builds a plugin.
type PluginFactory = func(args interface{}) (interfaces.Plugin, error)

// Registry is a collection of all available plugins. The framework uses a
// registry to enable and initialize configured plugins.
// All plugins must be in the registry before initializing the framework.
type Registry map[string]PluginFactory

// NewRegistry builds the registry with all plugins.
func NewRegistry() Registry {
	return Registry{
		defaults.DefaultApplicationsPluginName:   defaults.NewDefaultApplicationsPlugin,
		defaults.DefaultRequestsPluginName:       defaults.NewDefaultRequestsPlugin,
	}
}

// Register adds a new plugin to the registry. If a plugin with the same name
// exists, it returns an error.
func (r Registry) Register(name string, factory PluginFactory) error {
	if _, ok := r[name]; ok {
		return fmt.Errorf("a plugin named %v already exists", name)
	}
	r[name] = factory
	return nil
}

// Generate plugin by plugin name and arguments
func (r Registry) GeneratePlugin(name string, args interface{}) (interfaces.Plugin, error) {
	if pf, ok := r[name]; ok {
		return pf(args)
	}
	return nil, fmt.Errorf("unregistered plugin: %s", name)
}
