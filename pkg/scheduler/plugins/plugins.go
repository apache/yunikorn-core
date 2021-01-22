package plugins

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/plugins/defaults"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

type SchedulingPlugins struct {
	applicationsPlugin ApplicationsPlugin
	requestsPlugin     RequestsPlugin

	sync.RWMutex
}

var plugins SchedulingPlugins

func init() {
	plugins = SchedulingPlugins{}
	// initialize default plugins
	Init(NewRegistry(), GetDefaultPluginsConfig())
}

func RegisterApplicationsPlugin(plugin interface{}) error {
	plugins.Lock()
	defer plugins.Unlock()
	if t, ok := plugin.(ApplicationsPlugin); ok {
		log.Logger().Info("register scheduler plugin: ApplicationsPlugin",
			zap.String("type", reflect.TypeOf(plugin).String()))
		plugins.applicationsPlugin = t
		return nil
	}
	return fmt.Errorf("%s can't be registered as ApplicationsPlugin", reflect.TypeOf(plugin).String())
}

func RegisterPlugin(plugin interface{}) error {
	plugins.Lock()
	defer plugins.Unlock()
	registeredPluginNames := make([]string, 0)
	if t, ok := plugin.(RequestsPlugin); ok {
		plugins.requestsPlugin = t
		registeredPluginNames = append(registeredPluginNames, "RequestsPlugin")
		log.Logger().Info("registered requests plugin", zap.String("type", reflect.TypeOf(plugin).String()))
	}
	if t, ok := plugin.(ApplicationsPlugin); ok {
		plugins.applicationsPlugin = t
		registeredPluginNames = append(registeredPluginNames, "ApplicationsPlugin")
		log.Logger().Info("registered applications plugin", zap.String("type", reflect.TypeOf(plugin).String()))
	}
	if len(registeredPluginNames) == 0 {
		return fmt.Errorf("%s can't be registered as a scheduling plugin", reflect.TypeOf(plugin).String())
	}
	return nil
}

func GetRequestsPlugin() RequestsPlugin {
	plugins.RLock()
	defer plugins.RUnlock()

	return plugins.requestsPlugin
}

func GetApplicationsPlugin() ApplicationsPlugin {
	plugins.RLock()
	defer plugins.RUnlock()

	return plugins.applicationsPlugin
}

// This plugin is responsible for creating new instances of Applications.
type ApplicationsPlugin interface {
	interfaces.Plugin
	// return a new instance of Applications
	NewApplications(queue interfaces.Queue) interfaces.Applications
}

// This plugin is responsible for creating new instances of Requests.
type RequestsPlugin interface {
	interfaces.Plugin
	// return a new instance of requests
	NewRequests() interfaces.Requests
}

func GetDefaultPluginsConfig() *configs.PluginsConfig {
	return &configs.PluginsConfig{
		Plugins: []*configs.PluginConfig{
			{
				Name: defaults.DefaultApplicationsPluginName,
			}, {
				Name: defaults.DefaultRequestsPluginName,
			},
		},
	}
}

func Init(registry Registry, pluginsConfig *configs.PluginsConfig) error {
	if pluginsConfig == nil {
		return fmt.Errorf("plugins config is nil")
	}
	// init requests plugin
	for _, pluginConfig := range pluginsConfig.Plugins {
		plugin, err := registry.GeneratePlugin(pluginConfig.Name,
			pluginConfig.Args)
		if err != nil {
			return err
		}
		err = RegisterPlugin(plugin)
		if err != nil {
			return err
		}
	}
	return nil
}
