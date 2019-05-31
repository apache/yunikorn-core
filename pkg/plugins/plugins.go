package plugins

var plugins = make(map[string]interface{})
var PredicatesPluginName = "predicates"

func GetSchedulerPlugin(name string) interface{} {
	return plugins[name]
}

func RegisterSchedulerPlugin(name string, plugin interface{}) {
	plugins[name] = plugin
}