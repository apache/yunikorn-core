package configs

func MockSchedulerConfigByData(data []byte) {
    SchedulerConfigLoader = func(policyGroup string) (config *SchedulerConfig, e error) {
        return LoadSchedulerConfigFromByteArray(data)
    }
}
