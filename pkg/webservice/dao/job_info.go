package dao

type JobsDAOInfo struct {
	Jobs []JobDAOInfo `json:"jobs"`
}

type JobDAOInfo struct {
	JobID          string              `json:"jobID"`
	UsedResource   string              `json:"usedResource"`
	Partition      string              `json:"partition"`
	QueueName      string              `json:"queueName"`
	SubmissionTime int64               `json:"submissionTime"`
	Allocations    []AllocationDAOInfo `json:"allocations"`
}

type AllocationDAOInfo struct {
	AllocationKey    string            `json:"allocationKey"`
	AllocationTags   map[string]string `json:"allocationTags"`
	Uuid             string            `json:"uuid"`
	ResourcePerAlloc string            `json:"resource"`
	Priority         string            `json:"priority"`
	QueueName        string            `json:"queueName"`
	NodeId           string            `json:"nodeId"`
	JobId            string            `json:"jobId"`
	Partition        string            `json:"partition"`
}