/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package configs

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/placement/types"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
)

const (
	RootQueue        = "root"
	DOT              = "."
	DotReplace       = "_dot_"
	DefaultPartition = "default"

	ApplicationSortPolicy   = "application.sort.policy"
	ApplicationSortPriority = "application.sort.priority"
	PriorityPolicy          = "priority.policy"
	PriorityOffset          = "priority.offset"
	PreemptionPolicy        = "preemption.policy"
	PreemptionDelay         = "preemption.delay"

	// app sort priority values
	ApplicationSortPriorityEnabled  = "enabled"
	ApplicationSortPriorityDisabled = "disabled"
)

// Priority
var MinPriority int32 = math.MinInt32
var MaxPriority int32 = math.MaxInt32

var DefaultPreemptionDelay = 30 * time.Second

// A queue can be a username with the dot replaced. Most systems allow a 32 character user name.
// The queue name must thus allow for at least that length with the replacement of dots.
var QueueNameRegExp = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// User and group name check: systems allow different things POSIX is the base but we need to be lenient and allow more.
// allow upper and lower case, add the @ and . (dot) and officially no length.
var UserRegExp = regexp.MustCompile(`^[_a-zA-Z][a-zA-Z0-9_.@-]*[$]?$`)

// Groups should have a slightly more restrictive regexp (no @ . or $ at the end)
var GroupRegExp = regexp.MustCompile(`^[_a-zA-Z][a-zA-Z0-9_-]*$`)

// all characters that make a name different from a regexp
var SpecialRegExp = regexp.MustCompile(`[\^$*+?()\[{}|]`)

// The rule maps to a go identifier check that regexp only
var RuleNameRegExp = regexp.MustCompile(`^[_a-zA-Z][a-zA-Z0-9_]*$`)

type placementStaticPath struct {
	path      string
	ruleChain string
	create    bool
	ruleNo    int
}

type placementPathCheckResult int

const (
	checkOK placementPathCheckResult = iota
	nonExistingQueue
	queueNotParent
)

// Check the ACL
func checkACL(acl string) error {
	// trim any white space
	acl = strings.TrimSpace(acl)
	// handle special cases: deny and wildcard
	if len(acl) == 0 || acl == security.WildCard {
		return nil
	}

	// should have no more than two groups defined
	fields := strings.Fields(acl)
	if len(fields) > 2 {
		return fmt.Errorf("multiple spaces found in ACL: '%s'", acl)
	}
	return nil
}

func checkQueueResource(cur QueueConfig, parentM *resources.Resource) (*resources.Resource, error) {
	curG, curM, err := checkResourceConfig(cur)
	if err != nil {
		return nil, err
	}
	if !parentM.FitInMaxUndef(curM) {
		return nil, fmt.Errorf("max resource of parent %s is smaller than maximum resource %s for queue %s", parentM.String(), curM.String(), cur.Name)
	}
	curM = resources.ComponentWiseMinPermissive(curM, parentM)
	sumG := resources.NewResource()
	for _, child := range cur.Queues {
		var childG *resources.Resource
		childG, err = checkQueueResource(child, curM)
		if err != nil {
			return nil, err
		}
		sumG.AddTo(childG)
	}
	if !resources.IsZero(curG) && !resources.FitIn(curG, sumG) {
		return nil, fmt.Errorf("guaranteed resource of parent %s is smaller than sum of guaranteed resources %s of the children for queue %s", curG.String(), sumG.String(), cur.Name)
	}
	if !curM.FitInMaxUndef(sumG) {
		return nil, fmt.Errorf("max resource %s is smaller than sum of guaranteed resources %s of the children for queue %s", curM.String(), sumG.String(), cur.Name)
	}
	if resources.IsZero(curG) {
		return sumG, nil
	}
	return curG, nil
}

func checkQueueMaxApplications(cur QueueConfig) error {
	var err error
	for _, child := range cur.Queues {
		if cur.MaxApplications != 0 && (cur.MaxApplications < child.MaxApplications || child.MaxApplications == 0) {
			return fmt.Errorf("parent maxRunningApps must be larger than child maxRunningApps")
		}
		err = checkQueueMaxApplications(child)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkResourceConfig(cur QueueConfig) (*resources.Resource, *resources.Resource, error) {
	var g, m *resources.Resource
	var err error
	g, err = resources.NewResourceFromConf(cur.Resources.Guaranteed)
	if err != nil {
		return nil, nil, err
	}
	m, err = resources.NewResourceFromConf(cur.Resources.Max)
	if err != nil {
		return nil, nil, err
	}
	if !m.FitInMaxUndef(g) {
		return nil, nil, fmt.Errorf("guaranteed resource %s is larger than maximum resource %s for queue %s", g.String(), m.String(), cur.Name)
	}
	return g, m, nil
}

// Check the placement rules for correctness
func checkPlacementRules(partition *PartitionConfig) error {
	// return if nothing defined
	if partition.PlacementRules == nil || len(partition.PlacementRules) == 0 {
		return nil
	}

	log.Logger().Debug("checking placement rule config",
		zap.String("partitionName", partition.Name))
	// top level rule checks, parents are called recursively
	for _, rule := range partition.PlacementRules {
		if err := checkPlacementRule(rule); err != nil {
			return err
		}
	}

	placementStaticPaths := getLongestPlacementPaths(partition.PlacementRules)
	for _, staticPath := range placementStaticPaths {
		queuePath := staticPath.path
		parts := strings.Split(strings.ToLower(queuePath), DOT)
		result := checkQueueHierarchyForPlacement(parts, staticPath.create, partition.Queues)
		if result == queueNotParent {
			return fmt.Errorf("placement rule no. #%d (%s) references a queue (%s) which is a leaf",
				staticPath.ruleNo, staticPath.ruleChain, queuePath)
		}
		if result == nonExistingQueue {
			return fmt.Errorf("placement rule no. #%d (%s) references non-existing queues (%s) and create is 'false'",
				staticPath.ruleNo, staticPath.ruleChain, queuePath)
		}
	}

	return nil
}

func checkQueueHierarchyForPlacement(path []string, create bool, conf []QueueConfig) placementPathCheckResult {
	queueName := path[0]

	// no more queues in the configuration
	if len(conf) == 0 {
		if !create {
			return nonExistingQueue
		}
		return checkOK
	}

	var queueConf *QueueConfig
	for _, queue := range conf {
		queue := queue
		if queue.Name == queueName {
			queueConf = &queue
			break
		}
	}

	// queue not found on this level
	if queueConf == nil {
		if !create {
			return nonExistingQueue
		}
		return checkOK
	}

	if !queueConf.Parent {
		return queueNotParent
	}

	if len(path) == 1 {
		return checkOK
	}

	path = path[1:]
	return checkQueueHierarchyForPlacement(path, create, queueConf.Queues)
}

// Check the specific rule for syntax.
// The create flag is checked automatically by the config parser and is not checked.
func checkPlacementRule(rule PlacementRule) error {
	// name must be valid go as it normally maps 1:1 to an object
	if !RuleNameRegExp.MatchString(rule.Name) {
		return fmt.Errorf("invalid rule name %s, a name must be a valid identifier", rule.Name)
	}
	// check the parent rule
	if rule.Parent != nil {
		if err := checkPlacementRule(*rule.Parent); err != nil {
			log.Logger().Debug("parent placement rule failed",
				zap.String("rule", rule.Name),
				zap.String("parentRule", rule.Parent.Name))
			return err
		}
	}
	// check filter if given
	if err := checkPlacementFilter(rule.Filter); err != nil {
		log.Logger().Debug("placement rule filter failed",
			zap.String("rule", rule.Name),
			zap.Any("filter", rule.Filter))
		return err
	}
	return nil
}

// Check the filter for syntax issues
// Trickery for the regexp part to make sure we filter out just a name and do not see it as a regexp.
// If the list is 1 item check if it is a valid user, then compile as a regexp and check for regexp characters.
// Each check must pass.
// If the list is more than one item we see all as a name and ignore anything that does not comply when initialising the filter.
func checkPlacementFilter(filter Filter) error {
	// empty (equals allow) or the literal "allow" or "deny" (case insensitive)
	if filter.Type != "" && !strings.EqualFold(filter.Type, "allow") && !strings.EqualFold(filter.Type, "deny") {
		return fmt.Errorf("invalid rule filter type %s, filter type  must be either '', allow or deny", filter.Type)
	}
	// check users and groups: as long as we have 1 good entry we accept it and continue
	// anything that does not parse in a list of users is ignored (like ACL list)
	if len(filter.Users) == 1 {
		// for a length of 1 we could either have regexp or username
		isUser := UserRegExp.MatchString(filter.Users[0])
		// if it is not a user name it must be a regexp
		// two step check: first compile if that fails it is
		if !isUser {
			if _, err := regexp.Compile(filter.Users[0]); err != nil || !SpecialRegExp.MatchString(filter.Users[0]) {
				return fmt.Errorf("invalid rule filter user list is not a proper list or regexp: %v", filter.Users)
			}
		}
	}
	if len(filter.Groups) == 1 {
		// for a length of 1 we could either have regexp or groupname
		isGroup := GroupRegExp.MatchString(filter.Groups[0])
		// if it is not a group name it must be a regexp
		if !isGroup {
			if _, err := regexp.Compile(filter.Groups[0]); err != nil || !SpecialRegExp.MatchString(filter.Groups[0]) {
				return fmt.Errorf("invalid rule filter group list is not a proper list or regexp: %v", filter.Groups)
			}
		}
	}
	return nil
}

// Check a single limit entry
func checkLimit(limit Limit, currIdx int, userWildCardIdx, groupWildCardIdx *int) error {
	if len(limit.Users) == 0 && len(limit.Groups) == 0 {
		return fmt.Errorf("empty user and group lists defined in limit '%v'", limit)
	}

	for _, name := range limit.Users {
		if name != "*" && !UserRegExp.MatchString(name) {
			return fmt.Errorf("invalid limit user name '%s' in limit definition", name)
		}
		// The user without wildcard should not happen after the wildcard user
		// It means the wildcard for user should be the last item for limits object list which including the username,
		// and we should only set one wildcard user for all limits
		if name == "*" {
			if *userWildCardIdx != -1 && currIdx > *userWildCardIdx {
				return fmt.Errorf("should not set more than one wildcard user")
			}
			*userWildCardIdx = currIdx
		} else if *userWildCardIdx != -1 && currIdx > *userWildCardIdx {
			return fmt.Errorf("should not set no wildcard user %s after wildcard user limit", name)
		}
	}
	for _, name := range limit.Groups {
		if name != "*" && !GroupRegExp.MatchString(name) {
			return fmt.Errorf("invalid limit group name '%s' in limit definition", name)
		}
		// The group without wildcard should not happen after the wildcard group
		// It means the wildcard for group should be the last item for limits object list which including the group name,
		// and we should only set one wildcard group for all limits
		if name == "*" {
			if *groupWildCardIdx != -1 && currIdx > *groupWildCardIdx {
				return fmt.Errorf("should not set more than one wildcard group")
			}
			*groupWildCardIdx = currIdx
		} else if *groupWildCardIdx != -1 && currIdx > *groupWildCardIdx {
			return fmt.Errorf("should not set no wildcard group %s after wildcard group limit", name)
		}
	}
	var limitResource = resources.NewResource()
	var err error
	// check the resource (if defined)
	if len(limit.MaxResources) != 0 {
		limitResource, err = resources.NewResourceFromConf(limit.MaxResources)
		if err != nil {
			log.Logger().Debug("resource parsing failed",
				zap.Error(err))
			return err
		}
	}
	// at least some resource should be not null
	if limit.MaxApplications == 0 && resources.IsZero(limitResource) {
		return fmt.Errorf("invalid resource combination for limit names '%s' all resource limits are null", limit.Users)
	}
	return nil
}

// Check the defined limits list
func checkLimits(limits []Limit, obj string) error {
	// return if nothing defined
	if len(limits) == 0 {
		return nil
	}
	// walk over the list of limits
	log.Logger().Debug("checking limits configs",
		zap.String("objName", obj),
		zap.Int("limitsLength", len(limits)))

	var userWildCardIdx = -1
	var groupWildCardIdx = -1
	for index, limit := range limits {
		if err := checkLimit(limit, index, &userWildCardIdx, &groupWildCardIdx); err != nil {
			return err
		}
	}
	return nil
}

// Check for global policy
func checkNodeSortingPolicy(partition *PartitionConfig) error {
	// get the policy
	policy := partition.NodeSortPolicy

	// Defined polices.
	_, err := policies.SortingPolicyFromString(policy.Type)
	if err != nil {
		return err
	}

	for k, v := range policy.ResourceWeights {
		if v < float64(0) {
			return fmt.Errorf("negative resource weight for %s is not allowed", k)
		}
	}

	return nil
}

// Check the queue names configured for compliance and uniqueness
// - no duplicate names at each branched level in the tree
// - queue name is alphanumeric (case ignore) with - and _
// - queue name is maximum 16 char long
func checkQueues(queue *QueueConfig, level int) error {
	// check the ACLs (if defined)
	err := checkACL(queue.AdminACL)
	if err != nil {
		return err
	}
	err = checkACL(queue.SubmitACL)
	if err != nil {
		return err
	}

	// check the limits for this child (if defined)
	err = checkLimits(queue.Limits, queue.Name)
	if err != nil {
		return err
	}

	// check this level for name compliance and uniqueness
	queueMap := make(map[string]bool)
	for _, child := range queue.Queues {
		if !QueueNameRegExp.MatchString(child.Name) {
			return fmt.Errorf("invalid child name %s, a name must only have alphanumeric characters,"+
				" - or _, and be no longer than 64 characters", child.Name)
		}
		if queueMap[strings.ToLower(child.Name)] {
			return fmt.Errorf("duplicate child name found with name %s, level %d", child.Name, level)
		}
		queueMap[strings.ToLower(child.Name)] = true
	}

	// recurse into the depth if this level passed
	for _, child := range queue.Queues {
		err = checkQueues(&child, level+1)
		if err != nil {
			return err
		}
	}
	return nil
}

// Check the structure of the queue in the config:
// - exactly 1 root queue, added if missing
// - the parent flag is set on queues that are missing it
// - no duplicates at each level
// - name must comply with regexp
func checkQueuesStructure(partition *PartitionConfig) error {
	if partition.Queues == nil {
		return fmt.Errorf("queue config is not set")
	}

	log.Logger().Debug("checking partition queue config",
		zap.String("partitionName", partition.Name))

	// handle no root queue cases
	var insertRoot bool
	if len(partition.Queues) != 1 {
		// multiple or no top level queues: insert the root queue
		insertRoot = true
	} else {
		// A single queue at the top must be the root queue, if not insert it
		if strings.ToLower(partition.Queues[0].Name) != RootQueue {
			insertRoot = true
		} else {
			// make sure root is a parent
			partition.Queues[0].Parent = true
		}
	}

	// insert the root queue if not there
	if insertRoot {
		log.Logger().Debug("inserting root queue",
			zap.Int("numOfQueues", len(partition.Queues)))
		var rootQueue QueueConfig
		rootQueue.Name = RootQueue
		rootQueue.Parent = true
		rootQueue.Queues = partition.Queues
		var newRoot []QueueConfig
		newRoot = append(newRoot, rootQueue)
		partition.Queues = newRoot
	}

	// check name uniqueness: we have a root to start with directly
	var rootQueue = partition.Queues[0]
	// special check for root resources: must not be set
	if rootQueue.Resources.Guaranteed != nil || rootQueue.Resources.Max != nil {
		return fmt.Errorf("root queue must not have resource limits set")
	}
	return checkQueues(&rootQueue, 1)
}

// Check the state dump file path, if configured, is a valid path that can be written to.
func checkDeprecatedStateDumpFilePath(partition *PartitionConfig) error {
	if partition.StateDumpFilePath != "" {
		log.Logger().Warn("Ignoring deprecated partition setting 'statedumpfilepath'. This parameter will be removed in a future release.")
	}
	return nil
}

func ensureDir(fileName string) error {
	if err := os.MkdirAll(filepath.Dir(fileName), os.ModePerm); err != nil {
		return err
	}
	return nil
}

// Check the partition configuration. Any parsing issues will return an error which means that the
// configuration is invalid. This *must* be called before the configuration is activated. Any
// configuration that does not pass must be rejected.
// Check performed:
// - at least 1 partition must be defined
// - no more than 1 partition called "default"
// For the sub components:
// - The queue config is syntax checked
// - The placement rules are syntax checked
// - The user objects are syntax checked
func Validate(newConfig *SchedulerConfig) error {
	if newConfig == nil {
		return fmt.Errorf("scheduler config is not set")
	}

	// check uniqueness
	partitionMap := make(map[string]bool)
	for i, partition := range newConfig.Partitions {
		if partition.Name == "" || strings.ToLower(partition.Name) == DefaultPartition {
			partition.Name = DefaultPartition
		}
		if partitionMap[strings.ToLower(partition.Name)] {
			return fmt.Errorf("duplicate partition name found with name %s", partition.Name)
		}
		partitionMap[strings.ToLower(partition.Name)] = true
		// check the queue structure
		err := checkQueuesStructure(&partition)
		if err != nil {
			return err
		}
		_, err = checkQueueResource(partition.Queues[0], nil)
		if err != nil {
			return err
		}
		err = checkPlacementRules(&partition)
		if err != nil {
			return err
		}
		err = checkLimits(partition.Limits, partition.Name)
		if err != nil {
			return err
		}
		err = checkNodeSortingPolicy(&partition)
		if err != nil {
			return err
		}
		err = checkDeprecatedStateDumpFilePath(&newConfig.Partitions[i])
		if err != nil {
			return err
		}
		err = checkQueueMaxApplications(partition.Queues[0])
		if err != nil {
			return err
		}
		// write back the partition to keep changes
		newConfig.Partitions[i] = partition
	}
	return nil
}

// returns the longest fixed queue path defined by the placement rule chain
// e.g. the chain is user->tag->fixed, returns something like "root.users.<tag>.<user>",
// the longest static part is "root.users"
func getLongestPlacementPaths(rules []PlacementRule) []placementStaticPath {
	paths := make([]placementStaticPath, 0)

	for i, rule := range rules {
		path, ruleChain, _ := getLongestStaticPath(rule)
		placementPath := placementStaticPath{
			path:      path,
			create:    rule.Create,
			ruleChain: ruleChain,
			ruleNo:    i,
		}
		paths = append(paths, placementPath)
	}

	return paths
}

func getLongestStaticPath(rule PlacementRule) (string, string, bool) {
	var parentPath string
	var ruleChain string
	dynamicParent := false

	if rule.Parent != nil {
		var chain string
		parentPath, chain, dynamicParent = getLongestStaticPath(*rule.Parent)
		ruleChain = rule.Name + "->" + chain
	} else {
		ruleChain = rule.Name
		parentPath = RootQueue
	}

	if rule.Name == types.Fixed {
		queueName := strings.ToLower(rule.Value)
		qualified := strings.HasPrefix(queueName, RootQueue)
		if qualified {
			return queueName, ruleChain, false
		}
		// there is a parent rule other than "fixed", we can't do anything about that
		if dynamicParent {
			return RootQueue, ruleChain, true
		}
		return parentPath + "." + queueName, ruleChain, false
	}

	return parentPath, ruleChain, true
}
