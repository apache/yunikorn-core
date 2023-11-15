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
	"regexp"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
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

	// placement rule validation
	placementOK placementPathCheckResult = iota
	errNonExistingQueue
	errQueueNotLeaf
	errLastQueueLeaf
)

type placementPathCheckResult int

// Priority
var MinPriority int32 = math.MinInt32
var MaxPriority int32 = math.MaxInt32

var DefaultPreemptionDelay = 30 * time.Second

// A queue can be a username with the dot replaced. Most systems allow a 32 character user name.
// The queue name must thus allow for at least that length with the replacement of dots.
var QueueNameRegExp = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// User and group name check: systems allow different things POSIX is the base but we need to be lenient and allow more.
// allow upper and lower case, add the @ and . (dot) and officially no length.
var UserRegExp = regexp.MustCompile(`^[_a-zA-Z][a-zA-Z0-9:_.@-]*[$]?$`)

// Groups should have a slightly more restrictive regexp (no @ . or $ at the end)
var GroupRegExp = regexp.MustCompile(`^[_a-zA-Z][a-zA-Z0-9:_.-]*$`)

// all characters that make a name different from a regexp
var SpecialRegExp = regexp.MustCompile(`[\^$*+?()\[{}|]`)

// The rule maps to a go identifier check that regexp only
var RuleNameRegExp = regexp.MustCompile(`^[_a-zA-Z][a-zA-Z0-9_]*$`)

type placementStaticPath struct {
	path           string
	ruleChain      string
	create         bool
	hasDynamicPart bool
	ruleNo         int
}

// Check the ACL
func checkACL(acl string) error {
	// trim any white space
	acl = strings.TrimSpace(acl)
	// handle special cases: deny and wildcard
	if len(acl) == 0 || acl == common.Wildcard {
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
	if !curG.FitInMaxUndef(sumG) {
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

func checkLimitResource(cur QueueConfig, users map[string]map[string]*resources.Resource, groups map[string]map[string]*resources.Resource, queuePath string) error {
	var curQueuePath string
	if cur.Name == RootQueue {
		curQueuePath = RootQueue
	} else {
		curQueuePath = queuePath + DOT + cur.Name
	}

	users[curQueuePath] = make(map[string]*resources.Resource)
	groups[curQueuePath] = make(map[string]*resources.Resource)

	// Carry forward (populate) the parent limit settings to the next level
	for u, v := range users[queuePath] {
		users[curQueuePath][u] = v.Clone()
	}
	for g, v := range groups[queuePath] {
		groups[curQueuePath][g] = v.Clone()
	}

	// compare user & group limit setting between the current queue and parent queue
	for _, limit := range cur.Limits {
		limitMaxResources, err := resources.NewResourceFromConf(limit.MaxResources)
		if err != nil {
			return err
		}

		for _, user := range limit.Users {
			// Is user limit setting exists?
			if userMaxResource, ok := users[queuePath][user]; ok {
				if !userMaxResource.FitInMaxUndef(limitMaxResources) {
					return fmt.Errorf("user %s max resource %s of queue %s is greater than immediate or ancestor parent maximum resource %s", user, limitMaxResources.String(), cur.Name, userMaxResource.String())
				}
				// Override with min resource
				users[curQueuePath][user] = resources.ComponentWiseMinPermissive(limitMaxResources, userMaxResource)
			} else {
				users[curQueuePath][user] = limitMaxResources
			}
		}
		for _, group := range limit.Groups {
			// Is group limit setting exists?
			if groupMaxResource, ok := groups[queuePath][group]; ok {
				if !groupMaxResource.FitInMaxUndef(limitMaxResources) {
					return fmt.Errorf("group %s max resource %s of queue %s is greater than immediate or ancestor parent maximum resource %s", group, limitMaxResources.String(), cur.Name, groupMaxResource.String())
				}
				// Override with min resource
				groups[curQueuePath][group] = resources.ComponentWiseMinPermissive(limitMaxResources, groupMaxResource)
			} else {
				groups[curQueuePath][group] = limitMaxResources
			}
		}
	}

	// traverse child queues
	for _, child := range cur.Queues {
		err := checkLimitResource(child, users, groups, curQueuePath)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkQueueMaxApplications(cur QueueConfig) error {
	var err error
	for _, child := range cur.Queues {
		if cur.MaxApplications != 0 && (cur.MaxApplications < child.MaxApplications || child.MaxApplications == 0) {
			return fmt.Errorf("parent maxApplications must be larger than child maxApplications")
		}
		err = checkQueueMaxApplications(child)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkLimitMaxApplications(cur QueueConfig, users map[string]map[string]uint64, groups map[string]map[string]uint64, queuePath string) error {
	var curQueuePath string
	if cur.Name == RootQueue {
		curQueuePath = RootQueue
	} else {
		curQueuePath = queuePath + DOT + cur.Name
	}

	// Carry forward (populate) the parent limit settings to the next level if limits are not configured for the current queue
	if len(cur.Limits) == 0 {
		if _, ok := users[queuePath]; ok {
			users[curQueuePath] = users[queuePath]
		}
		if _, ok := groups[queuePath]; ok {
			groups[curQueuePath] = groups[queuePath]
		}
	} else {
		// compare user & group limit setting between the current queue and parent queue
		for _, limit := range cur.Limits {
			limitMaxApplications := limit.MaxApplications
			for _, user := range limit.Users {
				// Is user limit setting exists?
				if userMaxApplications, ok := users[queuePath][user]; ok {
					if userMaxApplications != 0 && (userMaxApplications < limitMaxApplications || limitMaxApplications == 0) {
						return fmt.Errorf("user %s max applications %d of queue %s is greater than immediate or ancestor parent max applications %d", user, limitMaxApplications, cur.Name, userMaxApplications)
					}
					// Override with min resource
					if _, ok := users[curQueuePath]; !ok {
						users[curQueuePath] = make(map[string]uint64)
					}
					users[curQueuePath][user] = common.Min(limitMaxApplications, userMaxApplications)
				} else {
					if _, ok := users[curQueuePath]; !ok {
						users[curQueuePath] = make(map[string]uint64)
					}
					users[curQueuePath][user] = limitMaxApplications
				}
			}

			for _, group := range limit.Groups {
				// Is user limit setting exists?
				if groupMaxApplications, ok := groups[queuePath][group]; ok {
					if groupMaxApplications != 0 && (groupMaxApplications < limitMaxApplications || limitMaxApplications == 0) {
						return fmt.Errorf("group %s max applications %d of queue %s is greater than immediate or ancestor parent max applications %d", group, limitMaxApplications, cur.Name, groupMaxApplications)
					}
					// Override with min resource
					if _, ok := groups[curQueuePath]; !ok {
						groups[curQueuePath] = make(map[string]uint64)
					}
					groups[curQueuePath][group] = common.Min(limitMaxApplications, groupMaxApplications)
				} else {
					if _, ok := groups[curQueuePath]; !ok {
						groups[curQueuePath] = make(map[string]uint64)
					}
					groups[curQueuePath][group] = limitMaxApplications
				}
			}
		}
	}
	// traverse child queues
	for _, child := range cur.Queues {
		err := checkLimitMaxApplications(child, users, groups, curQueuePath)
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

	log.Log(log.Config).Debug("checking placement rule config",
		zap.String("partitionName", partition.Name))
	// top level rule checks, parents are called recursively
	for _, rule := range partition.PlacementRules {
		if err := checkPlacementRule(rule); err != nil {
			return err
		}
	}

	placementStaticPaths, err := getLongestPlacementPaths(partition.PlacementRules)
	if err != nil {
		return err
	}
	for _, staticPath := range placementStaticPaths {
		queuePath := staticPath.path
		create := staticPath.create
		hasDynamicPart := staticPath.hasDynamicPart

		parts := strings.Split(strings.ToLower(queuePath), DOT)
		result, lastQueue := checkQueueHierarchyForPlacement(parts, create, hasDynamicPart, partition.Queues, nil)
		if result == errQueueNotLeaf {
			return fmt.Errorf("placement rule no. #%d (%s) references a queue (%s) which is not a leaf",
				staticPath.ruleNo, staticPath.ruleChain, queuePath)
		}
		if result == errNonExistingQueue {
			return fmt.Errorf("placement rule no. #%d (%s) references non-existing queues (%s) and create is 'false'",
				staticPath.ruleNo, staticPath.ruleChain, queuePath)
		}
		if result == errLastQueueLeaf {
			return fmt.Errorf("placement rule no. #%d (%s) references non-existing queues (%s) which cannot be created because the last queue (%s) in the hierarchy is a leaf",
				staticPath.ruleNo, staticPath.ruleChain, queuePath, lastQueue)
		}
	}

	return nil
}

func checkQueueHierarchyForPlacement(path []string, create, hasDynamicPart bool, conf []QueueConfig, parentConf *QueueConfig) (result placementPathCheckResult, lastQueueName string) {
	queueName := path[0]
	lastQueueName = ""

	// no more queues in the configuration
	if len(conf) == 0 {
		if !parentConf.Parent {
			// path in the hierarchy is shorter, but the last queue is a leaf
			result = errLastQueueLeaf
			lastQueueName = parentConf.Name
			return
		}
		if !create {
			result = errNonExistingQueue
			return
		}
		result = placementOK
		return
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
			result = errNonExistingQueue
			return
		}
		result = placementOK
		return
	}

	if len(path) == 1 {
		if hasDynamicPart {
			// the "fixed" rule is followed by other rules like tag, user, etc. (root.dev.<user>),
			// which means that the "fixed" part must point to a parent
			if queueConf.Parent {
				result = placementOK
				return
			}
			result = errQueueNotLeaf
			return
		}
		if queueConf.Parent {
			result = errQueueNotLeaf
			return
		}
		result = placementOK
		return
	}

	path = path[1:]
	return checkQueueHierarchyForPlacement(path, create, hasDynamicPart, queueConf.Queues, queueConf)
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
			log.Log(log.Config).Debug("parent placement rule failed",
				zap.String("rule", rule.Name),
				zap.String("parentRule", rule.Parent.Name))
			return err
		}
	}
	// check filter if given
	if err := checkPlacementFilter(rule.Filter); err != nil {
		log.Log(log.Config).Debug("placement rule filter failed",
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
		user := filter.Users[0]
		isUser := UserRegExp.MatchString(user)
		// if it is not a user name it must be a regexp
		// two step check: first compile if that fails it is
		if !isUser {
			if _, err := regexp.Compile(user); err != nil || !SpecialRegExp.MatchString(user) {
				return fmt.Errorf("invalid rule filter user list is not a proper list or regexp: %v", filter.Users)
			}
		}
	}
	if len(filter.Groups) == 1 {
		// for a length of 1 we could either have regexp or groupname
		group := filter.Groups[0]
		isGroup := GroupRegExp.MatchString(group)
		// if it is not a group name it must be a regexp
		if !isGroup {
			if _, err := regexp.Compile(group); err != nil || !SpecialRegExp.MatchString(group) {
				return fmt.Errorf("invalid rule filter group list is not a proper list or regexp: %v", filter.Groups)
			}
		}
	}
	return nil
}

// Check a single limit entry
func checkLimit(limit Limit, existedUserName map[string]bool, existedGroupName map[string]bool, queue *QueueConfig) error {
	if len(limit.Users) == 0 && len(limit.Groups) == 0 {
		return fmt.Errorf("empty user and group lists defined in limit '%v'", limit)
	}

	for _, name := range limit.Users {
		if name != "*" && !UserRegExp.MatchString(name) {
			return fmt.Errorf("invalid limit user name '%s' in limit definition", name)
		}

		if existedUserName[name] {
			return fmt.Errorf("duplicated user name %s , already existed", name)
		}
		existedUserName[name] = true

		// The user without wildcard should not happen after the wildcard user
		// It means the wildcard for user should be the last item for limits object list which including the username,
		// and we should only set one wildcard user for all limits
		if existedUserName["*"] && name != "*" {
			return fmt.Errorf("should not set no wildcard user %s after wildcard user limit", name)
		}
	}
	for _, name := range limit.Groups {
		if name != "*" && !GroupRegExp.MatchString(name) {
			return fmt.Errorf("invalid limit group name '%s' in limit definition", name)
		}

		if existedGroupName[name] {
			return fmt.Errorf("duplicated group name %s , already existed", name)
		}
		existedGroupName[name] = true

		// The group without wildcard should not happen after the wildcard group
		// It means the wildcard for group should be the last item for limits object list which including the group name,
		// and we should only set one wildcard group for all limits
		if existedGroupName["*"] && name != "*" {
			return fmt.Errorf("should not set no wildcard group %s after wildcard group limit", name)
		}
	}

	// Specifying a wildcard for the group limit sets a cumulative limit for all users in that queue.
	// If there is no specific group mentioned the wildcard group limit would thus be the same as the queue limit.
	// For that reason we do not allow specifying only one group limit that is using the wildcard.
	// There must be at least one limit with a group name defined.
	if existedGroupName["*"] && len(existedGroupName) == 1 {
		return fmt.Errorf("should not specify only one group limit that is using the wildcard. " +
			"There must be at least one limit with a group name defined ")
	}

	var limitResource = resources.NewResource()
	var err error
	// check the resource (if defined)
	if len(limit.MaxResources) != 0 {
		limitResource, err = resources.NewResourceFromConf(limit.MaxResources)
		if err != nil {
			log.Log(log.Config).Debug("resource parsing failed",
				zap.Error(err))
			return err
		}
		if resources.IsZero(limitResource) {
			return fmt.Errorf("MaxResources is zero in '%s' limit, all resource types are zero", limit.Limit)
		}
	}
	// at least some resource should be not null
	if limit.MaxApplications == 0 && len(limit.MaxResources) == 0 {
		return fmt.Errorf("invalid resource combination for limit %s all resource limits are null", limit.Limit)
	}

	if queue.MaxApplications != 0 && queue.MaxApplications < limit.MaxApplications {
		return fmt.Errorf("invalid MaxApplications settings for limit %s exceed current the queue MaxApplications", limit.Limit)
	}

	// If queue is RootQueue, the queue.Resources.Max will be null, we don't need to check for root queue
	// But we may need to check the root resource during loading the config and after partition resource loading when update node
	if queue.Name != RootQueue {
		queueMaxResource, err := resources.NewResourceFromConf(queue.Resources.Max)
		if err != nil {
			log.Log(log.Config).Debug("resource parsing failed",
				zap.Error(err))
			return fmt.Errorf("parse queue %s max resource failed: %s", queue.Name, err.Error())
		}
		if !queueMaxResource.FitInMaxUndef(limitResource) {
			return fmt.Errorf("invalid MaxResources settings for limit %s exeecd current the queue MaxResources", limit.Limit)
		}
	}

	return nil
}

// Check the defined limits list
func checkLimits(limits []Limit, obj string, queue *QueueConfig) error {
	// return if nothing defined
	if len(limits) == 0 {
		return nil
	}
	// walk over the list of limits
	log.Log(log.Config).Debug("checking limits configs",
		zap.String("objName", obj),
		zap.Int("limitsLength", len(limits)))

	existedUserName := make(map[string]bool)
	existedGroupName := make(map[string]bool)

	for _, limit := range limits {
		if err := checkLimit(limit, existedUserName, existedGroupName, queue); err != nil {
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
	err = checkLimits(queue.Limits, queue.Name, queue)
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

	log.Log(log.Config).Debug("checking partition queue config",
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
		log.Log(log.Config).Debug("inserting root queue",
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
		log.Log(log.Deprecation).Warn("Ignoring deprecated partition setting 'statedumpfilepath'. This parameter will be removed in a future release.")
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
		err = checkLimits(partition.Limits, partition.Name, &partition.Queues[0])
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

		if err = checkLimitResource(partition.Queues[0], make(map[string]map[string]*resources.Resource), make(map[string]map[string]*resources.Resource), common.Empty); err != nil {
			return err
		}

		if err = checkLimitMaxApplications(partition.Queues[0], make(map[string]map[string]uint64), make(map[string]map[string]uint64), common.Empty); err != nil {
			return err
		}
		// write back the partition to keep changes
		newConfig.Partitions[i] = partition
	}
	return nil
}

// returns the longest fixed queue path defined by the placement rule chain
// e.g. the chain is fixed->tag->user, returns something like "root.users.<tag>.<user>",
// the longest static part is "root.users"
func getLongestPlacementPaths(rules []PlacementRule) ([]placementStaticPath, error) {
	paths := make([]placementStaticPath, 0)

	for i, rule := range rules {
		path, ruleChain, hasDynamicPart, err := getLongestStaticPath(rule)
		if err != nil {
			return nil, err
		}
		if strings.Index(path, RootQueue) != 0 {
			continue
		}
		placementPath := placementStaticPath{
			path:           path,
			create:         rule.Create,
			ruleChain:      ruleChain,
			hasDynamicPart: hasDynamicPart,
			ruleNo:         i,
		}
		paths = append(paths, placementPath)
	}

	return paths, nil
}

func getLongestStaticPath(rule PlacementRule) (staticPath, ruleChain string, foundDynamicRule bool, err error) {
	rules := getRuleChain(rule)

	for _, r := range rules {
		if ruleChain == "" {
			ruleChain = r.Name
		} else {
			ruleChain = ruleChain + "->" + r.Name
		}
		if foundDynamicRule {
			continue
		}

		if r.Name != types.Fixed {
			if staticPath == "" {
				staticPath = "<dynamic>"
			}
			foundDynamicRule = true
			continue
		}

		queueName := r.Value
		qualified := strings.HasPrefix(queueName, RootQueue)
		if qualified {
			if staticPath != "" {
				// error, only the first fixed rule can be fully qualified
				err = fmt.Errorf("illegal fully qualified 'fixed' rule with value %s", queueName)
				return staticPath, ruleChain, foundDynamicRule, err
			}
			staticPath = queueName
			continue
		}
		if staticPath == "" {
			staticPath = RootQueue
		}
		staticPath = staticPath + "." + queueName
	}

	return staticPath, ruleChain, foundDynamicRule, nil
}

func getRuleChain(r PlacementRule) []PlacementRule {
	rules := make([]PlacementRule, 0)
	if r.Parent != nil {
		rules = append(rules, getRuleChain(*r.Parent)...)
	}

	rules = append(rules, r)
	return rules
}
