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

package ugm

import (
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/log"
)

var once sync.Once
var m *Manager

// Manager implements tracker. A User Group Manager to track the usage for both user and groups.
// Holds object of both user and group trackers
type Manager struct {
	userTrackers              map[string]*UserTracker
	groupTrackers             map[string]*GroupTracker
	userWildCardLimitsConfig  map[string]*LimitConfig    // Hold limits settings of user '*'
	groupWildCardLimitsConfig map[string]*LimitConfig    // Hold limits settings of group '*'
	configuredGroups          map[string][]string        // Hold groups for all configured queue paths.
	userLimits                map[string]map[string]bool // Holds queue path * user limit config
	groupLimits               map[string]map[string]bool // Holds queue path * group limit config
	sync.RWMutex
}

func newManager() *Manager {
	manager := &Manager{
		userTrackers:              make(map[string]*UserTracker),
		groupTrackers:             make(map[string]*GroupTracker),
		userWildCardLimitsConfig:  make(map[string]*LimitConfig),
		groupWildCardLimitsConfig: make(map[string]*LimitConfig),
	}
	return manager
}

func GetUserManager() *Manager {
	once.Do(func() {
		m = newManager()
	})
	return m
}

// LimitConfig Holds limit settings of wild card user/group
type LimitConfig struct {
	maxResources    *resources.Resource
	maxApplications uint64
}

// IncreaseTrackedResource Increase the resource usage for the given user group and queue path combination.
// As and when every allocation or asks requests fulfilled on application, corresponding user and group
// resource usage would be increased against specific application.
func (m *Manager) IncreaseTrackedResource(queuePath, applicationID string, usage *resources.Resource, user security.UserGroup) bool {
	log.Log(log.SchedUGM).Debug("Increasing resource usage",
		zap.String("user", user.User),
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage))
	if queuePath == common.Empty || applicationID == common.Empty || usage == nil || user.User == common.Empty {
		log.Log(log.SchedUGM).Debug("Mandatory parameters are missing to increase the resource usage")
		return false
	}
	// since we check headroom before an increase this should never result in a creation...
	// some tests might not go through a scheduling that cycle so leave this
	userTracker := m.getUserTracker(user.User)
	// make sure the user has a groupTracker for this application, if not yet there add it
	// since we check headroom before an increase this should never result in a call...
	// some tests might not go through a scheduling cycle so leave this
	if !userTracker.hasGroupForApp(applicationID) {
		m.ensureGroupTrackerForApp(queuePath, applicationID, user)
	}
	return userTracker.increaseTrackedResource(queuePath, applicationID, usage)
}

// DecreaseTrackedResource Decrease the resource usage for the given user group and queue path combination.
// As and when every allocation or asks release happens, corresponding user and group
// resource usage would be decreased against specific application. When the final asks release happens, removeApp should be set to true and
// application itself would be removed from the tracker and no more usage would be tracked further for that specific application.
func (m *Manager) DecreaseTrackedResource(queuePath, applicationID string, usage *resources.Resource, user security.UserGroup, removeApp bool) bool {
	log.Log(log.SchedUGM).Debug("Decreasing resource usage",
		zap.String("user", user.User),
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage),
		zap.Bool("removeApp", removeApp))
	if queuePath == common.Empty || applicationID == common.Empty || usage == nil || user.User == common.Empty {
		log.Log(log.SchedUGM).Debug("Mandatory parameters are missing to decrease the resource usage")
		return false
	}

	userTracker := m.GetUserTracker(user.User)
	if userTracker == nil {
		log.Log(log.SchedUGM).Error("user tracker must be available in userTrackers map",
			zap.String("user", user.User))
		return false
	}
	// get the group now as the decrease might remove the app from the user if removeApp is true
	appGroup := userTracker.getGroupForApp(applicationID)
	log.Log(log.SchedUGM).Debug("Decreasing resource usage for user",
		zap.String("user", user.User),
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.String("tracked group", appGroup),
		zap.Stringer("resource", usage),
		zap.Bool("removeApp", removeApp))
	removeQT, decreased := userTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
	if !decreased {
		return decreased
	}
	if removeQT {
		log.Log(log.SchedUGM).Debug("Removing user from manager",
			zap.String("user", user.User))
		delete(m.userTrackers, user.User)
	}
	// if the app did not have a group we're done otherwise update the groupTracker
	if appGroup == common.Empty {
		return decreased
	}
	groupTracker := m.GetGroupTracker(appGroup)
	if groupTracker == nil {
		log.Log(log.SchedUGM).Error("group tracker should be available in groupTrackers map",
			zap.String("applicationID", applicationID),
			zap.String("applicationID", appGroup))
		return decreased
	}
	log.Log(log.SchedUGM).Debug("Decreasing resource usage for group",
		zap.String("group", appGroup),
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage),
		zap.Bool("removeApp", removeApp))
	removeQT, decreased = groupTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
	if !decreased {
		return decreased
	}
	if removeQT {
		log.Log(log.SchedUGM).Debug("Removing group from manager",
			zap.String("group", appGroup),
			zap.String("queue path", queuePath),
			zap.String("application", applicationID),
			zap.Bool("removeApp", removeApp))
		delete(m.groupTrackers, appGroup)
	}
	return true
}

func (m *Manager) GetUserResources(user security.UserGroup) *resources.Resource {
	m.RLock()
	defer m.RUnlock()
	ut := m.userTrackers[user.User]
	if ut != nil && len(ut.GetUserResourceUsageDAOInfo().Queues.ResourceUsage.Resources) > 0 {
		return ut.GetUserResourceUsageDAOInfo().Queues.ResourceUsage
	}
	return nil
}

func (m *Manager) GetGroupResources(group string) *resources.Resource {
	m.RLock()
	defer m.RUnlock()
	gt := m.groupTrackers[group]
	if gt != nil && len(gt.GetGroupResourceUsageDAOInfo().Queues.ResourceUsage.Resources) > 0 {
		return gt.GetGroupResourceUsageDAOInfo().Queues.ResourceUsage
	}
	return nil
}

func (m *Manager) GetUsersResources() []*UserTracker {
	m.RLock()
	defer m.RUnlock()
	var userTrackers []*UserTracker
	for _, tracker := range m.userTrackers {
		userTrackers = append(userTrackers, tracker)
	}
	return userTrackers
}

// GetUserTracker returns the UserTracker object if defined for the user and a nil otherwise.
func (m *Manager) GetUserTracker(user string) *UserTracker {
	m.RLock()
	defer m.RUnlock()
	return m.userTrackers[user]
}

func (m *Manager) GetGroupsResources() []*GroupTracker {
	m.RLock()
	defer m.RUnlock()
	var groupTrackers []*GroupTracker
	for _, tracker := range m.groupTrackers {
		groupTrackers = append(groupTrackers, tracker)
	}
	return groupTrackers
}

// GetGroupTracker returns the GroupTracker object if defined for the group and a nil otherwise.
func (m *Manager) GetGroupTracker(group string) *GroupTracker {
	m.RLock()
	defer m.RUnlock()
	return m.groupTrackers[group]
}

// ensureGroupTrackerForApp creates a group tracker to user and application link.
// The userTracker MUST have been created and the application SHOULD not be tracked yet for the user.
func (m *Manager) ensureGroupTrackerForApp(queuePath, applicationID string, user security.UserGroup) {
	userTracker := m.GetUserTracker(user.User)
	// sanity check: caller should not have called this function if the application is already tracked
	if userTracker.hasGroupForApp(applicationID) {
		return
	}
	// check which group this matches
	appGroup := m.ensureGroup(user, queuePath)
	var groupTracker *GroupTracker

	// something matched, get the tracker or create if it does not exist
	if appGroup != common.Empty {
		groupTracker = m.GetGroupTracker(appGroup)
		if groupTracker == nil {
			log.Log(log.SchedUGM).Debug("Group tracker doesn't exists. Creating appGroup tracker",
				zap.String("queue path", queuePath),
				zap.String("appGroup", appGroup))
			groupTracker = newGroupTracker(appGroup)
			m.Lock()
			m.groupTrackers[appGroup] = groupTracker
			m.Unlock()
		}
	}
	log.Log(log.SchedUGM).Debug("Group tracker set for user application",
		zap.String("appGroup", appGroup),
		zap.String("user", user.User),
		zap.String("application", applicationID),
		zap.String("queue path", queuePath))
	// set this even if groupTracker is nil, as that was the final result of the resolution
	// a nil group tracker means we do not track
	userTracker.setGroupForApp(applicationID, groupTracker)
}

// ensureGroup returns the group to be used for tracking based on the user and queuePath
// A user can belong to zero or more groups.
// Limits are configured for different groups at different queue hierarchy.
// Among these multiple groups stored in security.UserGroup, matching against group for which limit has been configured happens from leaf to root and first
// matching group would be picked and used as user's group
func (m *Manager) ensureGroup(user security.UserGroup, queuePath string) string {
	// no groups nothing to do here
	if len(user.Groups) == 0 {
		return common.Empty
	}
	m.RLock()
	defer m.RUnlock()
	return m.ensureGroupInternal(user.Groups, queuePath)
}

// ensureGroupInternal checks the config for a matching group to track against.
// Matching starts at the leaf queue and works upwards towards the root.
// If nothing matches an empty string is returned.
func (m *Manager) ensureGroupInternal(userGroups []string, queuePath string) string {
	if configGroups, ok := m.configuredGroups[queuePath]; ok {
		for _, configGroup := range configGroups {
			for _, g := range userGroups {
				if configGroup == g {
					log.Log(log.SchedUGM).Debug("Found matching group for user",
						zap.String("queue path", queuePath),
						zap.String("matched group", configGroup))
					return configGroup
				}
			}
		}
	}
	// nothing matched check if we have the wildcard
	if m.groupWildCardLimitsConfig[queuePath] != nil {
		return common.Wildcard
	}
	// no match at this level check one level higher if it is there, otherwise no match
	parentPath := getParentPath(queuePath)
	if parentPath == common.Empty {
		return common.Empty
	}
	return m.ensureGroupInternal(userGroups, parentPath)
}

func (m *Manager) isUserRemovable(ut *UserTracker) bool {
	if len(ut.getTrackedApplications()) == 0 && resources.IsZero(ut.queueTracker.resourceUsage) {
		return true
	}
	return false
}

func (m *Manager) isGroupRemovable(gt *GroupTracker) bool {
	if len(gt.getTrackedApplications()) == 0 && resources.IsZero(gt.queueTracker.resourceUsage) {
		return true
	}
	return false
}

func (m *Manager) UpdateConfig(config configs.QueueConfig, queuePath string) error {
	m.Lock()
	defer m.Unlock()

	m.userWildCardLimitsConfig = make(map[string]*LimitConfig)
	m.groupWildCardLimitsConfig = make(map[string]*LimitConfig)
	m.configuredGroups = make(map[string][]string)

	// deep copy of the existing user and group limits
	earlierUserLimits := make(map[string]map[string]bool)
	earlierGroupLimits := make(map[string]map[string]bool)
	for k, v := range m.userLimits {
		if _, ok := earlierUserLimits[k]; !ok {
			earlierUserLimits[k] = make(map[string]bool)
		}
		earlierUserLimits[k] = v
	}
	for k, v := range m.groupLimits {
		if _, ok := earlierGroupLimits[k]; !ok {
			earlierGroupLimits[k] = make(map[string]bool)
		}
		earlierGroupLimits[k] = v
	}
	m.userLimits = make(map[string]map[string]bool)
	m.groupLimits = make(map[string]map[string]bool)
	return m.internalProcessConfig(config, queuePath, earlierUserLimits, earlierGroupLimits)
}

func (m *Manager) internalProcessConfig(cur configs.QueueConfig, queuePath string, earlierUserLimits map[string]map[string]bool, earlierGroupLimits map[string]map[string]bool) error {
	currentUserLimits := make(map[string]bool)
	currentGroupLimits := make(map[string]bool)
	// Traverse limits of specific queue path
	for _, limit := range cur.Limits {
		var maxResource *resources.Resource
		var err error
		if maxResource, err = resources.NewResourceFromConf(limit.MaxResources); err != nil {
			log.Log(log.SchedUGM).Warn("Problem in using the limit max resources settings.",
				zap.String("queue path", queuePath),
				zap.Any("limit max resources", limit.MaxResources),
				zap.Error(err))
			return fmt.Errorf("problem in using the max resources settings for queuepath: %s. reason: %w", queuePath, err)
		}
		limitConfig := &LimitConfig{maxResources: maxResource, maxApplications: limit.MaxApplications}
		for _, user := range limit.Users {
			if user == common.Empty {
				continue
			}
			log.Log(log.SchedUGM).Debug("Processing user limits configuration",
				zap.String("user", user),
				zap.String("limit", limit.Limit),
				zap.String("queue path", queuePath),
				zap.Uint64("max application", limit.MaxApplications),
				zap.Any("max resources", limit.MaxResources))
			if user == common.Wildcard {
				m.userWildCardLimitsConfig[queuePath] = limitConfig
				continue
			}
			if err := m.setUserLimits(user, limitConfig, queuePath); err != nil {
				return err
			}
			currentUserLimits[user] = true
			if _, ok := m.userLimits[queuePath]; !ok {
				m.userLimits[queuePath] = make(map[string]bool)
			}
			m.userLimits[queuePath][user] = true
		}
		for _, group := range limit.Groups {
			if group == common.Empty {
				continue
			}
			log.Log(log.SchedUGM).Debug("Processing group limits configuration",
				zap.String("group", group),
				zap.String("limit", limit.Limit),
				zap.String("queue path", queuePath),
				zap.Uint64("max application", limit.MaxApplications),
				zap.Any("max resources", limit.MaxResources))
			if err := m.setGroupLimits(group, limitConfig, queuePath); err != nil {
				return err
			}
			currentGroupLimits[group] = true
			if _, ok := m.groupLimits[queuePath]; !ok {
				m.groupLimits[queuePath] = make(map[string]bool)
			}
			m.groupLimits[queuePath][group] = true
			if group == common.Wildcard {
				m.groupWildCardLimitsConfig[queuePath] = limitConfig
			} else {
				m.configuredGroups[queuePath] = append(m.configuredGroups[queuePath], group)
			}
		}
	}
	if err := m.clearEarlierSetLimits(currentUserLimits, currentGroupLimits, earlierUserLimits, earlierGroupLimits, queuePath); err != nil {
		return err
	}

	if len(cur.Queues) > 0 {
		for _, child := range cur.Queues {
			childQueuePath := queuePath + configs.DOT + child.Name
			if err := m.internalProcessConfig(child, childQueuePath, earlierUserLimits, earlierGroupLimits); err != nil {
				return err
			}
		}
	}
	return nil
}

// clearEarlierSetLimits Clear already configured limits of users and groups for which limits have been configured before but not now
func (m *Manager) clearEarlierSetLimits(currentUserLimits map[string]bool, currentGroupLimits map[string]bool, earlierUserLimits map[string]map[string]bool,
	earlierGroupLimits map[string]map[string]bool, queuePath string) error {
	// Clear already configured limits of group for which limits have been configured before but not now
	for _, gt := range m.groupTrackers {
		appUsersMap := m.clearEarlierSetGroupLimits(gt, queuePath, currentGroupLimits, earlierGroupLimits)
		if len(appUsersMap) > 0 {
			for app, user := range appUsersMap {
				ut := m.userTrackers[user]
				ut.setGroupForApp(app, nil)
			}
		}
	}

	// Clear already configured limits of user for which limits have been configured before but not now
	for _, ut := range m.userTrackers {
		m.clearEarlierSetUserLimits(ut, queuePath, currentUserLimits, earlierUserLimits)
	}
	return nil
}

func (m *Manager) clearEarlierSetUserLimits(ut *UserTracker, queuePath string, currentUserLimits map[string]bool, earlierUserLimits map[string]map[string]bool) {
	u := ut.userName
	// Need to clear user config only when
	// 1. config set earlier but not now
	// 2. user already tracked for the queue path
	earlier := earlierUserLimits[queuePath][u]
	current := currentUserLimits[u]
	// Is this user already tracked for the queue path?
	if earlier && !current && ut.IsQueuePathTrackedCompletely(queuePath) {
		log.Log(log.SchedUGM).Debug("Need to clear earlier set configs for user",
			zap.String("user", u),
			zap.String("queue path", queuePath))
		// Is there any running applications in end queue of this queue path? If not, then remove the linkage between end queue and its immediate parent
		if ut.IsUnlinkRequired(queuePath) {
			ut.UnlinkQT(queuePath)
		} else {
			ut.setLimits(queuePath, resources.NewResource(), 0)
			log.Log(log.SchedUGM).Debug("Cleared earlier set limit configs for user",
				zap.String("user", u),
				zap.String("queue path", queuePath))
		}
		// Does "root" queue has any child queue trackers? At some point during this whole traversal, root might
		// not have any child queue trackers. When the situation comes, remove the linkage between the user and
		// its root queue tracker
		if ut.canBeRemoved() {
			delete(m.userTrackers, ut.userName)
		}
	}
}

func (m *Manager) clearEarlierSetGroupLimits(gt *GroupTracker, queuePath string, currentGroupLimits map[string]bool, earlierGroupLimits map[string]map[string]bool) map[string]string {
	appUsersMap := make(map[string]string)
	g := gt.groupName
	// Need to clear group config only when
	// 1. config set earlier but not now
	// 2. group already tracked for the queue path
	earlier := earlierGroupLimits[queuePath][g]
	current := currentGroupLimits[g]
	if earlier && !current && gt.IsQueuePathTrackedCompletely(queuePath) {
		log.Log(log.SchedUGM).Debug("Need to clear earlier set configs for group",
			zap.String("group", g),
			zap.String("queue path", queuePath))
		appUsersMap = gt.decreaseAllTrackedResourceUsage(queuePath)
		// Is there any running applications in end queue of this queue path? If not, then remove the linkage between end queue and its immediate parent
		if gt.IsUnlinkRequired(queuePath) {
			gt.UnlinkQT(queuePath)
		} else {
			gt.setLimits(queuePath, resources.NewResource(), 0)
			log.Log(log.SchedUGM).Debug("Cleared earlier set limit configs for group",
				zap.String("group", g),
				zap.String("queue path", queuePath))
		}
		// Does "root" queue has any child queue trackers? At some point during this whole traversal, root might
		// not have any child queue trackers. When the situation comes, remove the linkage between the group and
		// its root queue tracker
		if gt.canBeRemoved() {
			delete(m.groupTrackers, gt.groupName)
		}
	}
	return appUsersMap
}

func (m *Manager) setUserLimits(user string, limitConfig *LimitConfig, queuePath string) error {
	log.Log(log.SchedUGM).Debug("Setting user limits",
		zap.String("user", user),
		zap.String("queue path", queuePath),
		zap.Uint64("max application", limitConfig.maxApplications),
		zap.Any("max resources", limitConfig.maxResources))
	userTracker, ok := m.userTrackers[user]
	if !ok {
		log.Log(log.SchedUGM).Debug("User tracker does not exist. Creating user tracker object to set the limit configuration",
			zap.String("user", user),
			zap.String("queue path", queuePath))
		userTracker = newUserTracker(user)
		m.userTrackers[user] = userTracker
	}
	userTracker.setLimits(queuePath, limitConfig.maxResources, limitConfig.maxApplications)
	return nil
}

func (m *Manager) setGroupLimits(group string, limitConfig *LimitConfig, queuePath string) error {
	log.Log(log.SchedUGM).Debug("Setting group limits",
		zap.String("group", group),
		zap.String("queue path", queuePath),
		zap.Uint64("max application", limitConfig.maxApplications),
		zap.Any("max resources", limitConfig.maxResources))
	groupTracker, ok := m.groupTrackers[group]
	if !ok {
		log.Log(log.SchedUGM).Debug("Group tracker does not exist. Creating group tracker object to set the limit configuration",
			zap.String("group", group),
			zap.String("queue path", queuePath))
		groupTracker = newGroupTracker(group)
		m.groupTrackers[group] = groupTracker
	}
	groupTracker.setLimits(queuePath, limitConfig.maxResources, limitConfig.maxApplications)
	return nil
}

// getUserTracker returns the requested user tracker and creates one if it does not exist.
// This only happens if the user does not have any limits in the config.
// Wildcard limits should be applied for this user as part of the checks.
func (m *Manager) getUserTracker(user string) *UserTracker {
	m.Lock()
	defer m.Unlock()
	if ut, ok := m.userTrackers[user]; ok {
		return ut
	}
	log.Log(log.SchedUGM).Debug("User tracker doesn't exists. Creating user tracker.",
		zap.String("user", user))
	userTracker := newUserTracker(user)
	m.userTrackers[user] = userTracker
	return userTracker
}

func (m *Manager) getUserWildCardLimitsConfig(queuePath string) *LimitConfig {
	m.RLock()
	defer m.RUnlock()
	if config, ok := m.userWildCardLimitsConfig[queuePath]; ok {
		return config
	}
	return nil
}

func (m *Manager) getGroupWildCardLimitsConfig(queuePath string) *LimitConfig {
	m.RLock()
	defer m.RUnlock()
	if config, ok := m.groupWildCardLimitsConfig[queuePath]; ok {
		return config
	}
	return nil
}

// Headroom calculates the headroom for this specific application that runs as the user and group.
func (m *Manager) Headroom(queuePath, applicationID string, user security.UserGroup) *resources.Resource {
	userTracker := m.getUserTracker(user.User)
	userHeadroom := userTracker.headroom(queuePath)
	log.Log(log.SchedUGM).Debug("Calculated headroom for user",
		zap.String("user", user.User),
		zap.String("queue path", queuePath),
		zap.Stringer("user headroom", userHeadroom))
	// make sure the user has a groupTracker for this application, if not yet there add it
	if !userTracker.hasGroupForApp(applicationID) {
		m.ensureGroupTrackerForApp(queuePath, applicationID, user)
	}
	// check if this application now has group tracking, if not we're done
	appGroup := userTracker.getGroupForApp(applicationID)
	if appGroup == common.Empty {
		return userHeadroom
	}
	groupTracker := m.GetGroupTracker(appGroup)
	if groupTracker == nil {
		return userHeadroom
	}
	groupHeadroom := groupTracker.headroom(queuePath)
	log.Log(log.SchedUGM).Debug("Calculated headroom for group",
		zap.String("group", appGroup),
		zap.String("queue path", queuePath),
		zap.Stringer("group headroom", groupHeadroom))
	return resources.ComponentWiseMinPermissive(userHeadroom, groupHeadroom)
}

// CanRunApp checks the maxApplications for this specific application that runs as the user and group.
func (m *Manager) CanRunApp(queuePath, applicationID string, user security.UserGroup) bool {
	userTracker := m.getUserTracker(user.User)
	userCanRunApp := userTracker.canRunApp(queuePath, applicationID)
	log.Log(log.SchedUGM).Debug("Check whether user can run app",
		zap.String("user", user.User),
		zap.String("queue path", queuePath),
		zap.Bool("can run app", userCanRunApp))
	// make sure the user has a groupTracker for this application, if not yet there add it
	if !userTracker.hasGroupForApp(applicationID) {
		m.ensureGroupTrackerForApp(queuePath, applicationID, user)
	}
	// check if this application now has group tracking, if not we're done
	appGroup := userTracker.getGroupForApp(applicationID)
	if appGroup == common.Empty {
		return userCanRunApp
	}
	groupTracker := m.GetGroupTracker(appGroup)
	if groupTracker == nil {
		return userCanRunApp
	}
	groupCanRunApp := groupTracker.canRunApp(queuePath, applicationID)
	log.Log(log.SchedUGM).Debug("Check whether group can run app",
		zap.String("group", appGroup),
		zap.String("queue path", queuePath),
		zap.Bool("can run app", groupCanRunApp))
	return userCanRunApp && groupCanRunApp
}

// ClearUserTrackers only for tests
func (m *Manager) ClearUserTrackers() {
	m.Lock()
	defer m.Unlock()
	m.userTrackers = make(map[string]*UserTracker)
}

func (m *Manager) ClearGroupTrackers() {
	m.Lock()
	defer m.Unlock()
	m.groupTrackers = make(map[string]*GroupTracker)
}
