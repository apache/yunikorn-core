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
	userWildCardLimitsConfig  map[string]*LimitConfig // Hold limits settings of user '*'
	groupWildCardLimitsConfig map[string]*LimitConfig // Hold limits settings of group '*'
	configuredGroups          map[string][]string     // Hold groups for all configured queue paths.
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
		zap.String("group", user.Groups[0]),
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage))
	if queuePath == "" || applicationID == "" || usage == nil || user.User == "" {
		log.Log(log.SchedUGM).Debug("Mandatory parameters are missing to increase the resource usage",
			zap.String("user", user.User),
			zap.String("queue path", queuePath),
			zap.String("application", applicationID),
			zap.Stringer("resource", usage))
		return false
	}
	userTracker := m.getUserTracker(user.User, true)
	m.ensureGroupTrackerForApp(queuePath, applicationID, user)
	log.Log(log.SchedUGM).Debug("Increasing resource usage for user",
		zap.String("user", user.User),
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage))
	increased := userTracker.increaseTrackedResource(queuePath, applicationID, usage)
	if !increased {
		return increased
	}
	return true
}

// DecreaseTrackedResource Decrease the resource usage for the given user group and queue path combination.
// As and when every allocation or asks release happens, corresponding user and group
// resource usage would be decreased against specific application. When the final asks release happens, removeApp should be set to true and
// application itself would be removed from the tracker and no more usage would be tracked further for that specific application.
func (m *Manager) DecreaseTrackedResource(queuePath, applicationID string, usage *resources.Resource, user security.UserGroup, removeApp bool) bool {
	log.Log(log.SchedUGM).Debug("Decreasing resource usage", zap.String("user", user.User),
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage),
		zap.Bool("removeApp", removeApp))
	if queuePath == "" || applicationID == "" || usage == nil || user.User == "" {
		log.Log(log.SchedUGM).Debug("Mandatory parameters are missing to decrease the resource usage",
			zap.String("user", user.User),
			zap.String("queue path", queuePath),
			zap.String("application", applicationID),
			zap.Stringer("resource", usage),
			zap.Bool("removeApp", removeApp))
		return false
	}

	userTracker := m.getUserTracker(user.User, false)
	if userTracker == nil {
		log.Log(log.SchedUGM).Error("user tracker must be available in userTrackers map",
			zap.String("user", user.User))
		return false
	}
	group := userTracker.getGroupForApp(applicationID)
	groupTracker := m.getGroupTracker(group, false)

	log.Log(log.SchedUGM).Debug("Decreasing resource usage for user",
		zap.String("user", user.User),
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage),
		zap.Bool("removeApp", removeApp))
	removeQT, decreased := userTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
	if !decreased {
		return decreased
	}
	if removeApp && removeQT {
		log.Log(log.SchedUGM).Debug("Removing user from manager",
			zap.String("user", user.User),
			zap.String("queue path", queuePath),
			zap.String("application", applicationID),
			zap.Bool("removeApp", removeApp))
		delete(m.userTrackers, user.User)
	}

	if groupTracker != nil {
		log.Log(log.SchedUGM).Debug("Decreasing resource usage for group",
			zap.String("group", group),
			zap.String("queue path", queuePath),
			zap.String("application", applicationID),
			zap.Stringer("resource", usage),
			zap.Bool("removeApp", removeApp))
		removeQT, decreased = groupTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
		if !decreased {
			return decreased
		}
		if removeApp && removeQT {
			log.Log(log.SchedUGM).Debug("Removing group from manager",
				zap.String("group", group),
				zap.String("queue path", queuePath),
				zap.String("application", applicationID),
				zap.Bool("removeApp", removeApp))
			delete(m.groupTrackers, group)
		}
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

func (m *Manager) GetUserTracker(user string) *UserTracker {
	m.RLock()
	defer m.RUnlock()
	if m.userTrackers[user] != nil {
		return m.userTrackers[user]
	}
	return nil
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

func (m *Manager) GetGroupTracker(group string) *GroupTracker {
	m.RLock()
	defer m.RUnlock()
	if m.groupTrackers[group] != nil {
		return m.groupTrackers[group]
	}
	return nil
}

func (m *Manager) ensureGroupTrackerForApp(queuePath string, applicationID string, user security.UserGroup) string {
	m.Lock()
	defer m.Unlock()
	userTracker := m.userTrackers[user.User]
	if !userTracker.hasGroupForApp(applicationID) {
		var groupTracker *GroupTracker
		group := m.internalEnsureGroup(user, queuePath)
		if group != "" {
			if m.groupTrackers[group] == nil {
				log.Log(log.SchedUGM).Debug("Group tracker doesn't exists. Creating group tracker",
					zap.String("application", applicationID),
					zap.String("queue path", queuePath),
					zap.String("user", user.User),
					zap.String("group", group))
				groupTracker = newGroupTracker(group)
				m.groupTrackers[group] = groupTracker
			} else {
				log.Log(log.SchedUGM).Debug("Group tracker already exists and linking (reusing) the same with application",
					zap.String("application", applicationID),
					zap.String("queue path", queuePath),
					zap.String("user", user.User),
					zap.String("group", group))
				groupTracker = m.groupTrackers[group]
			}
		}
		userTracker.setGroupForApp(applicationID, groupTracker)
		return group
	} else {
		return userTracker.getGroupForApp(applicationID)
	}
}

// ensureGroup User may belong to zero or multiple groups. Limits are configured for different groups at different queue hierarchy.
// Among these multiple groups stored in security.UserGroup, matching against group for which limit has been configured happens from leaf to root and first
// matching group would be picked and used as user's group for all activities in UGM module.
func (m *Manager) ensureGroup(user security.UserGroup, queuePath string) string {
	m.RLock()
	defer m.RUnlock()
	return m.internalEnsureGroup(user, queuePath)
}

// lock free version
func (m *Manager) internalEnsureGroup(user security.UserGroup, queuePath string) string {
	userTracker := m.userTrackers[user.User]
	if userTracker != nil {
		if configGroups, ok := m.configuredGroups[queuePath]; ok {
			for _, configGroup := range configGroups {
				for _, g := range user.Groups {
					if configGroup == g {
						log.Log(log.SchedUGM).Debug("Found matching group for user",
							zap.String("user", user.User),
							zap.String("queue path", queuePath),
							zap.String("matched group", configGroup))
						return configGroup
					}
				}
			}
		}
		parentQueuePath, _ := getParentQueuePath(queuePath)
		if parentQueuePath != "" {
			qt := userTracker.queueTracker.getChildQueueTracker(parentQueuePath)
			return m.internalEnsureGroup(user, qt.queuePath)
		}
	}
	return ""
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
	return m.internalProcessConfig(config, queuePath)
}

func (m *Manager) internalProcessConfig(cur configs.QueueConfig, queuePath string) error {
	// Holds user and group for which limits have been configured with specific queue path
	userLimits := make(map[string]bool)
	groupLimits := make(map[string]bool)
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
			if user == "" {
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
			if err := m.processUserConfig(user, limitConfig, queuePath, userLimits); err != nil {
				return err
			}
		}
		for _, group := range limit.Groups {
			if group == "" {
				continue
			}
			log.Log(log.SchedUGM).Debug("Processing group limits configuration",
				zap.String("group", group),
				zap.String("limit", limit.Limit),
				zap.String("queue path", queuePath),
				zap.Uint64("max application", limit.MaxApplications),
				zap.Any("max resources", limit.MaxResources))
			if group == common.Wildcard {
				m.groupWildCardLimitsConfig[queuePath] = limitConfig
				continue
			}
			if err := m.processGroupConfig(group, limitConfig, queuePath, groupLimits); err != nil {
				return err
			}
			m.configuredGroups[queuePath] = append(m.configuredGroups[queuePath], group)
		}
	}
	if err := m.clearEarlierSetLimits(userLimits, groupLimits, queuePath); err != nil {
		return err
	}

	if len(cur.Queues) > 0 {
		for _, child := range cur.Queues {
			childQueuePath := queuePath + configs.DOT + child.Name
			if err := m.internalProcessConfig(child, childQueuePath); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Manager) processUserConfig(user string, limitConfig *LimitConfig, queuePath string, userLimits map[string]bool) error {
	if err := m.setUserLimits(user, limitConfig, queuePath); err != nil {
		return err
	}
	userLimits[user] = true
	return nil
}

func (m *Manager) processGroupConfig(group string, limitConfig *LimitConfig, queuePath string, groupLimits map[string]bool) error {
	if err := m.setGroupLimits(group, limitConfig, queuePath); err != nil {
		return err
	}
	groupLimits[group] = true
	return nil
}

// clearEarlierSetLimits Clear already configured limits of users and groups for which limits have been configured before but not now
func (m *Manager) clearEarlierSetLimits(userLimits map[string]bool, groupLimits map[string]bool, queuePath string) error {
	// Clear already configured limits of user for which limits have been configured before but not now
	for u, ut := range m.userTrackers {
		// Is this user already tracked for the queue path?
		if ut.IsQueuePathTrackedCompletely(queuePath) {
			// Traverse all the group trackers linked to user through different applications and remove the linkage
			for appID, gt := range ut.appGroupTrackers {
				if gt != nil {
					g := gt.groupName
					// Is there any limit config set for group in the current configuration? If not, then remove the linkage by setting it to nil
					if ok := groupLimits[g]; !ok {
						log.Log(log.SchedUGM).Debug("Removed the linkage between user and group through applications",
							zap.String("user", u),
							zap.String("group", gt.groupName),
							zap.String("application id", appID),
							zap.String("queue path", queuePath))
						// removing the linkage only happens here by setting it to nil and deleting app id
						// but group resource usage so far remains as it is because we don't have app id wise resource usage with in group as of now.
						// YUNIKORN-1858 handles the group resource usage properly
						// In case of only one (last) application, group tracker would be removed from the manager.
						ut.setGroupForApp(appID, nil)
						gt.removeApp(appID)
						if len(gt.getTrackedApplications()) == 0 {
							log.Log(log.SchedUGM).Debug("Is this app the only running application in group?",
								zap.String("user", u),
								zap.String("group", gt.groupName),
								zap.Int("no. of applications", len(gt.getTrackedApplications())),
								zap.String("application id", appID),
								zap.String("queue path", queuePath))
							delete(m.groupTrackers, g)
						}
					}
				}
			}
		}
		m.clearEarlierSetUserLimits(ut, queuePath, userLimits)
	}

	// Clear already configured limits of group for which limits have been configured before but not now
	for _, gt := range m.groupTrackers {
		m.clearEarlierSetGroupLimits(gt, queuePath, groupLimits)
	}
	return nil
}

func (m *Manager) clearEarlierSetUserLimits(ut *UserTracker, queuePath string, userLimits map[string]bool) {
	// Is this user already tracked for the queue path?
	if ut.IsQueuePathTrackedCompletely(queuePath) {
		u := ut.userName
		// Is there any limit config set for user in the current configuration? If not, then clear those old limit settings
		if _, ok := userLimits[u]; !ok {
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
}

func (m *Manager) clearEarlierSetGroupLimits(gt *GroupTracker, queuePath string, groupLimits map[string]bool) {
	// Is this group already tracked for the queue path?
	if gt.IsQueuePathTrackedCompletely(queuePath) {
		g := gt.groupName
		// Is there any limit config set for group in the current configuration? If not, then clear those old limit settings
		if ok := groupLimits[g]; !ok {
			log.Log(log.SchedUGM).Debug("Need to clear earlier set configs for group",
				zap.String("group", g),
				zap.String("queue path", queuePath))
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
	}
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

func (m *Manager) getUserTracker(user string, createIfNotPresent bool) *UserTracker {
	m.Lock()
	defer m.Unlock()
	if ut, ok := m.userTrackers[user]; ok {
		return ut
	}
	if createIfNotPresent {
		log.Log(log.SchedUGM).Debug("User tracker doesn't exists. Creating user tracker.",
			zap.String("user", user))
		userTracker := newUserTracker(user)
		m.userTrackers[user] = userTracker
		return userTracker
	}
	return nil
}

func (m *Manager) getGroupTracker(group string, createIfNotPresent bool) *GroupTracker {
	m.Lock()
	defer m.Unlock()
	if gt, ok := m.groupTrackers[group]; ok {
		return gt
	}
	if createIfNotPresent {
		log.Log(log.SchedUGM).Debug("Group tracker doesn't exists. Creating group tracker.",
			zap.String("group", group))
		groupTracker := newGroupTracker(group)
		m.groupTrackers[group] = groupTracker
		return groupTracker
	}
	return nil
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

func (m *Manager) Headroom(queuePath string, user security.UserGroup) *resources.Resource {
	m.RLock()
	defer m.RUnlock()
	var userHeadroom *resources.Resource
	var groupHeadroom *resources.Resource
	if m.userTrackers[user.User] != nil {
		userHeadroom = m.userTrackers[user.User].headroom(queuePath)
		log.Log(log.SchedUGM).Debug("Calculated headroom for user",
			zap.String("user", user.User),
			zap.String("queue path", queuePath),
			zap.String("user headroom", userHeadroom.String()))
	}
	group := m.internalEnsureGroup(user, queuePath)
	if group != "" {
		if m.groupTrackers[group] != nil {
			groupHeadroom = m.groupTrackers[group].headroom(queuePath)
			log.Log(log.SchedUGM).Debug("Calculated headroom for group",
				zap.String("group", group),
				zap.String("queue path", queuePath),
				zap.String("group headroom", groupHeadroom.String()))
		}
	}
	return resources.ComponentWiseMinPermissive(userHeadroom, groupHeadroom)
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
