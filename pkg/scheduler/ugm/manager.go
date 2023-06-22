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

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/log"
)

var once sync.Once
var m *Manager

const maxresources = "maxresources"
const maxapplications = "maxapplications"

// Manager implements tracker. A User Group Manager to track the usage for both user and groups.
// Holds object of both user and group trackers
type Manager struct {
	userTrackers      map[string]*UserTracker
	groupTrackers     map[string]*GroupTracker
	userLimitsConfig  map[string]map[string]map[string]interface{} // Hold limits settings of user * queue path
	groupLimitsConfig map[string]map[string]map[string]interface{} // Hold limits settings of group * queue path
	sync.RWMutex
}

func newManager() *Manager {
	manager := &Manager{
		userTrackers:      make(map[string]*UserTracker),
		groupTrackers:     make(map[string]*GroupTracker),
		userLimitsConfig:  make(map[string]map[string]map[string]interface{}),
		groupLimitsConfig: make(map[string]map[string]map[string]interface{}),
	}
	return manager
}

func GetUserManager() *Manager {
	once.Do(func() {
		m = newManager()
	})
	return m
}

// IncreaseTrackedResource Increase the resource usage for the given user group and queue path combination.
// As and when every allocation or asks requests fulfilled on application, corresponding user and group
// resource usage would be increased against specific application.
func (m *Manager) IncreaseTrackedResource(queuePath string, applicationID string, usage *resources.Resource, user security.UserGroup) error {
	log.Log(log.SchedUGM).Debug("Increasing resource usage", zap.String("user", user.User),
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage))
	if queuePath == "" || applicationID == "" || usage == nil || user.User == "" {
		log.Log(log.SchedUGM).Error("Mandatory parameters are missing to increase the resource usage",
			zap.String("user", user.User),
			zap.String("queue path", queuePath),
			zap.String("application", applicationID),
			zap.Stringer("resource", usage))
		return fmt.Errorf("mandatory parameters are missing. queuepath: %s, application id: %s, resource usage: %s, user: %s",
			queuePath, applicationID, usage.String(), user.User)
	}
	m.Lock()
	defer m.Unlock()
	var userTracker *UserTracker
	if m.userTrackers[user.User] == nil {
		userTracker = newUserTracker(user.User)

		// Set the limits for all configured queue paths of the user
		for configQueuePath, config := range m.userLimitsConfig[user.User] {
			log.Log(log.SchedUGM).Debug("Setting the limit max applications settings.",
				zap.String("user", user.User),
				zap.String("queue path", configQueuePath))
			maxApps, ok := config[maxapplications].(uint64)
			if !ok {
				log.Log(log.SchedUGM).Warn("Problem in setting the limit max applications settings. Unable to cast the value from interface to uint64",
					zap.String("user", user.User),
					zap.String("queue path", configQueuePath),
					zap.Uint64("limit max applications", maxApps))
				return fmt.Errorf("unable to set the max applications. user: %s, queuepath : %s, applicationid: %s",
					user.User, configQueuePath, applicationID)
			}
			err := userTracker.setMaxApplications(maxApps, configQueuePath)
			if err != nil {
				log.Log(log.SchedUGM).Warn("Problem in setting the limit max applications settings.",
					zap.String("user", user.User),
					zap.String("queue path", configQueuePath),
					zap.Uint64("limit max applications", maxApps),
					zap.Error(err))
				return fmt.Errorf("unable to set the max applications. user: %s, queuepath : %s, applicationid: %s, usage: %s, reason: %w",
					user.User, configQueuePath, applicationID, usage.String(), err)
			}
			maxResources, ok := config[maxresources].(map[string]string)
			if !ok {
				log.Log(log.SchedUGM).Warn("Problem in setting the limit max resources settings. Unable to cast the value from interface to resource",
					zap.String("user", user.User),
					zap.String("queue path", configQueuePath),
					zap.Any("limit max resources", maxResources))
				return fmt.Errorf("unable to set the max resources. user: %s, queuepath : %s, applicationid: %s",
					user.User, configQueuePath, applicationID)
			}
			resource, resourceErr := resources.NewResourceFromConf(maxResources)
			if resourceErr != nil {
				log.Log(log.SchedUGM).Warn("Problem in setting the limit max resources settings.",
					zap.String("user", user.User),
					zap.String("queue path", configQueuePath),
					zap.Any("limit max resources", maxResources),
					zap.Error(resourceErr))
				return fmt.Errorf("unable to set the max resources. user: %s, queuepath : %s, applicationid: %s, usage: %s, reason: %w",
					user.User, configQueuePath, applicationID, usage.String(), resourceErr)
			}
			setMaxResourcesErr := userTracker.setMaxResources(resource, configQueuePath)
			if setMaxResourcesErr != nil {
				log.Log(log.SchedUGM).Warn("Problem in setting the limit max resources settings.",
					zap.String("user", user.User),
					zap.String("queue path", configQueuePath),
					zap.Any("limit max resources", maxResources),
					zap.Error(setMaxResourcesErr))
				return fmt.Errorf("unable to set the max resources. user: %s, queuepath : %s, applicationid: %s, usage: %s, reason: %w",
					user.User, configQueuePath, applicationID, usage.String(), setMaxResourcesErr)
			}
		}
		m.userTrackers[user.User] = userTracker
	} else {
		userTracker = m.userTrackers[user.User]
	}
	err := userTracker.increaseTrackedResource(queuePath, applicationID, usage)
	if err != nil {
		log.Log(log.SchedUGM).Error("Problem in increasing the user resource usage",
			zap.String("user", user.User),
			zap.String("queue path", queuePath),
			zap.String("application", applicationID),
			zap.Stringer("resource", usage),
			zap.String("err message", err.Error()))
		return err
	}
	if err = m.ensureGroupTrackerForApp(queuePath, applicationID, user); err != nil {
		return err
	}
	group, err := m.getGroup(user)
	if err != nil {
		return err
	}
	groupTracker := m.groupTrackers[group]
	if groupTracker != nil {
		err = groupTracker.increaseTrackedResource(queuePath, applicationID, usage)
		if err != nil {
			log.Log(log.SchedUGM).Error("Problem in increasing the group resource usage",
				zap.String("user", user.User),
				zap.String("group", group),
				zap.String("queue path", queuePath),
				zap.String("application", applicationID),
				zap.Stringer("resource", usage),
				zap.String("err message", err.Error()))
			return err
		}
	}
	return nil
}

// DecreaseTrackedResource Decrease the resource usage for the given user group and queue path combination.
// As and when every allocation or asks release happens, corresponding user and group
// resource usage would be decreased against specific application. When the final asks release happens, removeApp should be set to true and
// application itself would be removed from the tracker and no more usage would be tracked further for that specific application.
func (m *Manager) DecreaseTrackedResource(queuePath string, applicationID string, usage *resources.Resource, user security.UserGroup, removeApp bool) error {
	log.Log(log.SchedUGM).Debug("Decreasing resource usage", zap.String("user", user.User),
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage),
		zap.Bool("removeApp", removeApp))
	if queuePath == "" || applicationID == "" || usage == nil || user.User == "" {
		log.Log(log.SchedUGM).Error("Mandatory parameters are missing to decrease the resource usage",
			zap.String("user", user.User),
			zap.String("queue path", queuePath),
			zap.String("application", applicationID),
			zap.Stringer("resource", usage),
			zap.Bool("removeApp", removeApp))
		return fmt.Errorf("mandatory parameters are missing. queuepath: %s, application id: %s, resource usage: %s, user: %s",
			queuePath, applicationID, usage.String(), user.User)
	}
	m.Lock()
	defer m.Unlock()
	userTracker := m.userTrackers[user.User]
	if userTracker != nil {
		removeQT, err := userTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
		if err != nil {
			log.Log(log.SchedUGM).Error("Problem in decreasing the user resource usage",
				zap.String("user", user.User),
				zap.String("queue path", queuePath),
				zap.String("application", applicationID),
				zap.Stringer("resource", usage),
				zap.Bool("removeApp", removeApp),
				zap.String("err message", err.Error()))
			return err
		}
		if removeApp && removeQT {
			delete(m.userTrackers, user.User)
		}
	} else {
		log.Log(log.SchedUGM).Error("user tracker must be available in userTrackers map",
			zap.String("user", user.User))
		return fmt.Errorf("user tracker for %s is missing in userTrackers map", user.User)
	}

	group, err := m.getGroup(user)
	if err != nil {
		return err
	}
	groupTracker := m.groupTrackers[group]
	if groupTracker != nil {
		removeQT, err := groupTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
		if err != nil {
			log.Log(log.SchedUGM).Error("Problem in decreasing the group resource usage",
				zap.String("user", user.User),
				zap.String("group", group),
				zap.String("queue path", queuePath),
				zap.String("application", applicationID),
				zap.Stringer("resource", usage),
				zap.Bool("removeApp", removeApp),
				zap.String("err message", err.Error()))
			return err
		}
		if removeApp && removeQT {
			delete(m.groupTrackers, group)
		}
	} else {
		log.Log(log.SchedUGM).Error("appGroupTrackers tracker must be available in groupTrackers map",
			zap.String("appGroupTrackers", group))
		return fmt.Errorf("appGroupTrackers tracker for %s is missing in groupTrackers map", group)
	}
	return nil
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

func (m *Manager) ensureGroupTrackerForApp(queuePath string, applicationID string, user security.UserGroup) error {
	userTracker := m.userTrackers[user.User]
	if !userTracker.hasGroupForApp(applicationID) {
		var groupTracker *GroupTracker
		group, err := m.getGroup(user)
		if err != nil {
			return err
		}
		if m.groupTrackers[group] == nil {
			log.Log(log.SchedUGM).Debug("Group tracker does not exist. Creating group tracker object and linking the same with application",
				zap.String("application", applicationID),
				zap.String("queue path", queuePath),
				zap.String("user", user.User),
				zap.String("group", group))
			groupTracker = newGroupTracker(group)

			// Set the limits for all configured queue paths of the group
			for configQueuePath, config := range m.groupLimitsConfig[group] {
				log.Log(log.SchedUGM).Debug("Setting the limit max applications settings.",
					zap.String("group", group),
					zap.String("queue path", configQueuePath))
				maxApps, ok := config[maxapplications].(uint64)
				if !ok {
					log.Log(log.SchedUGM).Warn("Problem in setting the limit max applications settings. Unable to cast the value from interface to uint64",
						zap.String("group", group),
						zap.String("queue path", configQueuePath),
						zap.Uint64("limit max applications", maxApps))
					return fmt.Errorf("unable to set the max applications. group: %s, queuepath : %s, applicationid: %s",
						group, configQueuePath, applicationID)
				}
				if setMaxApplicationsErr := groupTracker.setMaxApplications(maxApps, configQueuePath); setMaxApplicationsErr != nil {
					log.Log(log.SchedUGM).Warn("Problem in setting the limit max applications settings.",
						zap.String("group", group),
						zap.String("queue path", configQueuePath),
						zap.Uint64("limit max applications", maxApps),
						zap.Error(setMaxApplicationsErr))
					return fmt.Errorf("unable to set the max applications. group: %s, queuepath : %s, applicationid: %s, reason: %w",
						group, configQueuePath, applicationID, setMaxApplicationsErr)
				}

				maxResources, ok := config[maxresources].(map[string]string)
				if !ok {
					log.Log(log.SchedUGM).Warn("Problem in setting the limit max resources settings. Unable to cast the value from interface to resource",
						zap.String("group", group),
						zap.String("queue path", configQueuePath),
						zap.Any("limit max resources", maxResources))
					return fmt.Errorf("unable to set the max resources. group: %s, queuepath : %s, applicationid: %s",
						group, configQueuePath, applicationID)
				}
				resource, resourceErr := resources.NewResourceFromConf(maxResources)
				if resourceErr != nil {
					log.Log(log.SchedUGM).Warn("Problem in setting the limit max resources settings.",
						zap.String("group", group),
						zap.String("queue path", configQueuePath),
						zap.Any("limit max resources", maxResources),
						zap.Error(resourceErr))
					return fmt.Errorf("unable to set the max resources. group: %s, queuepath : %s, applicationid: %s, reason: %w",
						group, configQueuePath, applicationID, resourceErr)
				}
				if setMaxResourcesErr := groupTracker.setMaxResources(resource, configQueuePath); setMaxResourcesErr != nil {
					log.Log(log.SchedUGM).Warn("Problem in setting the limit max resources settings.",
						zap.String("group", group),
						zap.String("queue path", configQueuePath),
						zap.Any("limit max resources", maxResources),
						zap.Error(setMaxResourcesErr))
					return fmt.Errorf("unable to set the max resources. group: %s, queuepath : %s, applicationid: %s, reason: %w",
						group, configQueuePath, applicationID, setMaxResourcesErr)
				}
			}
			m.groupTrackers[group] = groupTracker
		} else {
			log.Log(log.SchedUGM).Debug("Group tracker already exists and linking (reusing) the same with application",
				zap.String("application", applicationID),
				zap.String("queue path", queuePath),
				zap.String("user", user.User),
				zap.String("group", group))
			groupTracker = m.groupTrackers[group]
		}
		userTracker.setGroupForApp(applicationID, groupTracker)
	}
	return nil
}

// getGroup Based on the current limitations, username and group name is same. Groups[0] is always set and same as username.
// It would be changed in future based on user group resolution, limit configuration processing etc
func (m *Manager) getGroup(user security.UserGroup) (string, error) {
	if len(user.Groups) > 0 {
		return user.Groups[0], nil
	}
	return "", fmt.Errorf("group is not available in usergroup for user %s", user.User)
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

	// clear the local limit config maps before processing the limit config
	m.userLimitsConfig = make(map[string]map[string]map[string]interface{})
	m.groupLimitsConfig = make(map[string]map[string]map[string]interface{})
	return m.internalProcessConfig(config, queuePath)
}

func (m *Manager) internalProcessConfig(cur configs.QueueConfig, queuePath string) error {
	// Holds user and group for which limits have been configured with specific queue path
	userGroupLimits := make(map[string]bool)
	// Traverse limits of specific queue path
	for _, limit := range cur.Limits {
		tempLimitsMap := make(map[string]interface{})
		tempLimitsMap[maxresources] = limit.MaxResources
		tempLimitsMap[maxapplications] = limit.MaxApplications
		for _, user := range limit.Users {
			log.Log(log.SchedUGM).Debug("Processing user limits configuration",
				zap.String("user", user),
				zap.String("limit", limit.Limit),
				zap.String("queue path", queuePath),
				zap.Uint64("max application", limit.MaxApplications),
				zap.Any("max resources", limit.MaxResources))
			tempUserMap := make(map[string]map[string]interface{})
			tempUserMap[queuePath] = tempLimitsMap
			if err := m.processUserConfig(user, limit, queuePath, userGroupLimits, tempLimitsMap, tempUserMap); err != nil {
				return err
			}
		}
		for _, group := range limit.Groups {
			log.Log(log.SchedUGM).Debug("Processing group limits configuration",
				zap.String("group", group),
				zap.String("limit", limit.Limit),
				zap.String("queue path", queuePath),
				zap.Uint64("max application", limit.MaxApplications),
				zap.Any("max resources", limit.MaxResources))
			tempGroupMap := make(map[string]map[string]interface{})
			tempGroupMap[queuePath] = tempLimitsMap
			if err := m.processGroupConfig(group, limit, queuePath, userGroupLimits, tempLimitsMap, tempGroupMap); err != nil {
				return err
			}
		}
	}
	if err := m.clearEarlierSetLimits(userGroupLimits, queuePath); err != nil {
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

func (m *Manager) processUserConfig(user string, limit configs.Limit, queuePath string, userGroupLimits map[string]bool, tempLimitsMap map[string]interface{}, tempUserMap map[string]map[string]interface{}) error {
	if user == "*" {
		// traverse all tracked users
		for u, ut := range m.userTrackers {
			// Is this user already tracked for the queue path?
			if m.IsQueuePathTrackedCompletely(ut.queueTracker, queuePath) {
				log.Log(log.SchedUGM).Debug("Processing wild card user limits configuration for all existing users",
					zap.String("user", u),
					zap.String("limit", limit.Limit),
					zap.String("queue path", queuePath),
					zap.Uint64("max application", limit.MaxApplications),
					zap.Any("max resources", limit.MaxResources))

				// creates an entry for the user being processed always as it has been cleaned before
				if _, ok := m.userLimitsConfig[u]; ok {
					m.userLimitsConfig[u][queuePath] = tempLimitsMap
				} else {
					m.userLimitsConfig[u] = tempUserMap
				}
				if err := m.setUserLimits(u, limit, queuePath); err != nil {
					return err
				}
				userGroupLimits[u] = true
			}
		}
	} else {
		// creates an entry for the user being processed always as it has been cleaned before
		if _, ok := m.userLimitsConfig[user]; ok {
			m.userLimitsConfig[user][queuePath] = tempLimitsMap
		} else {
			m.userLimitsConfig[user] = tempUserMap
		}
		if err := m.setUserLimits(user, limit, queuePath); err != nil {
			return err
		}
		userGroupLimits[user] = true
	}
	return nil
}

func (m *Manager) processGroupConfig(group string, limit configs.Limit, queuePath string, userGroupLimits map[string]bool, tempLimitsMap map[string]interface{}, tempGroupMap map[string]map[string]interface{}) error {
	if group == "*" {
		// traverse all tracked groups
		for g, gt := range m.groupTrackers {
			// Is this group already tracked for the queue path?
			if m.IsQueuePathTrackedCompletely(gt.queueTracker, queuePath) {
				log.Log(log.SchedUGM).Debug("Processing wild card user limits configuration for all existing groups",
					zap.String("group", g),
					zap.String("limit", limit.Limit),
					zap.String("queue path", queuePath),
					zap.Uint64("max application", limit.MaxApplications),
					zap.Any("max resources", limit.MaxResources))
				// creates an entry for the group being processed always as it has been cleaned before
				if _, ok := m.groupLimitsConfig[g]; ok {
					m.groupLimitsConfig[g][queuePath] = tempLimitsMap
				} else {
					m.groupLimitsConfig[g] = tempGroupMap
				}
				if err := m.setGroupLimits(g, limit, queuePath); err != nil {
					return err
				}
				userGroupLimits[g] = true
			}
		}
	} else {
		// creates an entry for the group being processed always as it has been cleaned before
		if _, ok := m.groupLimitsConfig[group]; ok {
			m.groupLimitsConfig[group][queuePath] = tempLimitsMap
		} else {
			m.groupLimitsConfig[group] = tempGroupMap
		}
		if err := m.setGroupLimits(group, limit, queuePath); err != nil {
			return err
		}
		userGroupLimits[group] = true
	}
	return nil
}

func (m *Manager) clearEarlierSetLimits(userGroupLimits map[string]bool, queuePath string) error {
	// Clear already configured limits of user for which limits have been configured before but not now through #cur
	for u, ut := range m.userTrackers {
		// Is this user already tracked for the queue path?
		if m.IsQueuePathTrackedCompletely(ut.queueTracker, queuePath) {
			if _, ok := userGroupLimits[u]; !ok {
				err := ut.setMaxResources(resources.NewResource(), queuePath)
				if err != nil {
					return err
				}
				err = ut.setMaxApplications(0, queuePath)
				if err != nil {
					return err
				}
			}
		}
	}

	// Clear already configured limits of group for which limits have been configured before but not now through #cur
	for g, gt := range m.groupTrackers {
		// Is this group already tracked for the queue path?
		if m.IsQueuePathTrackedCompletely(gt.queueTracker, queuePath) {
			if _, ok := userGroupLimits[g]; !ok {
				if err := gt.setMaxResources(resources.NewResource(), queuePath); err != nil {
					return err
				}
				if err := gt.setMaxApplications(0, queuePath); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (m *Manager) setUserLimits(user string, limit configs.Limit, queuePath string) error {
	log.Log(log.SchedUGM).Debug("Setting user limits",
		zap.String("user", user),
		zap.String("limit", limit.Limit),
		zap.String("queue path", queuePath),
		zap.Uint64("max application", limit.MaxApplications),
		zap.Any("max resources", limit.MaxResources))
	userTracker, ok := m.userTrackers[user]
	if !ok {
		log.Log(log.SchedUGM).Debug("User tracker does not exist. Creating user tracker object to set the limit configuration",
			zap.String("user", user),
			zap.String("queue path", queuePath))
		userTracker = newUserTracker(user)
		m.userTrackers[user] = userTracker
	}
	if err := userTracker.setMaxApplications(limit.MaxApplications, queuePath); err != nil {
		log.Log(log.SchedUGM).Warn("Problem in setting the limit max applications settings.",
			zap.String("user", user),
			zap.String("queue path", queuePath),
			zap.Uint64("limit max applications", limit.MaxApplications),
			zap.Error(err))
		return fmt.Errorf("unable to set the limit for user %s because %w", user, err)
	}

	if resource, err := resources.NewResourceFromConf(limit.MaxResources); err == nil {
		if err = userTracker.setMaxResources(resource, queuePath); err != nil {
			log.Log(log.SchedUGM).Warn("Problem in setting the limit max resources settings.",
				zap.String("user", user),
				zap.String("queue path", queuePath),
				zap.Any("limit max resources", limit.MaxResources),
				zap.Error(err))
			return fmt.Errorf("unable to set the limit for user %s because %w", user, err)
		}
	} else {
		log.Log(log.SchedUGM).Warn("Problem in using the limit max resources settings.",
			zap.String("user", user),
			zap.String("queue path", queuePath),
			zap.Any("limit max resources", limit.MaxResources),
			zap.Error(err))
		return fmt.Errorf("unable to set the limit for user %s because %w", user, err)
	}
	return nil
}

func (m *Manager) setGroupLimits(group string, limit configs.Limit, queuePath string) error {
	log.Log(log.SchedUGM).Debug("Setting group limits",
		zap.String("group", group),
		zap.String("limit", limit.Limit),
		zap.String("queue path", queuePath),
		zap.Uint64("max application", limit.MaxApplications),
		zap.Any("max resources", limit.MaxResources))
	groupTracker, ok := m.groupTrackers[group]
	if !ok {
		log.Log(log.SchedUGM).Debug("Group tracker does not exist. Creating group tracker object to set the limit configuration",
			zap.String("group", group),
			zap.String("queue path", queuePath))
		groupTracker = newGroupTracker(group)
		m.groupTrackers[group] = groupTracker
	}
	if err := groupTracker.setMaxApplications(limit.MaxApplications, queuePath); err != nil {
		log.Log(log.SchedUGM).Warn("Problem in setting the limit max applications settings.",
			zap.String("group", group),
			zap.String("queue path", queuePath),
			zap.Uint64("limit max applications", limit.MaxApplications),
			zap.Error(err))
		return fmt.Errorf("unable to set the limit for group %s because %w", group, err)
	}

	if resource, err := resources.NewResourceFromConf(limit.MaxResources); err == nil {
		if err = groupTracker.setMaxResources(resource, queuePath); err != nil {
			log.Log(log.SchedUGM).Warn("Problem in setting the limit max resources settings.",
				zap.String("group", group),
				zap.String("queue path", queuePath),
				zap.Any("limit max resources", limit.MaxResources),
				zap.Error(err))
			return fmt.Errorf("unable to set the limit for group %s because %w", group, err)
		}
	} else {
		log.Log(log.SchedUGM).Warn("Problem in using the limit max resources settings.",
			zap.String("group", group),
			zap.String("queue path", queuePath),
			zap.Any("limit max resources", limit.MaxResources),
			zap.Error(err))
		return fmt.Errorf("unable to set the limit for group %s because %w", group, err)
	}
	return nil
}

func (m *Manager) IsQueuePathTrackedCompletely(qt *QueueTracker, queuePath string) bool {
	if queuePath == configs.RootQueue || queuePath == qt.queueName {
		return true
	}
	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
	if immediateChildQueueName != "" {
		if childUt, ok := qt.childQueueTrackers[immediateChildQueueName]; ok {
			return m.IsQueuePathTrackedCompletely(childUt, childQueuePath)
		}
	}
	return false
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
