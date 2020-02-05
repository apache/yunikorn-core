/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package security

import (
	"fmt"
	"os/user"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

const (
	negcache        = 30  // time to cache failures for lookups in seconds
	poscache        = 300 // time to cache a positive lookup in seconds
	cleanerInterval = 60  // default cleaner interval
)

// global variables
var now time.Time            // One clock to access
var instance *UserGroupCache // The instance of the cache
var once sync.Once           // Make sure we can only create the cache once

// Cache for the user entries.
type UserGroupCache struct {
	lock     sync.RWMutex
	interval time.Duration
	ugs      map[string]*UserGroup
	// methods that allow mocking of the class or extending to use non OS solutions
	lookup        func(userName string) (*user.User, error)
	lookupGroupID func(gid string) (*user.Group, error)
	groupIds      func(osUser *user.User) ([]string, error)
}

// The structure of the entry in the cache.
type UserGroup struct {
	User     string
	Groups   []string
	failed   bool
	resolved int64
}

// Get the resolver for the user and group info.
// Current setup allows three resolvers:
// * NO resolver: default, no user or group resolution just return the info (k8s use case)
// * OS resolver: uses the OS libraries to resolve user and group memberships
// * Test resolver: fake resolution for testing
// TODO need to make this fully configurable and look at reflection etc
func GetUserGroupCache(resolver string) *UserGroupCache {
	once.Do(func() {
		switch resolver {
		case "test":
			log.Logger().Info("creating test user group resolver")
			instance = GetUserGroupCacheTest()
		case "os":
			log.Logger().Info("creating OS user group resolver")
			instance = GetUserGroupCacheOS()
		default:
			log.Logger().Info("creating UserGroupCache without resolver")
			instance = GetUserGroupNoResolve()
		}
		instance.ugs = make(map[string]*UserGroup)
		log.Logger().Info("starting UserGroupCache cleaner",
			zap.String("cleanerInterval", instance.interval.String()))
		go instance.run()
	})
	return instance
}

// Run the cleanup in a separate routine
func (c *UserGroupCache) run() {
	for {
		time.Sleep(instance.interval)
		runStart := time.Now()
		c.cleanUpCache()
		log.Logger().Debug("time consumed cleaning the UserGroupCache",
			zap.String("duration", time.Since(runStart).String()))
	}
}

// Do the real work for the cache cleanup
func (c *UserGroupCache) cleanUpCache() {
	oldest := now.Unix() - poscache
	oldestFailed := now.Unix() - negcache
	// clean up the cache so we do not grow out of bounds
	instance.lock.Lock()
	defer instance.lock.Unlock()
	// walk over the entries in the map and delete the expired ones, cleanup based on the resolved time.
	// Negative cached entries will expire quicker
	for key, val := range c.ugs {
		if val.resolved < oldest || (val.failed && val.resolved < oldestFailed) {
			delete(c.ugs, key)
		}
	}
}

// reset the cached content, test use only
func (c *UserGroupCache) resetCache() {
	log.Logger().Debug("UserGroupCache reset")
	instance.lock.Lock()
	defer instance.lock.Unlock()
	c.ugs = make(map[string]*UserGroup)
}

func (c *UserGroupCache) ConvertUGI(ugi *si.UserGroupInformation) (UserGroup, error) {
	// check if we have a user to convert
	if ugi == nil || ugi.User == "" {
		return UserGroup{}, fmt.Errorf("empty user cannot resolve")
	}
	// try to resolve the user if group info is empty otherwise we just convert
	if len(ugi.Groups) == 0 {
		return c.GetUserGroup(ugi.User)
	}
	// If groups are already present we should just convert
	newUG := UserGroup{User: ugi.User}
	newUG.Groups = append(newUG.Groups, ugi.Groups...)
	newUG.resolved = now.Unix()
	c.lock.Lock()
	defer c.lock.Unlock()
	c.ugs[ugi.User] = &newUG
	return newUG, nil
}

// Get the user group information. An error will still return a UserGroup.
// The Failed flag in the object will be set to true for any failures.
// The information is cached, negatively and positively.
func (c *UserGroupCache) GetUserGroup(userName string) (UserGroup, error) {
	// check if we have a user to resolve
	if userName == "" {
		return UserGroup{}, fmt.Errorf("empty user cannot resolve")
	}
	// look in the cache before resolving
	c.lock.RLock()
	ug, ok := c.ugs[userName]
	c.lock.RUnlock()
	// return if this was not a negative cache that has not timed out
	if ok && !ug.failed {
		return *ug, nil
	}
	// nothing returned so create a new one
	if ug == nil {
		ug = &UserGroup{
			User: userName,
		}
	}
	// if we failed before we could get an object back, return the existing one with an error
	if ug.failed {
		return *ug, fmt.Errorf("user resolution failed, cached data returned: %v", time.Unix(ug.resolved, 0))
	}
	// resolve if we do not have it in the cache
	// find the user first, then resolve the groups
	osUser, err := c.lookup(userName)
	if err != nil {
		log.Logger().Error("Error resolving user: does not exist",
			zap.String("userName", userName),
			zap.Error(err))
		ug.failed = true
	}
	// skip group resolution if we did not find the user
	if osUser != nil {
		// groups are updated in the object directly
		err = ug.resolveGroups(osUser, c)
		// log a failure and continue
		if err != nil {
			log.Logger().Error("Error resolving groups for user",
				zap.String("userName", userName),
				zap.Error(err))
			ug.failed = true
		}
	}
	// all resolved (or not) but use this time stamp
	ug.resolved = now.Unix()

	// add it to the cache, even if we fail negative cache is also good to know
	c.lock.Lock()
	defer c.lock.Unlock()
	c.ugs[userName] = ug
	return *ug, err
}

// Resolve the groups for the user if the user exists
func (ug *UserGroup) resolveGroups(osUser *user.User, c *UserGroupCache) error {
	// resolve the primary group and add it first
	groupName, err := c.lookupGroupID(osUser.Gid)
	if err != nil {
		ug.Groups = append(ug.Groups, osUser.Gid)
	} else {
		ug.Groups = append(ug.Groups, groupName.Name)
	}
	var gids []string
	// resolve the group IDs for the user
	gids, err = c.groupIds(osUser)
	if err != nil {
		return err
	}
	// we have a list hide the failure to resolve some of the groups and just add them as IDs
	for _, gid := range gids {
		// skip the primary group if it is in the list
		if gid == osUser.Gid {
			continue
		}
		groupName, err = c.lookupGroupID(gid)
		if err != nil {
			ug.Groups = append(ug.Groups, gid)
		} else {
			ug.Groups = append(ug.Groups, groupName.Name)
		}
	}
	return nil
}
