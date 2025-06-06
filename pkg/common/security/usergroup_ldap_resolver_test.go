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

package security

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/go-ldap/ldap/v3"

	"github.com/apache/yunikorn-core/pkg/common"
)

// GetUserGroupCacheLdapMock returns a UserGroupCache with mocked LDAP functions for testing
func GetUserGroupCacheLdapMock() *UserGroupCache {
	return &UserGroupCache{
		ugs:           map[string]*UserGroup{},
		interval:      time.Second,
		lookup:        mockLdapLookupUser,
		lookupGroupID: mockLdapLookupGroupID,
		groupIds:      mockLDAPLookupGroupIds,
		stop:          make(chan struct{}),
	}
}

// mockLdapLookupUser is a mock implementation of LdapLookupUser for testing
func mockLdapLookupUser(userName string) (*user.User, error) {
	// Similar to the test resolver, but with LDAP-specific behavior
	if userName == Testuser1 || userName == Testuser {
		return &user.User{
			Uid:      "1000",
			Gid:      "1000",
			Username: userName,
		}, nil
	}
	if userName == Testuser2 {
		return &user.User{
			Uid:      "100",
			Gid:      "100",
			Username: "testuser2",
		}, nil
	}
	if userName == Testuser3 {
		return &user.User{
			Uid:      "1001",
			Gid:      "1001",
			Username: "testuser3",
		}, nil
	}
	if userName == Testuser4 {
		return &user.User{
			Uid:      "901",
			Gid:      "901",
			Username: "testuser4",
		}, nil
	}
	if userName == Testuser5 {
		return &user.User{
			Uid:      "1001",
			Gid:      "1001",
			Username: "testuser5",
		}, nil
	}
	if userName == "invalid-gid-user" {
		return &user.User{
			Uid:      "1001",
			Gid:      "1_001",
			Username: "invalid-gid-user",
		}, nil
	}
	// All other users fail
	return nil, fmt.Errorf("lookup failed for user: %s", userName)
}

// mockLdapLookupGroupID is a mock implementation of LdapLookupGroupID for testing
func mockLdapLookupGroupID(gid string) (*user.Group, error) {
	// For testing, we'll use a simple pattern
	if gid == "100" {
		return nil, fmt.Errorf("lookup failed for group: %s", gid)
	}
	// Special case for invalid-gid-user
	if gid == "1_001" {
		return nil, fmt.Errorf("lookup failed for group: %s", gid)
	}
	group := user.Group{Gid: gid}
	group.Name = "group" + gid
	return &group, nil
}

// mockLDAPLookupGroupIds is a mock implementation of LDAPLookupGroupIds for testing
func mockLDAPLookupGroupIds(osUser *user.User) ([]string, error) {
	if osUser.Username == Testuser1 || osUser.Username == Testuser {
		return []string{"1001"}, nil
	}
	if osUser.Username == Testuser2 {
		return []string{"1001", "1002"}, nil
	}
	// Group list might return primary group ID also
	if osUser.Username == Testuser3 {
		return []string{"1002", "1001", "1003", "1004"}, nil
	}
	if osUser.Username == Testuser4 {
		return []string{"901", "902"}, nil
	}
	return nil, fmt.Errorf("lookup failed for user: %s", osUser.Username)
}

// Mock LDAP search result for testing
func mockLdapSearchResult(username string) (*ldap.SearchResult, error) {
	if username == Testuser1 || username == Testuser {
		return &ldap.SearchResult{
			Entries: []*ldap.Entry{
				{
					Attributes: []*ldap.EntryAttribute{
						{
							Name:   "memberOf",
							Values: []string{"CN=group1001,OU=groups,DC=example,DC=com"},
						},
					},
				},
			},
		}, nil
	}
	if username == Testuser2 {
		return &ldap.SearchResult{
			Entries: []*ldap.Entry{
				{
					Attributes: []*ldap.EntryAttribute{
						{
							Name:   "memberOf",
							Values: []string{"CN=group1001,OU=groups,DC=example,DC=com", "CN=group1002,OU=groups,DC=example,DC=com"},
						},
					},
				},
			},
		}, nil
	}
	if username == Testuser3 {
		return &ldap.SearchResult{
			Entries: []*ldap.Entry{
				{
					Attributes: []*ldap.EntryAttribute{
						{
							Name:   "memberOf",
							Values: []string{"CN=group1002,OU=groups,DC=example,DC=com", "CN=group1001,OU=groups,DC=example,DC=com", "CN=group1003,OU=groups,DC=example,DC=com", "CN=group1004,OU=groups,DC=example,DC=com"},
						},
					},
				},
			},
		}, nil
	}
	if username == Testuser4 {
		return &ldap.SearchResult{
			Entries: []*ldap.Entry{
				{
					Attributes: []*ldap.EntryAttribute{
						{
							Name:   "memberOf",
							Values: []string{"CN=group901,OU=groups,DC=example,DC=com", "CN=group902,OU=groups,DC=example,DC=com"},
						},
					},
				},
			},
		}, nil
	}
	return nil, fmt.Errorf("ldap lookup failed for user: %s", username)
}

// LdapAccessMock implements the LdapAccess interface for testing
type LdapAccessMock struct {
	DialURLFunc  func(url string, options ...ldap.DialOpt) (*ldap.Conn, error)
	BindFunc     func(conn *ldap.Conn, username, password string) error
	SearchFunc   func(conn *ldap.Conn, searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error)
	CloseFunc    func(conn *ldap.Conn)
	SearchResult *ldap.SearchResult
	Error        error
}

func (m *LdapAccessMock) DialURL(url string, options ...ldap.DialOpt) (*ldap.Conn, error) {
	if m.DialURLFunc != nil {
		return m.DialURLFunc(url, options...)
	}
	return &ldap.Conn{}, nil
}

func (m *LdapAccessMock) Bind(conn *ldap.Conn, username, password string) error {
	if m.BindFunc != nil {
		return m.BindFunc(conn, username, password)
	}
	return nil
}

func (m *LdapAccessMock) Search(conn *ldap.Conn, searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error) {
	if m.SearchFunc != nil {
		return m.SearchFunc(conn, searchRequest)
	}
	return m.SearchResult, m.Error
}

func (m *LdapAccessMock) Close(conn *ldap.Conn) {
	if m.CloseFunc != nil {
		m.CloseFunc(conn)
	}
}

// Helper function to create a mock LDAP access with predefined search results
func newMockLdapAccess(searchResult *ldap.SearchResult, err error) *LdapAccessMock {
	return &LdapAccessMock{
		SearchResult: searchResult,
		Error:        err,
	}
}

// TestLdapSearch tests the new LdapSearch function with a mock LdapAccess
func TestLdapSearch(t *testing.T) {
	// Create a mock search result
	mockResult := &ldap.SearchResult{
		Entries: []*ldap.Entry{
			{
				Attributes: []*ldap.EntryAttribute{
					{
						Name:   "memberOf",
						Values: []string{"CN=group1,OU=groups,DC=example,DC=com", "CN=group2,OU=groups,DC=example,DC=com"},
					},
				},
			},
		},
	}

	// Create a mock LDAP access with the mock result
	mockAccess := newMockLdapAccess(mockResult, nil)

	// Call LdapSearch with the mock access
	result, err := LdapSearch(mockAccess, "testuser")

	// Verify results
	assert.NilError(t, err)
	assert.Assert(t, result != nil)
	assert.Equal(t, 1, len(result.Entries))
	assert.Equal(t, 1, len(result.Entries[0].Attributes))
	assert.Equal(t, "memberOf", result.Entries[0].Attributes[0].Name)
	assert.Equal(t, 2, len(result.Entries[0].Attributes[0].Values))
	assert.Equal(t, "CN=group1,OU=groups,DC=example,DC=com", result.Entries[0].Attributes[0].Values[0])
	assert.Equal(t, "CN=group2,OU=groups,DC=example,DC=com", result.Entries[0].Attributes[0].Values[1])
}

// TestLdapSearchError tests the error handling in LdapSearch
func TestLdapSearchError(t *testing.T) {
	// Test cases for different error scenarios
	testCases := []struct {
		name        string
		dialError   error
		bindError   error
		searchError error
	}{
		{
			name:        "Dial Error",
			dialError:   errors.New("dial error"),
			bindError:   nil,
			searchError: nil,
		},
		{
			name:        "Bind Error",
			dialError:   nil,
			bindError:   errors.New("bind error"),
			searchError: nil,
		},
		{
			name:        "Search Error",
			dialError:   nil,
			bindError:   nil,
			searchError: errors.New("search error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a mock LDAP access with the appropriate error
			mockAccess := &LdapAccessMock{
				DialURLFunc: func(url string, options ...ldap.DialOpt) (*ldap.Conn, error) {
					return &ldap.Conn{}, tc.dialError
				},
				BindFunc: func(conn *ldap.Conn, username, password string) error {
					return tc.bindError
				},
				SearchFunc: func(conn *ldap.Conn, searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error) {
					return nil, tc.searchError
				},
			}

			// Call LdapSearch with the mock access
			result, err := LdapSearch(mockAccess, "testuser")

			// Verify error
			assert.Assert(t, err != nil)
			assert.Assert(t, result == nil)

			// Check for specific error
			switch {
			case tc.dialError != nil:
				assert.Equal(t, tc.dialError.Error(), err.Error())
			case tc.bindError != nil:
				assert.Equal(t, tc.bindError.Error(), err.Error())
			case tc.searchError != nil:
				assert.Equal(t, tc.searchError.Error(), err.Error())
			}
		})
	}
}

func TestLdapLookups(t *testing.T) {
	tests := []struct {
		name     string
		testType string
		id       string
		validate func(t *testing.T, result interface{}, err error)
	}{
		{
			name:     "Lookup user",
			testType: "user",
			id:       "testuser",
			validate: func(t *testing.T, result interface{}, err error) {
				assert.NilError(t, err)
				u, ok := result.(*user.User)
				assert.Assert(t, ok, "invalid result type")
				assert.Equal(t, "testuser", u.Username)
				assert.Equal(t, "testuser", u.Gid)
				assert.Equal(t, "1211", u.Uid)
			},
		},
		{
			name:     "Lookup group",
			testType: "group",
			id:       "testgroup",
			validate: func(t *testing.T, result interface{}, err error) {
				assert.NilError(t, err)
				g, ok := result.(*user.Group)
				assert.Assert(t, ok, "invalid result type")
				assert.Equal(t, "testgroup", g.Gid)
				assert.Equal(t, "testgroup", g.Name)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.testType == "user" {
				u, err := LdapLookupUser(tt.id)
				tt.validate(t, u, err)
			} else if tt.testType == "group" {
				g, err := LdapLookupGroupID(tt.id)
				tt.validate(t, g, err)
			}
		})
	}
}

func TestLDAPLookupGroupIds(t *testing.T) {
	// Save the original newLdapAccessImpl function and ensure it's restored
	defer resetLdapAccessFactory()

	// Create a mock search result
	mockResult := &ldap.SearchResult{
		Entries: []*ldap.Entry{
			{
				Attributes: []*ldap.EntryAttribute{
					{
						Name:   "memberOf",
						Values: []string{"CN=group1,OU=groups,DC=example,DC=com", "CN=group2,OU=groups,DC=example,DC=com"},
					},
				},
			},
		},
	}

	// Mock the newLdapAccessImpl function to return our mock
	mockFactory := func(config *LdapResolverConfig) LdapAccess {
		return newMockLdapAccess(mockResult, nil)
	}

	// Replace the factory function
	newLdapAccessImpl = mockFactory

	u := &user.User{Username: "testuser"}
	groups, err := LDAPLookupGroupIds(u)
	assert.NilError(t, err)
	assert.Assert(t, strings.Contains(strings.Join(groups, ","), "group1"))
	assert.Assert(t, strings.Contains(strings.Join(groups, ","), "group2"))
}

func TestLDAPLookupGroupIdsError(t *testing.T) {
	// Ensure we restore the original factory at the end of the test
	defer resetLdapAccessFactory()

	// Mock the newLdapAccessImpl function to return our mock with an error
	mockFactory := func(config *LdapResolverConfig) LdapAccess {
		return newMockLdapAccess(nil, errors.New("ldap error"))
	}

	// Replace the factory function
	newLdapAccessImpl = mockFactory

	u := &user.User{Username: "testuser"}
	groups, err := LDAPLookupGroupIds(u)
	assert.Error(t, err, "ldap error")
	assert.Assert(t, groups == nil)
}

// Helper to reset ldapConf to defaults before each test
func resetLdapConfDefaults() {
	ldapConf = LdapResolverConfig{
		Host:         common.DefaultLdapHost,
		Port:         common.DefaultLdapPort,
		BaseDN:       common.DefaultLdapBaseDN,
		Filter:       common.DefaultLdapFilter,
		GroupAttr:    common.DefaultLdapGroupAttr,
		ReturnAttr:   common.DefaultLdapReturnAttr,
		BindUser:     common.DefaultLdapBindUser,
		BindPassword: common.DefaultLdapBindPassword,
		Insecure:     common.DefaultLdapInsecure,
		SSL:          common.DefaultLdapSSL,
	}
}

//nolint:funlen // Table-driven test for coverage, helpers used to reduce length
func TestReadSecrets(t *testing.T) {
	tests := getReadSecretsTestCases()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetLdapConfDefaults()
			_, cleanup := tt.setupFunc(t)
			defer cleanup()
			result := readSecrets()
			assert.Equal(t, tt.expectedResult, result)
			tt.validateFunc(t)
		})
	}
}

//nolint:funlen // Table-driven test helper for coverage, intentionally long
func getReadSecretsTestCases() []struct {
	name           string
	setupFunc      func(t *testing.T) (string, func())
	expectedResult bool
	validateFunc   func(t *testing.T)
} {
	return []struct {
		name           string
		setupFunc      func(t *testing.T) (string, func())
		expectedResult bool
		validateFunc   func(t *testing.T)
	}{
		{
			name: "Skips K8s metadata and directories",
			setupFunc: func(t *testing.T) (string, func()) {
				tmpDir := t.TempDir()
				err := os.Mkdir(filepath.Join(tmpDir, "..data"), 0755)
				assert.NilError(t, err)
				err = os.Mkdir(filepath.Join(tmpDir, "dir1"), 0755)
				assert.NilError(t, err)
				err = os.WriteFile(filepath.Join(tmpDir, "key1"), []byte("value1"), 0600)
				assert.NilError(t, err)
				err = os.WriteFile(filepath.Join(tmpDir, "..timestamp"), []byte("meta"), 0600)
				assert.NilError(t, err)
				origLdapMountPath := common.LdapMountPath
				common.LdapMountPath = tmpDir
				return tmpDir, func() { common.LdapMountPath = origLdapMountPath }
			},
			expectedResult: false,
			validateFunc: func(t *testing.T) {
				assert.Equal(t, common.DefaultLdapHost, ldapConf.Host)
				assert.Equal(t, common.DefaultLdapPort, ldapConf.Port)
				assert.Equal(t, common.DefaultLdapBaseDN, ldapConf.BaseDN)
				assert.Equal(t, common.DefaultLdapFilter, ldapConf.Filter)
				assert.Equal(t, common.DefaultLdapGroupAttr, ldapConf.GroupAttr)
				assert.Equal(t, strings.Join(common.DefaultLdapReturnAttr, ","), strings.Join(ldapConf.ReturnAttr, ","))
				assert.Equal(t, common.DefaultLdapBindUser, ldapConf.BindUser)
				assert.Equal(t, common.DefaultLdapBindPassword, ldapConf.BindPassword)
				assert.Equal(t, common.DefaultLdapInsecure, ldapConf.Insecure)
				assert.Equal(t, common.DefaultLdapSSL, ldapConf.SSL)
			},
		},
		{
			name: "Handles missing secrets directory",
			setupFunc: func(t *testing.T) (string, func()) {
				origLdapMountPath := common.LdapMountPath
				common.LdapMountPath = "/nonexistent"
				return "/nonexistent", func() { common.LdapMountPath = origLdapMountPath }
			},
			expectedResult: false,
			validateFunc: func(t *testing.T) {
				assert.Equal(t, common.DefaultLdapHost, ldapConf.Host)
				assert.Equal(t, common.DefaultLdapPort, ldapConf.Port)
				assert.Equal(t, common.DefaultLdapBaseDN, ldapConf.BaseDN)
				assert.Equal(t, common.DefaultLdapFilter, ldapConf.Filter)
				assert.Equal(t, common.DefaultLdapGroupAttr, ldapConf.GroupAttr)
				assert.Equal(t, strings.Join(common.DefaultLdapReturnAttr, ","), strings.Join(ldapConf.ReturnAttr, ","))
				assert.Equal(t, common.DefaultLdapBindUser, ldapConf.BindUser)
				assert.Equal(t, common.DefaultLdapBindPassword, ldapConf.BindPassword)
				assert.Equal(t, common.DefaultLdapInsecure, ldapConf.Insecure)
				assert.Equal(t, common.DefaultLdapSSL, ldapConf.SSL)
			},
		},
		{
			name: "Handles unknown key",
			setupFunc: func(t *testing.T) (string, func()) {
				tmpDir := t.TempDir()
				err := os.WriteFile(filepath.Join(tmpDir, "unknownKey"), []byte("somevalue"), 0600)
				assert.NilError(t, err)
				origLdapMountPath := common.LdapMountPath
				common.LdapMountPath = tmpDir
				return tmpDir, func() { common.LdapMountPath = origLdapMountPath }
			},
			expectedResult: false,
			validateFunc: func(t *testing.T) {
				assert.Equal(t, common.DefaultLdapHost, ldapConf.Host)
				assert.Equal(t, common.DefaultLdapPort, ldapConf.Port)
				assert.Equal(t, common.DefaultLdapBaseDN, ldapConf.BaseDN)
				assert.Equal(t, common.DefaultLdapFilter, ldapConf.Filter)
				assert.Equal(t, common.DefaultLdapGroupAttr, ldapConf.GroupAttr)
				assert.Equal(t, strings.Join(common.DefaultLdapReturnAttr, ","), strings.Join(ldapConf.ReturnAttr, ","))
				assert.Equal(t, common.DefaultLdapBindUser, ldapConf.BindUser)
				assert.Equal(t, common.DefaultLdapBindPassword, ldapConf.BindPassword)
				assert.Equal(t, common.DefaultLdapInsecure, ldapConf.Insecure)
				assert.Equal(t, common.DefaultLdapSSL, ldapConf.SSL)
			},
		},
		{
			name: "Handles invalid port and bool values",
			setupFunc: func(t *testing.T) (string, func()) {
				tmpDir := t.TempDir()
				err := os.WriteFile(filepath.Join(tmpDir, common.LdapPort), []byte("notanint"), 0600)
				assert.NilError(t, err)
				err = os.WriteFile(filepath.Join(tmpDir, common.LdapInsecure), []byte("notabool"), 0600)
				assert.NilError(t, err)
				err = os.WriteFile(filepath.Join(tmpDir, common.LdapSSL), []byte("notabool"), 0600)
				assert.NilError(t, err)
				origLdapMountPath := common.LdapMountPath
				common.LdapMountPath = tmpDir
				return tmpDir, func() { common.LdapMountPath = origLdapMountPath }
			},
			expectedResult: false,
			validateFunc: func(t *testing.T) {
				// Assert that ldapConf.Port is set to DefaultLdapPort when invalid int value is provided
				assert.Equal(t, common.DefaultLdapPort, ldapConf.Port)

				// Assert that rest of ldap conf is set to default values
				assert.Equal(t, common.DefaultLdapHost, ldapConf.Host)
				assert.Equal(t, common.DefaultLdapBaseDN, ldapConf.BaseDN)
				assert.Equal(t, common.DefaultLdapFilter, ldapConf.Filter)
				assert.Equal(t, common.DefaultLdapGroupAttr, ldapConf.GroupAttr)
				assert.Equal(t, strings.Join(common.DefaultLdapReturnAttr, ","), strings.Join(ldapConf.ReturnAttr, ","))
				assert.Equal(t, common.DefaultLdapBindUser, ldapConf.BindUser)
				assert.Equal(t, common.DefaultLdapBindPassword, ldapConf.BindPassword)
				assert.Equal(t, common.DefaultLdapInsecure, ldapConf.Insecure)
				assert.Equal(t, common.DefaultLdapSSL, ldapConf.SSL)
			},
		},
		{
			name: "Sets custom values",
			setupFunc: func(t *testing.T) (string, func()) {
				tmpDir := t.TempDir()
				err := os.WriteFile(filepath.Join(tmpDir, common.LdapHost), []byte("myhost"), 0600)
				assert.NilError(t, err)
				err = os.WriteFile(filepath.Join(tmpDir, common.LdapPort), []byte("1234"), 0600)
				assert.NilError(t, err)
				err = os.WriteFile(filepath.Join(tmpDir, common.LdapBaseDN), []byte("dc=test,dc=com"), 0600)
				assert.NilError(t, err)
				err = os.WriteFile(filepath.Join(tmpDir, common.LdapFilter), []byte("(&(uid=%s))"), 0600)
				assert.NilError(t, err)
				err = os.WriteFile(filepath.Join(tmpDir, common.LdapGroupAttr), []byte("groups"), 0600)
				assert.NilError(t, err)
				err = os.WriteFile(filepath.Join(tmpDir, common.LdapReturnAttr), []byte("memberOf,groups"), 0600)
				assert.NilError(t, err)
				err = os.WriteFile(filepath.Join(tmpDir, common.LdapBindUser), []byte("binduser"), 0600)
				assert.NilError(t, err)
				err = os.WriteFile(filepath.Join(tmpDir, common.LdapBindPassword), []byte("bindpass"), 0600)
				assert.NilError(t, err)
				err = os.WriteFile(filepath.Join(tmpDir, common.LdapInsecure), []byte("true"), 0600)
				assert.NilError(t, err)
				err = os.WriteFile(filepath.Join(tmpDir, common.LdapSSL), []byte("true"), 0600)
				assert.NilError(t, err)
				origLdapMountPath := common.LdapMountPath
				common.LdapMountPath = tmpDir
				return tmpDir, func() { common.LdapMountPath = origLdapMountPath }
			},
			expectedResult: true,
			validateFunc: func(t *testing.T) {
				assert.Equal(t, "myhost", ldapConf.Host)

				// Use strconv to verify the port value to ensure the import is used
				portStr := "1234"
				expectedPort, err := strconv.Atoi(portStr)
				assert.NilError(t, err, "failed to convert port string to int")
				assert.Equal(t, expectedPort, ldapConf.Port)

				assert.Equal(t, "dc=test,dc=com", ldapConf.BaseDN)
				assert.Equal(t, "(&(uid=%s))", ldapConf.Filter)
				assert.Equal(t, "groups", ldapConf.GroupAttr)
				assert.Equal(t, "memberOf,groups", strings.Join(ldapConf.ReturnAttr, ","))
				assert.Equal(t, "binduser", ldapConf.BindUser)
				assert.Equal(t, "bindpass", ldapConf.BindPassword)

				// Use strconv to verify boolean values
				insecureStr := "true"
				expectedInsecure, err := strconv.ParseBool(insecureStr)
				assert.NilError(t, err, "failed to convert insecure string to bool")
				assert.Equal(t, expectedInsecure, ldapConf.Insecure)

				sslStr := "true"
				expectedSSL, err := strconv.ParseBool(sslStr)
				assert.NilError(t, err, "failed to convert ssl string to bool")
				assert.Equal(t, expectedSSL, ldapConf.SSL)
			},
		},
		{
			name: "Missing required fields",
			setupFunc: func(t *testing.T) (string, func()) {
				tmpDir := t.TempDir()
				err := os.WriteFile(filepath.Join(tmpDir, common.LdapHost), []byte("ldap.example.com"), 0600)
				assert.NilError(t, err)
				err = os.WriteFile(filepath.Join(tmpDir, common.LdapPort), []byte("389"), 0600)
				assert.NilError(t, err)
				// Missing BaseDN, Filter, GroupAttr, ReturnAttr, BindUser, BindPassword
				origLdapMountPath := common.LdapMountPath
				common.LdapMountPath = tmpDir
				return tmpDir, func() { common.LdapMountPath = origLdapMountPath }
			},
			expectedResult: false,
			validateFunc: func(t *testing.T) {
				// No specific validation needed - we're testing the return value
			},
		},
		{
			name: "All required fields present",
			setupFunc: func(t *testing.T) (string, func()) {
				tmpDir := t.TempDir()
				requiredFields := map[string]string{
					common.LdapHost:         "ldap.example.com",
					common.LdapPort:         "389",
					common.LdapBaseDN:       "dc=example,dc=com",
					common.LdapFilter:       "(&(objectClass=user)(sAMAccountName=%s))",
					common.LdapGroupAttr:    "memberOf",
					common.LdapReturnAttr:   "memberOf",
					common.LdapBindUser:     "cn=admin,dc=example,dc=com",
					common.LdapBindPassword: "password",
				}

				for key, value := range requiredFields {
					err := os.WriteFile(filepath.Join(tmpDir, key), []byte(value), 0600)
					if err != nil {
						t.Fatalf("failed to write file %s: %v", key, err)
					}
				}

				origLdapMountPath := common.LdapMountPath
				common.LdapMountPath = tmpDir
				return tmpDir, func() { common.LdapMountPath = origLdapMountPath }
			},
			expectedResult: true,
			validateFunc: func(t *testing.T) {
				// No specific validation needed - we're testing the return value
			},
		},
	}
}

func TestUserGroupCacheLdap(t *testing.T) {
	tests := []struct {
		name         string
		validateFunc func(t *testing.T, cache *UserGroupCache)
	}{
		{
			name: "Cache initialization",
			validateFunc: func(t *testing.T, cache *UserGroupCache) {
				assert.Assert(t, cache != nil)
				assert.Assert(t, cache.ugs != nil)
				assert.Assert(t, cache.lookup != nil)
				assert.Assert(t, cache.lookupGroupID != nil)
				assert.Assert(t, cache.groupIds != nil)
			},
		},
		{
			name: "Cache interval",
			validateFunc: func(t *testing.T, cache *UserGroupCache) {
				interval := cache.interval
				expectedInterval := cleanerInterval * time.Second // 60 seconds
				assert.Equal(t, expectedInterval, interval, "LDAP resolver interval should be 60 seconds")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original function and restore after test
			origReadSecrets := readSecrets

			// Mock readSecrets to return true (successful configuration)
			readSecrets = func() bool {
				return true
			}
			defer func() { readSecrets = origReadSecrets }()

			// Get the LDAP user group cache
			cache := GetUserGroupCacheLdap()

			// Run the validation function
			tt.validateFunc(t, cache)
		})
	}
}

func TestMockLdapSearchResult(t *testing.T) {
	// Test valid users
	testCases := []struct {
		username      string
		expectedCount int
		expectError   bool
	}{
		{"testuser1", 1, false},
		{"testuser", 1, false},
		{"testuser2", 2, false},
		{"testuser3", 4, false},
		{"testuser4", 2, false},
		{"unknown", 0, true},
	}

	for _, tc := range testCases {
		t.Run(tc.username, func(t *testing.T) {
			result, err := mockLdapSearchResult(tc.username)

			if tc.expectError {
				assert.Assert(t, err != nil, "Expected error for user %s but got none", tc.username)
				assert.Assert(t, result == nil, "Expected nil result for user %s but got %v", tc.username, result)
				assert.ErrorContains(t, err, "ldap lookup failed for user: "+tc.username)
			} else {
				assert.NilError(t, err, "Unexpected error for user %s: %v", tc.username, err)
				assert.Assert(t, result != nil, "Expected non-nil result for user %s", tc.username)
				assert.Assert(t, len(result.Entries) > 0, "Expected entries for user %s", tc.username)
				assert.Assert(t, len(result.Entries[0].Attributes) > 0, "Expected attributes for user %s", tc.username)

				memberOfAttr := result.Entries[0].Attributes[0]
				assert.Equal(t, "memberOf", memberOfAttr.Name, "Expected 'memberOf' attribute for user %s", tc.username)
				assert.Equal(t, tc.expectedCount, len(memberOfAttr.Values),
					"Expected %d group values for user %s but got %d",
					tc.expectedCount, tc.username, len(memberOfAttr.Values))
			}
		})
	}
}

func TestLdapAccessImpl(t *testing.T) {
	// Create a mock LDAP access implementation
	mockAccess := &LdapAccessMock{
		DialURLFunc: func(url string, options ...ldap.DialOpt) (*ldap.Conn, error) {
			return &ldap.Conn{}, nil
		},
		BindFunc: func(conn *ldap.Conn, username, password string) error {
			return nil
		},
		SearchFunc: func(conn *ldap.Conn, searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error) {
			return &ldap.SearchResult{}, nil
		},
		CloseFunc: func(conn *ldap.Conn) {},
	}

	assert.Assert(t, mockAccess != nil)
	conn, err := mockAccess.DialURL("testurl")
	assert.NilError(t, err)
	assert.Assert(t, conn != nil)
	assert.NilError(t, mockAccess.Bind(&ldap.Conn{}, "user", "pass"))
	result, err := mockAccess.Search(&ldap.Conn{}, &ldap.SearchRequest{})
	assert.NilError(t, err)
	assert.Assert(t, result != nil)
	mockAccess.Close(&ldap.Conn{})
}

// TestLdapAccessImplMethods tests the LdapAccessImpl methods
func TestLdapAccessImplMethods(t *testing.T) {
	// Create a real implementation
	impl := &LdapAccessImpl{}

	// We can't actually connect to an LDAP server in unit tests,
	// but we can verify the methods don't panic when called with nil

	// Test DialURL - should return error with invalid URL
	conn, err := impl.DialURL("invalid://url")
	assert.Assert(t, err != nil)
	assert.Assert(t, conn == nil)

	// Other methods would panic if called with nil, so we can't test them directly
	// In a real scenario, we'd use a mock LDAP server or dependency injection
}
