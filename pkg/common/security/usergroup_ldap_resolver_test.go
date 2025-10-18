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

	"github.com/go-ldap/ldap/v3"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
)

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

type ConfigReaderMock struct{}

func (ConfigReaderMock) ReadLdapConfig() (*LdapConfig, error) {
	return &LdapConfig{}, nil
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

// TestLdapSearch tests the new ldapSearch function with a mock LdapAccess
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

	// Call ldapSearch with the mock access
	result, err := ldapSearch(mockAccess, LdapConfig{}, "testuser")

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

// TestLdapSearchError tests the error handling in ldapSearch
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

			// Call ldapSearch with the mock access
			result, err := ldapSearch(mockAccess, LdapConfig{}, "testuser")

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

	lu := &LdapLookup{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.testType {
			case "user":
				u, err := lu.LdapLookupUser(tt.id)
				tt.validate(t, u, err)
			case "group":
				g, err := lu.LdapLookupGroupID(tt.id)
				tt.validate(t, g, err)
			}
		})
	}
}

func TestLDAPLookupGroupIds(t *testing.T) {
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

	u := &user.User{Username: "testuser"}
	lu := &LdapLookup{
		access: newMockLdapAccess(mockResult, nil),
		config: LdapConfig{},
	}

	groups, err := lu.LDAPLookupGroupIds(u)
	assert.NilError(t, err)
	assert.Assert(t, strings.Contains(strings.Join(groups, ","), "group1"))
	assert.Assert(t, strings.Contains(strings.Join(groups, ","), "group2"))
}

func TestLDAPLookupGroupIdsError(t *testing.T) {
	u := &user.User{Username: "testuser"}
	lu := &LdapLookup{
		access: newMockLdapAccess(nil, errors.New("ldap error")),
		config: LdapConfig{},
	}
	groups, err := lu.LDAPLookupGroupIds(u)
	assert.Error(t, err, "ldap error")
	assert.Assert(t, groups == nil)
}

//nolint:funlen // Table-driven test for coverage, helpers used to reduce length
func TestReadSecrets(t *testing.T) {
	tests := getReadSecretsTestCases()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, cleanup := tt.setupFunc(t)
			defer cleanup()
			reader := &configReaderImpl{}
			ldapConf, err := reader.ReadLdapConfig()
			assert.Equal(t, tt.expectedResult, err == nil)
			if tt.nilConf && ldapConf == nil {
				return
			}
			tt.validateFunc(t, ldapConf)
		})
	}
}

//nolint:funlen // Table-driven test helper for coverage, intentionally long
func getReadSecretsTestCases() []struct {
	name           string
	setupFunc      func(t *testing.T) (string, func())
	expectedResult bool
	nilConf        bool
	validateFunc   func(t *testing.T, conf *LdapConfig)
} {
	return []struct {
		name           string
		setupFunc      func(t *testing.T) (string, func())
		expectedResult bool
		nilConf        bool
		validateFunc   func(t *testing.T, conf *LdapConfig)
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
			validateFunc: func(t *testing.T, ldapConf *LdapConfig) {
				assert.Equal(t, common.DefaultLdapHost, ldapConf.Host)
				assert.Equal(t, common.DefaultLdapPort, ldapConf.Port)
				assert.Equal(t, common.DefaultLdapBaseDN, ldapConf.BaseDN)
				assert.Equal(t, common.DefaultLdapFilter, ldapConf.Filter)
				assert.Equal(t, common.DefaultLdapGroupAttr, ldapConf.GroupAttr)
				assert.Equal(t, strings.Join(common.DefaultLdapReturnAttr, ","), strings.Join(ldapConf.ReturnAttr, ","))
				assert.Equal(t, common.DefaultLdapBindUser, ldapConf.BindUser)
				assert.Equal(t, common.DefaultLdapBindPassword, ldapConf.BindPassword)
				assert.Equal(t, common.DefaultLdapInsecure, ldapConf.Insecure)
				assert.Equal(t, common.DefaultLdapSSL, ldapConf.useSsl)
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
			nilConf:        true,
			validateFunc:   func(t *testing.T, ldapConf *LdapConfig) {},
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
			validateFunc: func(t *testing.T, ldapConf *LdapConfig) {
				assert.Equal(t, common.DefaultLdapHost, ldapConf.Host)
				assert.Equal(t, common.DefaultLdapPort, ldapConf.Port)
				assert.Equal(t, common.DefaultLdapBaseDN, ldapConf.BaseDN)
				assert.Equal(t, common.DefaultLdapFilter, ldapConf.Filter)
				assert.Equal(t, common.DefaultLdapGroupAttr, ldapConf.GroupAttr)
				assert.Equal(t, strings.Join(common.DefaultLdapReturnAttr, ","), strings.Join(ldapConf.ReturnAttr, ","))
				assert.Equal(t, common.DefaultLdapBindUser, ldapConf.BindUser)
				assert.Equal(t, common.DefaultLdapBindPassword, ldapConf.BindPassword)
				assert.Equal(t, common.DefaultLdapInsecure, ldapConf.Insecure)
				assert.Equal(t, common.DefaultLdapSSL, ldapConf.useSsl)
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
			validateFunc: func(t *testing.T, ldapConf *LdapConfig) {
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
				assert.Equal(t, common.DefaultLdapSSL, ldapConf.useSsl)
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
			validateFunc: func(t *testing.T, ldapConf *LdapConfig) {
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
				assert.Equal(t, expectedSSL, ldapConf.useSsl)
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
			validateFunc: func(_ *testing.T, _ *LdapConfig) {
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
			validateFunc: func(_ *testing.T, _ *LdapConfig) {
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
			// Get the LDAP user group cache
			cache := GetUserGroupCacheLdap(&ConfigReaderMock{}, newMockLdapAccess(nil, nil))

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

// TestLdapAccessImplMethods tests the ldapAccessImpl methods
func TestLdapAccessImplMethods(t *testing.T) {
	// Create a real implementation
	impl := &ldapAccessImpl{}

	// We can't actually connect to an LDAP server in unit tests,
	// but we can verify the methods don't panic when called with nil

	// Test DialURL - should return error with invalid URL
	conn, err := impl.DialURL("invalid://url")
	assert.Assert(t, err != nil)
	assert.Assert(t, conn == nil)

	// Other methods would panic if called with nil, so we can't test them directly
	// In a real scenario, we'd use a mock LDAP server or dependency injection
}
