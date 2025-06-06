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
	"crypto/tls"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/go-ldap/ldap/v3"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/log"
)

// This file contains the implementation of the LDAP resolver for user groups

// LdapAccess defines the interface for LDAP operations
type LdapAccess interface {
	// DialURL establishes a connection to the LDAP server
	DialURL(url string, options ...ldap.DialOpt) (*ldap.Conn, error)

	// Bind authenticates with the LDAP server
	Bind(conn *ldap.Conn, username, password string) error

	// Search performs an LDAP search operation
	Search(conn *ldap.Conn, searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error)

	// Close closes the LDAP connection
	Close(conn *ldap.Conn)
}

// LdapAccessImpl implements the LdapAccess interface with real LDAP operations
type LdapAccessImpl struct{}

func (l *LdapAccessImpl) DialURL(url string, options ...ldap.DialOpt) (*ldap.Conn, error) {
	return ldap.DialURL(url, options...)
}

func (l *LdapAccessImpl) Bind(conn *ldap.Conn, username, password string) error {
	return conn.Bind(username, password)
}

func (l *LdapAccessImpl) Search(conn *ldap.Conn, searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error) {
	return conn.Search(searchRequest)
}

func (l *LdapAccessImpl) Close(conn *ldap.Conn) {
	conn.Close()
}

// ldapAccessFactory is a function type that creates LdapAccess instances
type ldapAccessFactory func(config *LdapResolverConfig) LdapAccess

// defaultLdapAccessFactory is the default factory function that creates real LdapAccessImpl instances
var defaultLdapAccessFactory ldapAccessFactory = func(config *LdapResolverConfig) LdapAccess {
	return &LdapAccessImpl{}
}

// newLdapAccessImpl creates a new LdapAccess instance using the current factory
// This can be replaced in tests to return mock implementations
var newLdapAccessImpl = defaultLdapAccessFactory

// resetLdapAccessFactory resets the factory to the default implementation
// This is used in tests to ensure the global state is restored
func resetLdapAccessFactory() {
	newLdapAccessImpl = defaultLdapAccessFactory
}

// LDAPResolverConfig holds the configuration for the LDAP resolver
type LdapResolverConfig struct {
	Host         string
	Port         int
	BaseDN       string
	Filter       string
	GroupAttr    string
	ReturnAttr   []string
	BindUser     string
	BindPassword string
	Insecure     bool
	SSL          bool
}

// Default values for the LDAP resolver
var ldapConf = LdapResolverConfig{
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

// read secrets from the secrets directory
// returns true if at least one secret was loaded and the configuration is valid, false otherwise
var readSecrets = func() bool {
	secretsDir := common.LdapMountPath

	// Read all files from secrets directory
	files, err := os.ReadDir(secretsDir)
	if err != nil {
		log.Log(log.Security).Error("Unable to access LDAP secrets directory",
			zap.String("directory", secretsDir),
			zap.Error(err))
		return false
	}

	secretCount := 0
	validSecrets := make(map[string]interface{})

	// Iterate over all secret files in the secrets directory
	for _, file := range files {
		fileName := file.Name()

		// Skip non-secret entries such as Kubernetes internal metadata (e.g., symlinks like "..data" or directories like "..timestamp")
		if strings.HasPrefix(fileName, "..") || file.IsDir() {
			log.Log(log.Security).Info("Ignoring non-secret entry (Kubernetes metadata entry or directory)",
				zap.String("name", fileName))
			continue
		}

		secretKey := fileName
		secretValueBytes, err := os.ReadFile(filepath.Join(secretsDir, secretKey))
		if err != nil {
			log.Log(log.Security).Warn("Could not read secret file",
				zap.String("file", secretKey),
				zap.Error(err))
			continue
		}
		secretValue := strings.TrimSpace(string(secretValueBytes))

		// Validate the secret value
		validatedValue, err := ValidateSecretValue(secretKey, secretValue)
		if err != nil {
			log.Log(log.Security).Warn("Invalid LDAP secret value",
				zap.String("key", secretKey),
				zap.Error(err))
			continue
		}

		// Store the validated value
		validSecrets[secretKey] = validatedValue
		secretCount++

		log.Log(log.Security).Debug("Loaded LDAP secret",
			zap.String("key", secretKey))
	}

	// Apply validated values to the configuration
	if host, ok := validSecrets[common.LdapHost].(string); ok {
		ldapConf.Host = host
	}
	if port, ok := validSecrets[common.LdapPort].(int); ok {
		ldapConf.Port = port
	}
	if baseDN, ok := validSecrets[common.LdapBaseDN].(string); ok {
		ldapConf.BaseDN = baseDN
	}
	if filter, ok := validSecrets[common.LdapFilter].(string); ok {
		ldapConf.Filter = filter
	}
	if groupAttr, ok := validSecrets[common.LdapGroupAttr].(string); ok {
		ldapConf.GroupAttr = groupAttr
	}
	if returnAttr, ok := validSecrets[common.LdapReturnAttr].([]string); ok {
		ldapConf.ReturnAttr = returnAttr
	}
	if bindUser, ok := validSecrets[common.LdapBindUser].(string); ok {
		ldapConf.BindUser = bindUser
	}
	if bindPassword, ok := validSecrets[common.LdapBindPassword].(string); ok {
		ldapConf.BindPassword = bindPassword
	}
	if insecure, ok := validSecrets[common.LdapInsecure].(bool); ok {
		ldapConf.Insecure = insecure
	}
	if ssl, ok := validSecrets[common.LdapSSL].(bool); ok {
		ldapConf.SSL = ssl
	}

	// Validate the entire configuration
	validator := NewLdapValidator()
	isValid := validator.ValidateConfig(&ldapConf)

	// Check if all required fields were provided in the secrets
	requiredFields := []string{
		common.LdapHost,
		common.LdapPort,
		common.LdapBaseDN,
		common.LdapFilter,
		common.LdapGroupAttr,
		common.LdapReturnAttr,
		common.LdapBindUser,
		common.LdapBindPassword,
	}

	missingFields := []string{}
	for _, field := range requiredFields {
		if _, ok := validSecrets[field]; !ok {
			missingFields = append(missingFields, field)
		}
	}

	if len(missingFields) > 0 {
		log.Log(log.Security).Error("Missing required LDAP configuration fields",
			zap.Strings("missingFields", missingFields))
		isValid = false
	}

	log.Log(log.Security).Info("Finished loading LDAP secrets",
		zap.Int("numberOfSecretsLoaded", secretCount),
		zap.Bool("configurationValid", isValid),
		zap.Int("missingRequiredFields", len(missingFields)))

	return secretCount > 0 && isValid && len(missingFields) == 0
}

func GetUserGroupCacheLdap() *UserGroupCache {
	secretsLoaded := readSecrets()

	if !secretsLoaded {
		// Log a FATAL level message - this is very prominent and will typically cause the application to exit
		log.Log(log.Security).Fatal("LDAP configuration not found or invalid. No secrets were loaded from the secrets directory.",
			zap.String("secretsPath", common.LdapMountPath),
			zap.String("resolution", "Ensure LDAP secrets are properly mounted and accessible"))

		// If the Fatal log doesn't cause an exit (depends on logger configuration),
		// we could also panic here to ensure the application stops
		panic("LDAP configuration not found or invalid")
	}

	return &UserGroupCache{
		ugs:           map[string]*UserGroup{},
		interval:      cleanerInterval * time.Second,
		lookup:        LdapLookupUser,
		lookupGroupID: LdapLookupGroupID,
		groupIds:      LDAPLookupGroupIds,
		stop:          make(chan struct{}),
	}
}

// Default linux behaviour: a user is member of the primary group with the same name
func LdapLookupUser(userName string) (*user.User, error) {
	log.Log(log.Security).Debug("Performing LDAP user lookup",
		zap.String("username", userName),
		zap.String("defaultUID", common.DefaultLdapUserUID))
	return &user.User{
		Uid:      common.DefaultLdapUserUID,
		Gid:      userName,
		Username: userName,
	}, nil
}

func LdapLookupGroupID(gid string) (*user.Group, error) {
	log.Log(log.Security).Debug("Looking up LDAP group ID",
		zap.String("groupID", gid))
	group := user.Group{Gid: gid}
	group.Name = gid
	return &group, nil
}

func LDAPLookupGroupIds(osUser *user.User) ([]string, error) {
	ldapAccess := newLdapAccessImpl(&ldapConf)
	sr, err := LdapSearch(ldapAccess, osUser.Username)
	if err != nil {
		log.Log(log.Security).Error("Failed to connect to LDAP for group lookup",
			zap.String("user", osUser.Username),
			zap.Error(err))
		return nil, err
	}

	var groups []string
	for _, entry := range sr.Entries {
		attr := entry.GetAttributeValues("memberOf")
		log.Log(log.Security).Debug("LDAP 'memberOf' attributes for user",
			zap.String("user", osUser.Username),
			zap.Strings("attributes", attr))
		for i := range attr {
			s := strings.Split(attr[i], ",")
			newgroup := strings.Split(s[0], "CN=")
			groups = append(groups, newgroup[1])
		}
	}
	return groups, nil
}

// LdapSearch performs an LDAP search for the specified username
// This replaces the old LDAPConn_Bind function with a more testable approach
func LdapSearch(ldapAccess LdapAccess, userName string) (*ldap.SearchResult, error) {
	var LDAP_URI string
	if ldapConf.SSL {
		LDAP_URI = "ldaps"
	} else {
		LDAP_URI = "ldap"
	}

	ldapaddr := fmt.Sprintf("%s://%s:%d", LDAP_URI, ldapConf.Host, ldapConf.Port)
	log.Log(log.Security).Debug("Attempting LDAP connection",
		zap.String("address", ldapaddr),
		zap.Bool("ssl", ldapConf.SSL),
		zap.Bool("insecureSkipVerify", ldapConf.Insecure))

	l, err := ldapAccess.DialURL(ldapaddr,
		ldap.DialWithTLSConfig(&tls.Config{InsecureSkipVerify: ldapConf.Insecure})) // #nosec G402
	if err != nil {
		log.Log(log.Security).Error("Error connecting to LDAP server",
			zap.String("address", ldapaddr),
			zap.Error(err))
		return nil, err
	}
	defer ldapAccess.Close(l)

	log.Log(log.Security).Debug("LDAP connection successful, attempting bind",
		zap.String("bindUser", ldapConf.BindUser))
	err = ldapAccess.Bind(l, ldapConf.BindUser, ldapConf.BindPassword)
	if err != nil {
		log.Log(log.Security).Error("Failed to bind with LDAP server",
			zap.String("bindDN", ldapConf.BindUser),
			zap.Error(err))
		return nil, err
	}

	filter := fmt.Sprintf(ldapConf.Filter, userName)
	log.Log(log.Security).Debug("Executing LDAP search",
		zap.String("baseDN", ldapConf.BaseDN),
		zap.String("filter", filter),
		zap.Strings("attributesToReturn", ldapConf.ReturnAttr))

	searchRequest := ldap.NewSearchRequest(
		ldapConf.BaseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		filter,
		ldapConf.ReturnAttr,
		nil,
	)
	sr, err := ldapAccess.Search(l, searchRequest)
	if err != nil {
		log.Log(log.Security).Error("Failed to execute LDAP search query",
			zap.String("filter", filter),
			zap.String("baseDN", ldapConf.BaseDN),
			zap.Error(err))
		return nil, err
	}

	log.Log(log.Security).Debug("LDAP search completed successfully",
		zap.String("username", userName),
		zap.Int("entriesFound", len(sr.Entries)))
	return sr, nil
}
