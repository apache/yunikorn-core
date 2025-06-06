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
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/log"
)

// ValidationLevel defines the severity of validation issues
type ValidationLevel int

const (
	// ValidationWarning indicates a non-critical issue that allows operation but might cause problems
	ValidationWarning ValidationLevel = iota
	// ValidationError indicates a critical issue that prevents proper operation
	ValidationError
)

// ValidationIssue represents a single validation problem
type ValidationIssue struct {
	Field   string
	Message string
	Level   ValidationLevel
}

// LdapValidator provides validation for LDAP configuration
type LdapValidator struct {
	issues []ValidationIssue
}

// NewLdapValidator creates a new validator instance
func NewLdapValidator() *LdapValidator {
	return &LdapValidator{
		issues: make([]ValidationIssue, 0),
	}
}

// ValidateConfig validates the entire LDAP configuration
func (v *LdapValidator) ValidateConfig(config *LdapResolverConfig) bool {
	v.validateHost(config.Host)
	v.validatePort(config.Port)
	v.validateBaseDN(config.BaseDN)
	v.validateFilter(config.Filter)
	v.validateGroupAttr(config.GroupAttr)
	v.validateReturnAttr(config.ReturnAttr)
	v.validateBindUser(config.BindUser)
	v.validateBindPassword(config.BindPassword)

	// Consistency checks
	v.validateConsistency(config)

	// Log all issues
	v.logIssues()

	// Return true if no errors (warnings are acceptable)
	return !v.hasErrors()
}

// validateHost validates the LDAP host
func (v *LdapValidator) validateHost(host string) {
	if host == "" {
		v.addIssue("Host", "Host cannot be empty", ValidationError)
		return
	}

	// Check if it's an IP address
	if net.ParseIP(host) != nil {
		return // Valid IP address
	}

	// Check if it's a valid hostname
	hostnameRegex := regexp.MustCompile(`^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$`)
	if !hostnameRegex.MatchString(host) {
		v.addIssue("Host", fmt.Sprintf("Invalid hostname format: %s", host), ValidationWarning)
	}
}

// validatePort validates the LDAP port
func (v *LdapValidator) validatePort(port int) {
	if port < 1 || port > 65535 {
		v.addIssue("Port", fmt.Sprintf("Port must be between 1 and 65535, got: %d", port), ValidationError)
	}
}

// validateBaseDN validates the LDAP base DN
func (v *LdapValidator) validateBaseDN(baseDN string) {
	if baseDN == "" {
		v.addIssue("BaseDN", "BaseDN cannot be empty", ValidationError)
		return
	}

	// Check for at least one domain component
	if !strings.Contains(strings.ToLower(baseDN), "dc=") {
		v.addIssue("BaseDN", "BaseDN should contain at least one domain component (dc=)", ValidationWarning)
	}

	// Check for valid DN format
	dnRegex := regexp.MustCompile(`^(?:(?:[a-zA-Z0-9]+=[^,]+)(?:,(?:[a-zA-Z0-9]+=[^,]+))*)?$`)
	if !dnRegex.MatchString(baseDN) {
		v.addIssue("BaseDN", fmt.Sprintf("Invalid DN format: %s", baseDN), ValidationWarning)
	}
}

// validateFilter validates the LDAP filter
func (v *LdapValidator) validateFilter(filter string) {
	if filter == "" {
		v.addIssue("Filter", "Filter cannot be empty", ValidationError)
		return
	}

	// Check for username placeholder
	if !strings.Contains(filter, "%s") {
		v.addIssue("Filter", "Filter must contain '%s' placeholder for username substitution", ValidationError)
	}

	// Check for balanced parentheses
	if !hasBalancedParentheses(filter) {
		v.addIssue("Filter", "Filter has unbalanced parentheses", ValidationError)
	}

	// Basic filter format check
	filterRegex := regexp.MustCompile(`^\(.*\)$`)
	if !filterRegex.MatchString(filter) {
		v.addIssue("Filter", "Filter should be enclosed in parentheses", ValidationWarning)
	}
}

// validateGroupAttr validates the LDAP group attribute
func (v *LdapValidator) validateGroupAttr(groupAttr string) {
	if groupAttr == "" {
		v.addIssue("GroupAttr", "GroupAttr cannot be empty", ValidationError)
		return
	}

	// Check for valid attribute name format
	attrRegex := regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9\-_]*$`)
	if !attrRegex.MatchString(groupAttr) {
		v.addIssue("GroupAttr", fmt.Sprintf("Invalid attribute name format: %s", groupAttr), ValidationWarning)
	}
}

// validateReturnAttr validates the LDAP return attributes
func (v *LdapValidator) validateReturnAttr(returnAttr []string) {
	if len(returnAttr) == 0 {
		v.addIssue("ReturnAttr", "ReturnAttr cannot be empty", ValidationError)
		return
	}

	attrRegex := regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9\-_]*$`)
	for _, attr := range returnAttr {
		if !attrRegex.MatchString(attr) {
			v.addIssue("ReturnAttr", fmt.Sprintf("Invalid attribute name format: %s", attr), ValidationWarning)
		}
	}
}

// validateBindUser validates the LDAP bind user
func (v *LdapValidator) validateBindUser(bindUser string) {
	if bindUser == "" {
		v.addIssue("BindUser", "BindUser cannot be empty", ValidationError)
		return
	}

	// Check if it's a DN format
	dnRegex := regexp.MustCompile(`^(?:(?:[a-zA-Z0-9]+=[^,]+)(?:,(?:[a-zA-Z0-9]+=[^,]+))*)?$`)
	if dnRegex.MatchString(bindUser) {
		return // Valid DN format
	}

	// Check if it's a username format
	usernameRegex := regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9\._\-@]*$`)
	if !usernameRegex.MatchString(bindUser) {
		v.addIssue("BindUser", fmt.Sprintf("BindUser is neither a valid DN nor a valid username: %s", bindUser), ValidationWarning)
	}
}

// validateBindPassword validates the LDAP bind password
func (v *LdapValidator) validateBindPassword(bindPassword string) {
	if bindPassword == "" {
		v.addIssue("BindPassword", "BindPassword cannot be empty", ValidationError)
		return
	}

	// Check for minimum length
	if len(bindPassword) < 3 {
		v.addIssue("BindPassword", "BindPassword is too short", ValidationWarning)
	}

	// We don't check for password complexity here as it depends on the LDAP server policy
}

// validateConsistency performs cross-field validation
func (v *LdapValidator) validateConsistency(config *LdapResolverConfig) {
	// Check SSL and port consistency
	if config.SSL && config.Port != 636 {
		v.addIssue("Port", fmt.Sprintf("SSL is enabled but port is not the default LDAPS port (636), using: %d", config.Port), ValidationWarning)
	}

	// Check SSL and Insecure consistency
	if config.SSL && config.Insecure {
		v.addIssue("SSL/Insecure", "Both SSL and Insecure are enabled, which may indicate a security misconfiguration", ValidationWarning)
	}
}

// addIssue adds a validation issue to the list
func (v *LdapValidator) addIssue(field, message string, level ValidationLevel) {
	v.issues = append(v.issues, ValidationIssue{
		Field:   field,
		Message: message,
		Level:   level,
	})
}

// hasErrors checks if there are any validation errors
func (v *LdapValidator) hasErrors() bool {
	for _, issue := range v.issues {
		if issue.Level == ValidationError {
			return true
		}
	}
	return false
}

// logIssues logs all validation issues
func (v *LdapValidator) logIssues() {
	for _, issue := range v.issues {
		if issue.Level == ValidationError {
			log.Log(log.Security).Error("LDAP configuration validation error",
				zap.String("field", issue.Field),
				zap.String("message", issue.Message))
		} else {
			log.Log(log.Security).Warn("LDAP configuration validation warning",
				zap.String("field", issue.Field),
				zap.String("message", issue.Message))
		}
	}
}

// hasBalancedParentheses checks if a string has balanced parentheses
func hasBalancedParentheses(s string) bool {
	count := 0
	for _, c := range s {
		if c == '(' {
			count++
		} else if c == ')' {
			count--
			if count < 0 {
				return false
			}
		}
	}
	return count == 0
}

// ValidateSecretValue validates a single secret value based on its key
func ValidateSecretValue(key, value string) (interface{}, error) {
	switch key {
	case common.LdapHost:
		return validateHostValue(value)
	case common.LdapPort:
		return validatePortValue(value)
	case common.LdapBaseDN:
		return validateBaseDNValue(value)
	case common.LdapFilter:
		return validateFilterValue(value)
	case common.LdapGroupAttr:
		return validateGroupAttrValue(value)
	case common.LdapReturnAttr:
		return validateReturnAttrValue(value)
	case common.LdapBindUser:
		return validateBindUserValue(value)
	case common.LdapBindPassword:
		return validateBindPasswordValue(value)
	case common.LdapInsecure, common.LdapSSL:
		return validateBoolValue(value)
	default:
		return nil, fmt.Errorf("unknown LDAP secret key: %s", key)
	}
}

// Individual validation functions for each secret type
func validateHostValue(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("host cannot be empty")
	}
	return value, nil
}

func validatePortValue(value string) (int, error) {
	port, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid port number: %s", err)
	}
	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("port must be between 1 and 65535, got: %d", port)
	}
	return port, nil
}

func validateBaseDNValue(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("baseDN cannot be empty")
	}
	return value, nil
}

func validateFilterValue(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("filter cannot be empty")
	}
	if !strings.Contains(value, "%s") {
		return "", fmt.Errorf("filter must contain '%%s' placeholder for username substitution")
	}
	if !hasBalancedParentheses(value) {
		return "", fmt.Errorf("filter has unbalanced parentheses")
	}
	return value, nil
}

func validateGroupAttrValue(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("groupAttr cannot be empty")
	}
	return value, nil
}

func validateReturnAttrValue(value string) ([]string, error) {
	if value == "" {
		return nil, fmt.Errorf("returnAttr cannot be empty")
	}
	attrs := strings.Split(value, ",")
	if len(attrs) == 0 {
		return nil, fmt.Errorf("returnAttr must contain at least one attribute")
	}
	return attrs, nil
}

func validateBindUserValue(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("bindUser cannot be empty")
	}
	return value, nil
}

func validateBindPasswordValue(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("bindPassword cannot be empty")
	}
	return value, nil
}

func validateBoolValue(value string) (bool, error) {
	boolValue, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid boolean value: %s", err)
	}
	return boolValue, nil
}
