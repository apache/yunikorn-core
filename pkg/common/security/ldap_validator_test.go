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
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
)

func TestValidateHostValue(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		wantError bool
	}{
		{"Valid hostname", "ldap.example.com", false},
		{"Valid IP", "192.168.1.1", false},
		{"Empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validateHostValue(tt.value)
			if tt.wantError {
				assert.Assert(t, err != nil)
			} else {
				assert.NilError(t, err)
			}
		})
	}
}

func TestValidatePortValue(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		wantError bool
	}{
		{"Valid port", "389", false},
		{"Valid port range", "65535", false},
		{"Invalid port - too high", "65536", true},
		{"Invalid port - too low", "0", true},
		{"Invalid port - not a number", "abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validatePortValue(tt.value)
			if tt.wantError {
				assert.Assert(t, err != nil)
			} else {
				assert.NilError(t, err)
			}
		})
	}
}

func TestValidateFilterValue(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		wantError bool
	}{
		{"Valid filter", "(&(objectClass=user)(sAMAccountName=%s))", false},
		{"Missing placeholder", "(&(objectClass=user)(sAMAccountName=user))", true},
		{"Unbalanced parentheses", "(&(objectClass=user)(sAMAccountName=%s)", true},
		{"Empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validateFilterValue(tt.value)
			if tt.wantError {
				assert.Assert(t, err != nil)
			} else {
				assert.NilError(t, err)
			}
		})
	}
}

func TestValidateReturnAttrValue(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		wantError bool
	}{
		{"Valid single attr", "memberOf", false},
		{"Valid multiple attrs", "memberOf,cn,mail", false},
		{"Empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs, err := validateReturnAttrValue(tt.value)
			if tt.wantError {
				assert.Assert(t, err != nil)
			} else {
				assert.NilError(t, err)
				if tt.value == "memberOf" {
					assert.Equal(t, 1, len(attrs))
					assert.Equal(t, "memberOf", attrs[0])
				} else if tt.value == "memberOf,cn,mail" {
					assert.Equal(t, 3, len(attrs))
					assert.Equal(t, "memberOf", attrs[0])
					assert.Equal(t, "cn", attrs[1])
					assert.Equal(t, "mail", attrs[2])
				}
			}
		})
	}
}

func TestValidateBoolValue(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		expected  bool
		wantError bool
	}{
		{"Valid true", "true", true, false},
		{"Valid false", "false", false, false},
		{"Valid 1", "1", true, false},
		{"Valid 0", "0", false, false},
		{"Invalid", "notabool", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := validateBoolValue(tt.value)
			if tt.wantError {
				assert.Assert(t, err != nil)
			} else {
				assert.NilError(t, err)
				assert.Equal(t, tt.expected, val)
			}
		})
	}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name     string
		config   *LdapResolverConfig
		expected bool
	}{
		{
			name: "Valid configuration",
			config: &LdapResolverConfig{
				Host:         "ldap.example.com",
				Port:         389,
				BaseDN:       "dc=example,dc=com",
				Filter:       "(&(objectClass=user)(sAMAccountName=%s))",
				GroupAttr:    "memberOf",
				ReturnAttr:   []string{"memberOf"},
				BindUser:     "cn=admin,dc=example,dc=com",
				BindPassword: "password",
				Insecure:     false,
				SSL:          false,
			},
			expected: true,
		},
		{
			name: "Invalid configuration - empty fields",
			config: &LdapResolverConfig{
				Host:         "",
				Port:         0,
				BaseDN:       "",
				Filter:       "invalid-filter",
				GroupAttr:    "",
				ReturnAttr:   []string{},
				BindUser:     "",
				BindPassword: "",
				Insecure:     true,
				SSL:          true,
			},
			expected: false,
		},
		{
			name: "Invalid configuration - missing placeholder in filter",
			config: &LdapResolverConfig{
				Host:         "ldap.example.com",
				Port:         389,
				BaseDN:       "dc=example,dc=com",
				Filter:       "(&(objectClass=user)(sAMAccountName=user))", // Missing %s placeholder
				GroupAttr:    "memberOf",
				ReturnAttr:   []string{"memberOf"},
				BindUser:     "cn=admin,dc=example,dc=com",
				BindPassword: "password",
				Insecure:     false,
				SSL:          false,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewLdapValidator()
			result := validator.ValidateConfig(tt.config)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestValidateBaseDN tests the validateBaseDN method with various inputs
func TestValidateBaseDN(t *testing.T) {
	tests := []struct {
		name          string
		baseDN        string
		expectWarning bool
		expectError   bool
	}{
		{
			name:          "Valid BaseDN",
			baseDN:        "dc=example,dc=com",
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Empty BaseDN",
			baseDN:        "",
			expectWarning: false,
			expectError:   true,
		},
		{
			name:          "Invalid format - missing value",
			baseDN:        "dc=,dc=com",
			expectWarning: true,
			expectError:   false,
		},
		{
			name:          "Invalid format - missing equals",
			baseDN:        "dcexample,dc=com",
			expectWarning: true,
			expectError:   false,
		},
		{
			name:          "Invalid format - unbalanced commas",
			baseDN:        "dc=example,dc=com,",
			expectWarning: true,
			expectError:   false,
		},
		{
			name:          "No domain component",
			baseDN:        "cn=admin,ou=users",
			expectWarning: true,
			expectError:   false,
		},
		{
			name:          "Invalid format - extra comma",
			baseDN:        "dc=example,,dc=com",
			expectWarning: true,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewLdapValidator()
			validator.validateBaseDN(tt.baseDN)

			hasError := false
			hasWarning := false

			for _, issue := range validator.issues {
				if issue.Field == "BaseDN" {
					if issue.Level == ValidationError {
						hasError = true
					} else if issue.Level == ValidationWarning {
						hasWarning = true
					}
				}
			}

			assert.Equal(t, tt.expectError, hasError, "Expected error: %v, got: %v", tt.expectError, hasError)
			assert.Equal(t, tt.expectWarning, hasWarning, "Expected warning: %v, got: %v", tt.expectWarning, hasWarning)
		})
	}
}

// TestValidateGroupAttr tests the validateGroupAttr method with various inputs
func TestValidateGroupAttr(t *testing.T) {
	tests := []struct {
		name          string
		groupAttr     string
		expectWarning bool
		expectError   bool
	}{
		{
			name:          "Valid attribute name",
			groupAttr:     "memberOf",
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Empty attribute name",
			groupAttr:     "",
			expectWarning: false,
			expectError:   true,
		},
		{
			name:          "Invalid format - starts with number",
			groupAttr:     "1memberOf",
			expectWarning: true,
			expectError:   false,
		},
		{
			name:          "Invalid format - special characters",
			groupAttr:     "member@Of",
			expectWarning: true,
			expectError:   false,
		},
		{
			name:          "Valid with hyphen",
			groupAttr:     "member-of",
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Valid with underscore",
			groupAttr:     "member_of",
			expectWarning: false,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewLdapValidator()
			validator.validateGroupAttr(tt.groupAttr)

			hasError := false
			hasWarning := false

			for _, issue := range validator.issues {
				if issue.Field == "GroupAttr" {
					if issue.Level == ValidationError {
						hasError = true
					} else if issue.Level == ValidationWarning {
						hasWarning = true
					}
				}
			}

			assert.Equal(t, tt.expectError, hasError, "Expected error: %v, got: %v", tt.expectError, hasError)
			assert.Equal(t, tt.expectWarning, hasWarning, "Expected warning: %v, got: %v", tt.expectWarning, hasWarning)
		})
	}
}

// TestValidateBindUser tests the validateBindUser method with various inputs
func TestValidateBindUser(t *testing.T) {
	tests := []struct {
		name          string
		bindUser      string
		expectWarning bool
		expectError   bool
	}{
		{
			name:          "Valid DN format",
			bindUser:      "cn=admin,dc=example,dc=com",
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Valid username format",
			bindUser:      "admin",
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Valid username with domain",
			bindUser:      "admin@example.com",
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Empty bind user",
			bindUser:      "",
			expectWarning: false,
			expectError:   true,
		},
		{
			name:          "Invalid format - special characters",
			bindUser:      "admin!#$%",
			expectWarning: true,
			expectError:   false,
		},
		{
			name:          "Invalid DN format",
			bindUser:      "cn=admin,dc=example,=com",
			expectWarning: true,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewLdapValidator()
			validator.validateBindUser(tt.bindUser)

			hasError := false
			hasWarning := false

			for _, issue := range validator.issues {
				if issue.Field == "BindUser" {
					if issue.Level == ValidationError {
						hasError = true
					} else if issue.Level == ValidationWarning {
						hasWarning = true
					}
				}
			}

			assert.Equal(t, tt.expectError, hasError, "Expected error: %v, got: %v", tt.expectError, hasError)
			assert.Equal(t, tt.expectWarning, hasWarning, "Expected warning: %v, got: %v", tt.expectWarning, hasWarning)
		})
	}
}

// TestValidateBindPassword tests the validateBindPassword method with various inputs
func TestValidateBindPassword(t *testing.T) {
	tests := []struct {
		name          string
		bindPassword  string
		expectWarning bool
		expectError   bool
	}{
		{
			name:          "Valid password",
			bindPassword:  "password123",
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Empty password",
			bindPassword:  "",
			expectWarning: false,
			expectError:   true,
		},
		{
			name:          "Very short password",
			bindPassword:  "a",
			expectWarning: true,
			expectError:   false,
		},
		{
			name:          "Password with special characters",
			bindPassword:  "p@ssw0rd!",
			expectWarning: false,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewLdapValidator()
			validator.validateBindPassword(tt.bindPassword)

			hasError := false
			hasWarning := false

			for _, issue := range validator.issues {
				if issue.Field == "BindPassword" {
					if issue.Level == ValidationError {
						hasError = true
					} else if issue.Level == ValidationWarning {
						hasWarning = true
					}
				}
			}

			assert.Equal(t, tt.expectError, hasError, "Expected error: %v, got: %v", tt.expectError, hasError)
			assert.Equal(t, tt.expectWarning, hasWarning, "Expected warning: %v, got: %v", tt.expectWarning, hasWarning)
		})
	}
}

// TestValidateReturnAttr tests the validateReturnAttr method with various inputs
func TestValidateReturnAttr(t *testing.T) {
	tests := []struct {
		name          string
		returnAttr    []string
		expectWarning bool
		expectError   bool
	}{
		{
			name:          "Valid single attribute",
			returnAttr:    []string{"memberOf"},
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Valid multiple attributes",
			returnAttr:    []string{"memberOf", "cn", "mail"},
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Empty array",
			returnAttr:    []string{},
			expectWarning: false,
			expectError:   true,
		},
		{
			name:          "Invalid attribute name",
			returnAttr:    []string{"member@Of"},
			expectWarning: true,
			expectError:   false,
		},
		{
			name:          "Mix of valid and invalid",
			returnAttr:    []string{"memberOf", "123invalid"},
			expectWarning: true,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewLdapValidator()
			validator.validateReturnAttr(tt.returnAttr)

			hasError := false
			hasWarning := false

			for _, issue := range validator.issues {
				if issue.Field == "ReturnAttr" {
					if issue.Level == ValidationError {
						hasError = true
					} else if issue.Level == ValidationWarning {
						hasWarning = true
					}
				}
			}

			assert.Equal(t, tt.expectError, hasError, "Expected error: %v, got: %v", tt.expectError, hasError)
			assert.Equal(t, tt.expectWarning, hasWarning, "Expected warning: %v, got: %v", tt.expectWarning, hasWarning)
		})
	}
}

// TestValidateHost tests the validateHost method with various inputs
func TestValidateHost(t *testing.T) {
	tests := []struct {
		name          string
		host          string
		expectWarning bool
		expectError   bool
	}{
		{
			name:          "Valid hostname",
			host:          "ldap.example.com",
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Valid IP address",
			host:          "192.168.1.1",
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Empty host",
			host:          "",
			expectWarning: false,
			expectError:   true,
		},
		{
			name:          "Invalid hostname - starts with hyphen",
			host:          "-ldap.example.com",
			expectWarning: true,
			expectError:   false,
		},
		{
			name:          "Invalid hostname - contains invalid characters",
			host:          "ldap_example.com",
			expectWarning: true,
			expectError:   false,
		},
		{
			name:          "Invalid hostname - double dots",
			host:          "ldap..example.com",
			expectWarning: true,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewLdapValidator()
			validator.validateHost(tt.host)

			hasError := false
			hasWarning := false

			for _, issue := range validator.issues {
				if issue.Field == "Host" {
					if issue.Level == ValidationError {
						hasError = true
					} else if issue.Level == ValidationWarning {
						hasWarning = true
					}
				}
			}

			assert.Equal(t, tt.expectError, hasError, "Expected error: %v, got: %v", tt.expectError, hasError)
			assert.Equal(t, tt.expectWarning, hasWarning, "Expected warning: %v, got: %v", tt.expectWarning, hasWarning)
		})
	}
}

// TestValidateFilter tests the validateFilter method with various inputs
func TestValidateFilter(t *testing.T) {
	tests := []struct {
		name          string
		filter        string
		expectWarning bool
		expectError   bool
	}{
		{
			name:          "Valid filter",
			filter:        "(&(objectClass=user)(sAMAccountName=%s))",
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Empty filter",
			filter:        "",
			expectWarning: false,
			expectError:   true,
		},
		{
			name:          "Missing placeholder",
			filter:        "(&(objectClass=user)(sAMAccountName=user))",
			expectWarning: false,
			expectError:   true,
		},
		{
			name:          "Unbalanced parentheses",
			filter:        "(&(objectClass=user)(sAMAccountName=%s)",
			expectWarning: false,
			expectError:   true,
		},
		{
			name:          "Not enclosed in parentheses",
			filter:        "objectClass=user&sAMAccountName=%s",
			expectWarning: true,
			expectError:   false, // This filter does contain the %s placeholder, so it's not an error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewLdapValidator()
			validator.validateFilter(tt.filter)

			hasError := false
			hasWarning := false

			for _, issue := range validator.issues {
				if issue.Field == "Filter" {
					if issue.Level == ValidationError {
						hasError = true
					} else if issue.Level == ValidationWarning {
						hasWarning = true
					}
				}
			}

			assert.Equal(t, tt.expectError, hasError, "Expected error: %v, got: %v", tt.expectError, hasError)
			assert.Equal(t, tt.expectWarning, hasWarning, "Expected warning: %v, got: %v", tt.expectWarning, hasWarning)
		})
	}
}

func TestValidateSecretValue(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		value     string
		wantError bool
	}{
		{"Valid host", common.LdapHost, "ldap.example.com", false},
		{"Valid port", common.LdapPort, "389", false},
		{"Valid baseDN", common.LdapBaseDN, "dc=example,dc=com", false},
		{"Valid filter", common.LdapFilter, "(&(objectClass=user)(sAMAccountName=%s))", false},
		{"Valid groupAttr", common.LdapGroupAttr, "memberOf", false},
		{"Valid returnAttr", common.LdapReturnAttr, "memberOf,cn", false},
		{"Valid bindUser", common.LdapBindUser, "cn=admin,dc=example,dc=com", false},
		{"Valid bindPassword", common.LdapBindPassword, "password", false},
		{"Valid insecure", common.LdapInsecure, "true", false},
		{"Valid SSL", common.LdapSSL, "false", false},
		{"Invalid key", "unknown", "value", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateSecretValue(tt.key, tt.value)
			if tt.wantError {
				assert.Assert(t, err != nil)
			} else {
				assert.NilError(t, err)
			}
		})
	}
}

func TestHasBalancedParentheses(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"Empty string", "", true},
		{"Simple balanced", "()", true},
		{"Nested balanced", "((()))", true},
		{"Complex balanced", "(a(b)c(d(e)f)g)", true},
		{"Unbalanced - too many open", "(()", false},
		{"Unbalanced - too many closed", "())", false},
		{"Unbalanced - wrong order", ")(", false},
		{"No parentheses", "abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasBalancedParentheses(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestLdapValidatorAddIssue(t *testing.T) {
	validator := NewLdapValidator()

	// Add a warning
	validator.addIssue("TestField", "Test warning", ValidationWarning)

	// Add an error
	validator.addIssue("TestField2", "Test error", ValidationError)

	// Check that issues were added
	assert.Equal(t, 2, len(validator.issues))
	assert.Equal(t, "TestField", validator.issues[0].Field)
	assert.Equal(t, "Test warning", validator.issues[0].Message)
	assert.Equal(t, ValidationWarning, validator.issues[0].Level)
	assert.Equal(t, "TestField2", validator.issues[1].Field)
	assert.Equal(t, "Test error", validator.issues[1].Message)
	assert.Equal(t, ValidationError, validator.issues[1].Level)

	// Check hasErrors
	assert.Assert(t, validator.hasErrors())
}

func TestLdapValidatorValidateConsistency(t *testing.T) {
	tests := []struct {
		name          string
		config        *LdapResolverConfig
		expectWarning bool
	}{
		{
			name: "No warnings",
			config: &LdapResolverConfig{
				SSL:      false,
				Insecure: false,
				Port:     389,
			},
			expectWarning: false,
		},
		{
			name: "SSL with non-standard port",
			config: &LdapResolverConfig{
				SSL:      true,
				Insecure: false,
				Port:     389,
			},
			expectWarning: true,
		},
		{
			name: "SSL with insecure",
			config: &LdapResolverConfig{
				SSL:      true,
				Insecure: true,
				Port:     636,
			},
			expectWarning: true,
		},
		{
			name: "SSL with standard port",
			config: &LdapResolverConfig{
				SSL:      true,
				Insecure: false,
				Port:     636,
			},
			expectWarning: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewLdapValidator()
			validator.validateConsistency(tt.config)

			hasWarnings := len(validator.issues) > 0
			assert.Equal(t, tt.expectWarning, hasWarnings)
		})
	}
}

// TestValidatePort tests the validatePort method with various inputs
func TestValidatePort(t *testing.T) {
	tests := []struct {
		name          string
		port          int
		expectWarning bool
		expectError   bool
	}{
		{
			name:          "Valid port - LDAP",
			port:          389,
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Valid port - LDAPS",
			port:          636,
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Valid port - custom",
			port:          1389,
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Invalid port - too low",
			port:          0,
			expectWarning: false,
			expectError:   true,
		},
		{
			name:          "Invalid port - too high",
			port:          65536,
			expectWarning: false,
			expectError:   true,
		},
		{
			name:          "Valid port - minimum",
			port:          1,
			expectWarning: false,
			expectError:   false,
		},
		{
			name:          "Valid port - maximum",
			port:          65535,
			expectWarning: false,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewLdapValidator()
			validator.validatePort(tt.port)

			hasError := false
			hasWarning := false

			for _, issue := range validator.issues {
				if issue.Field == "Port" {
					if issue.Level == ValidationError {
						hasError = true
					} else if issue.Level == ValidationWarning {
						hasWarning = true
					}
				}
			}

			assert.Equal(t, tt.expectError, hasError, "Expected error: %v, got: %v", tt.expectError, hasError)
			assert.Equal(t, tt.expectWarning, hasWarning, "Expected warning: %v, got: %v", tt.expectWarning, hasWarning)
		})
	}
}

// TestLogIssues tests the logIssues method
func TestLogIssues(t *testing.T) {
	validator := NewLdapValidator()

	// Add a warning
	validator.addIssue("TestField1", "Test warning message", ValidationWarning)

	// Add an error
	validator.addIssue("TestField2", "Test error message", ValidationError)

	// Call logIssues - we can't easily capture the log output in a unit test,
	// but we can at least verify it doesn't panic
	validator.logIssues()

	// Verify the issues are still present after logging
	assert.Equal(t, 2, len(validator.issues))
	assert.Equal(t, "TestField1", validator.issues[0].Field)
	assert.Equal(t, ValidationWarning, validator.issues[0].Level)
	assert.Equal(t, "Test warning message", validator.issues[0].Message)
	assert.Equal(t, "TestField2", validator.issues[1].Field)
	assert.Equal(t, ValidationError, validator.issues[1].Level)
	assert.Equal(t, "Test error message", validator.issues[1].Message)
}

// TestValidateBindUserValue tests the validateBindUserValue function
func TestValidateBindUserValue(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		wantError bool
	}{
		{"Valid DN", "cn=admin,dc=example,dc=com", false},
		{"Valid username", "admin", false},
		{"Empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validateBindUserValue(tt.value)
			if tt.wantError {
				assert.Assert(t, err != nil)
			} else {
				assert.NilError(t, err)
			}
		})
	}
}

// TestValidateBaseDNValueEdgeCases tests edge cases for validateBaseDNValue
func TestValidateBaseDNValueEdgeCases(t *testing.T) {
	// Test empty value
	_, err := validateBaseDNValue("")
	assert.Assert(t, err != nil)
	assert.ErrorContains(t, err, "baseDN cannot be empty")
}

// TestValidateGroupAttrValueEdgeCases tests edge cases for validateGroupAttrValue
func TestValidateGroupAttrValueEdgeCases(t *testing.T) {
	// Test empty value
	_, err := validateGroupAttrValue("")
	assert.Assert(t, err != nil)
	assert.ErrorContains(t, err, "groupAttr cannot be empty")
}

// TestValidateReturnAttrValueEdgeCases tests edge cases for validateReturnAttrValue
func TestValidateReturnAttrValueEdgeCases(t *testing.T) {
	// Test empty value
	_, err := validateReturnAttrValue("")
	assert.Assert(t, err != nil)
	assert.ErrorContains(t, err, "returnAttr cannot be empty")
}

// TestValidateBindPasswordValueEdgeCases tests edge cases for validateBindPasswordValue
func TestValidateBindPasswordValueEdgeCases(t *testing.T) {
	// Test empty value
	_, err := validateBindPasswordValue("")
	assert.Assert(t, err != nil)
	assert.ErrorContains(t, err, "bindPassword cannot be empty")
}
