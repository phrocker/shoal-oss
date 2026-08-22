/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package shoal

import (
	"errors"
	"fmt"
)

// ErrorCode identifies a transport-independent failure category.
type ErrorCode string

const (
	ErrorInvalidArgument ErrorCode = "invalid_argument"
	ErrorNotFound        ErrorCode = "not_found"
	ErrorConflict        ErrorCode = "conflict"
	ErrorUnauthorized    ErrorCode = "unauthorized"
	ErrorUnavailable     ErrorCode = "unavailable"
	ErrorCanceled        ErrorCode = "canceled"
	ErrorDeadline        ErrorCode = "deadline_exceeded"
	ErrorInternal        ErrorCode = "internal"
)

// Error is a transport-independent Shoal failure.
type Error struct {
	Code    ErrorCode
	Message string
	cause   error
}

// NewError constructs a public error without an underlying cause.
func NewError(code ErrorCode, message string) *Error {
	return &Error{Code: code, Message: message}
}

// WrapError constructs a public error that retains its underlying cause.
func WrapError(code ErrorCode, message string, cause error) *Error {
	return &Error{Code: code, Message: message, cause: cause}
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message == "" {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// IsErrorCode reports whether err or any wrapped error has code.
func IsErrorCode(err error, code ErrorCode) bool {
	var target *Error
	return errors.As(err, &target) && target.Code == code
}
