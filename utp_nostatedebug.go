// Copyright (c) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

//+build !utpstatedebug

package utp

func (c *Conn) stateDebugLog(msg string, keys ...interface{}) {}
func (c *Conn) stateDebugLogLocked(msg string, keys ...interface{}) {}
