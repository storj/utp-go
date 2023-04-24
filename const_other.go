// Copyright (c) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

//go:build !windows
// +build !windows

package utp

import "syscall"

const (
	msg_trunc = syscall.MSG_TRUNC
)
