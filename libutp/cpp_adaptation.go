// Copyright (c) 2021 Storj Labs, Inc.
// Copyright (c) 2010 BitTorrent, Inc.
// See LICENSE for copying information.

package libutp

import (
	"fmt"
	"io/ioutil"
	"runtime"
	"strings"
)

// this is pretty dumb, but just for the sake of an easier port.
func dumbAssert(val bool) {
	if val {
		return
	}
	var pc [5]uintptr // more than 1 just in case CallersFrames needs them to account for inlined functions
	callers := runtime.Callers(2, pc[:])
	if callers == 0 {
		panic("failed assertion, can't get runtime stack")
	}
	frames := runtime.CallersFrames(pc[:])
	for {
		frame, more := frames.Next()
		if !more {
			break
		}
		if frame.Func == nil {
			continue
		}
		if frame.File == "" || frame.Line == 0 {
			panic(fmt.Sprintf("failed assertion in %q (line number unknown)", frame.Function))
		}
		line := readLineFromFile(frame.File, frame.Line)
		message := fmt.Sprintf("failed assertion in %s:%d", frame.File, frame.Line)
		if line != "" {
			message += "\n\n>>> " + line + "\n"
		}
		panic(message)
	}
	panic("failed assertion, and can't get caller info")
}

// lol memory efficiency.
func readLineFromFile(filePath string, lineNum int) string {
	if lineNum == 0 {
		return ""
	}
	contents, err := ioutil.ReadFile(filePath)
	if err != nil {
		return ""
	}
	lines := strings.Split(string(contents), "\n")
	if len(lines) > lineNum {
		return lines[lineNum-1]
	}
	return ""
}

// what generics? who needs generics? pshh

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func minUint(a, b uint) uint {
	if a < b {
		return a
	}
	return b
}

func minUint32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func maxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func maxUint(a, b uint) uint {
	if a > b {
		return a
	}
	return b
}

func abs(i int) int {
	if i < 0 {
		return -i
	}
	return i
}

func clamp(lo, x, hi int) int {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}
