// Copyright (c) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package utp

type rstInfoList []rstInfo

func (ril rstInfoList) MoveUpLast(index int) bool {
	assert(index < len(ril))
	c := len(ril) - 1
	itemToMove := ril[c]
	ril = ril[:c]
	if index != c {
		ril[index] = itemToMove
		return true
	}
	return false
}

func (ril rstInfoList) Compact()      {}
func (ril rstInfoList) GetCount() int { return len(ril) }
func (ril rstInfoList) GetAlloc() int { return cap(ril) }
