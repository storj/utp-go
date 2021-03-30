package utp

type RSTInfoList []rstInfo

func (ril RSTInfoList) MoveUpLast(index int) bool {
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

func (ril RSTInfoList) Compact() {}
func (ril RSTInfoList) GetCount() int { return len(ril) }
func (ril RSTInfoList) GetAlloc() int { return cap(ril) }
