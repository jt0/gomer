package util

type Dirtyable interface {
	IsDirty() bool
	SetDirty()
	ClearDirty()
}

type Dirty struct {
	dirty bool
}

func (d Dirty) IsDirty() bool {
	return d.dirty
}

func (d Dirty) SetDirty() {
	d.dirty = true
}

func (d Dirty) ClearDirty() {
	d.dirty = false
}
