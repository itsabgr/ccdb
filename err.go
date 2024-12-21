package ccdb

type ErrModified struct{}

func (ErrModified) Error() string {
	return "modified"
}

func (ErrModified) String() string {
	return "error: modified"
}

func (ErrModified) ErrModified() {}
