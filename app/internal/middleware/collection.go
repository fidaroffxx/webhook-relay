package middleware

type Collection struct {
	canViewStatus *CanViewStatus
}

func NewCollection() *Collection {
	return &Collection{
		canViewStatus: NewCanViewStatus(),
	}
}

func (c *Collection) GetCanViewStatus() *CanViewStatus {
	return c.canViewStatus
}
