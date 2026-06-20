package model

type Status struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

func (status *Status) TableName() string {
	return "statuses"
}
