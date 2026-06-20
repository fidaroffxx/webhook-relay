package base

type Error struct {
	TraceID string `json:"traceID"`
	Code    int    `json:"code"`
	Message string `json:"message"`
}
