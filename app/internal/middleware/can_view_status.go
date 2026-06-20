package middleware

import (
	"log"
	"net/http"
)

type CanViewStatus struct {
}

func NewCanViewStatus() *CanViewStatus {
	return &CanViewStatus{}
}

func (c *CanViewStatus) Can() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Println(r.RequestURI)

			next.ServeHTTP(w, r)
		})
	}
}
