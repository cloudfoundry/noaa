package test_helpers

import (
	"fmt"
	"net/http"
)

type AuthFailure struct {
	Message string
}

func (failer AuthFailure) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("WWW-Authenticate", "Basic")
	rw.WriteHeader(http.StatusUnauthorized)
	fmt.Fprintf(rw, "You are not authorized. %s", failer.Message)
}
