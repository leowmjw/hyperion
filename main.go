package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
)

var buildVersion string

type AppHandler struct {
	serveMux *http.ServeMux
}

func NewApp() *AppHandler {
	serveMux := http.NewServeMux()
	return &AppHandler{serveMux}
}

func (a *AppHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	a.serveMux.ServeHTTP(rw, req)
}

func main() {
	fmt.Printf("hyperion%s\n", buildVersion)
	httpAddr := "127.0.0.1:8000"
	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		log.Fatalf("FATAL: failed to listen on (%s) - %s", httpAddr, err.Error())
	}
	log.Printf("listening on %s", httpAddr)

	app := NewApp()

	server := &http.Server{Handler: app}
	err = server.Serve(listener)
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		log.Printf("ERROR: http.Serve() - %s", err.Error())
	}
	log.Printf("HTTP: closing %s", listener.Addr().String())
}
