package main

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/thorn3r/tkv/pkg/kvstore"
)

func helloMuxHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello there\n"))
}

var kvs = kvstore.NewKVStore()

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/", helloMuxHandler)
	r.HandleFunc("/v1/{key}", kvs.KeyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", kvs.KeyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", kvs.KeyValueDeleteHandler).Methods("DELETE")
	log.Fatal(http.ListenAndServe(":8080", r))
}
