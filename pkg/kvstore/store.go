package kvstore

import (
	"errors"
	"io"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	txl "github.com/thorn3r/tkv/pkg/txlogger"
	klog "k8s.io/klog/v2"
)

var ErrorNoSuchKey = errors.New("no such key")
var txlogger txl.TxLogger

type KVStore struct {
	sync.RWMutex
	m        map[string]string
	txlogger txl.TxLogger
}

func NewKVStore() *KVStore {
	kvs := &KVStore{m: make(map[string]string)}
	err := kvs.initializeTxLogger()
	if err != nil {
		klog.Errorf("failed to initialize txlogger: %v", err)
	}
	return kvs
}

func (kvs *KVStore) Put(key string, value string) error {
	kvs.Lock()
	kvs.m[key] = value
	kvs.Unlock()

	return nil
}

func (kvs *KVStore) Get(key string) (string, error) {
	kvs.RLock()
	value, ok := kvs.m[key]
	kvs.RUnlock()
	if !ok {
		return "", ErrorNoSuchKey
	}
	return value, nil
}

func (kvs *KVStore) Delete(key string) error {
	kvs.Lock()
	delete(kvs.m, key)
	kvs.Unlock()

	return nil
}

func (kvs *KVStore) initializeTxLogger() error {
	var err error
	kvs.txlogger, err = txl.NewPostgresTxLogger("psq.conf")
	if err != nil {
		return err
	}

	events, errors := kvs.txlogger.ReadEvents()
	e, ok := txl.Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case txl.EventPut:
				//klog.Infof("DEBUG: PUTing key %v value %v", e.Key, e.Value)
				err = kvs.Put(e.Key, e.Value)
			case txl.EventDelete:
				//klog.Infof("DEBUG: DELETEing key %v value %v", e.Key, e.Value)
				err = kvs.Delete(e.Key)
			}
		}
	}
	kvs.txlogger.Run()
	return err
}
func (kvs *KVStore) KeyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusInternalServerError,
		)
		return
	}
	kvs.txlogger.WritePut(key, string(value))
	err = kvs.Put(key, string(value))
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusInternalServerError,
		)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (kvs *KVStore) KeyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := kvs.Get(key)
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))
}

func (kvs *KVStore) KeyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	kvs.txlogger.WriteDelete(key)
	err := kvs.Delete(key)
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
