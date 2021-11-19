package entcache

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// NATS provides an NATS cache that implements the AddGetter interface.
type NATS struct {
	kvStore nats.KeyValue
}

var _ AddGetDeleter = (*NATS)(nil)

// NewNATS returns a new NATS cache level from the given NATS JetStream's bucket.
//
//	conn, err := nats.Connect(nats.DefaultURL)
//	if err != nil {
//		panic(err)
//	}
//	js, err := conn.JetStream()
//	if err != nil {
//		panic(err)
//	}
//	kvStore, err := js.CreateKeyValue(&nats.KeyValueConfig{
//		Bucket:      "entcache",
//		Description: "Ent cache",
//		Storage:     nats.MemoryStorage,
//	})
//	if err != nil {
//		panic(err)
//	}
//	entcache.NewNATS(kvStore)
//
func NewNATS(kvStore nats.KeyValue) *NATS {
	return &NATS{
		kvStore: kvStore,
	}
}

// Add adds the entry to the cache.
func (n *NATS) Add(_ context.Context, k Key, e *Entry, ttl time.Duration) error {
	key := fmt.Sprint(k)
	if key == "" {
		return nil
	}

	buf, err := e.MarshalBinary()
	if err != nil {
		return err
	}

	_, err = n.kvStore.Put(key, buf)
	return err
}

// Get gets an entry from the cache.
func (n *NATS) Get(ctx context.Context, k Key) (*Entry, error) {
	key := fmt.Sprint(k)
	if key == "" {
		return nil, ErrNotFound
	}

	entry, err := n.kvStore.Get(key)
	if err == nats.ErrKeyNotFound {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}

	e := &Entry{}
	if err := e.UnmarshalBinary(entry.Value()); err != nil {
		return nil, err
	}

	return e, nil
}

// Del deletes an entry from the cache.
func (n *NATS) Del(_ context.Context, k Key) error {
	key := fmt.Sprint(k)
	if key == "" {
		return nil
	}

	return n.kvStore.Delete(key)
}
