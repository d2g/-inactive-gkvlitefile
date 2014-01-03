package gkvlitefile

import (
	"encoding/json"
	"github.com/steveyen/gkvlite"
	"math/rand"
	"sync"
)

type Collection struct {
	gkvlite.Collection
	Channel    chan bool
	writeMutex sync.Mutex

	// By Providing Marshaller and Unmarshaller Functions we can marshal to BSON
	// Rather then JSON without being dependant on BSON.
	marshal   MarshalFunction
	unmarshal UnmarshalFunction
}

type MarshalFunction func(interface{}) ([]byte, error)
type UnmarshalFunction func([]byte, interface{}) error

func (t *Collection) SetItem(item *gkvlite.Item) error {
	t.writeMutex.Lock()
	err := t.Collection.SetItem(item)
	t.Channel <- true
	t.writeMutex.Unlock()
	return err
}

func (t *Collection) Set(key []byte, val []byte) error {
	return t.SetItem(&gkvlite.Item{Key: key, Val: val, Priority: rand.Int31()})
}

func (t *Collection) Marshal() MarshalFunction {
	if t.Marshal != nil {
		return t.marshal
	} else {
		return json.Marshal
	}
}

func (t *Collection) SetMarshal(override MarshalFunction) {
	t.marshal = override
}

func (t *Collection) Unmarshal() UnmarshalFunction {
	if t.Unmarshal != nil {
		return t.unmarshal
	} else {
		return json.Unmarshal
	}
}

func (t *Collection) SetUnmarshal(override UnmarshalFunction) {
	t.unmarshal = override
}

func (t *Collection) SetObject(key string, object interface{}) error {
	marshalFunction := t.Marshal()

	byteObject, err := marshalFunction(object)

	if err != nil {
		return err
	}

	return t.Set([]byte(key), byteObject)
}

func (t *Collection) GetObject(key string, object interface{}) error {

	byteObject, err := t.Collection.Get([]byte(key))
	if err != nil {
		return err
	}

	if byteObject != nil {
		unmarshalFunction := t.Unmarshal()

		err = unmarshalFunction(byteObject, object)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Collection) Delete(key []byte) (bool, error) {
	t.writeMutex.Lock()
	deleted, err := t.Collection.Delete(key)
	t.Channel <- true
	t.writeMutex.Unlock()
	return deleted, err
}

func (t *Collection) Write() error {
	t.writeMutex.Lock()

	err := t.Collection.Write()

	t.Channel <- true
	t.writeMutex.Unlock()
	return err
}
