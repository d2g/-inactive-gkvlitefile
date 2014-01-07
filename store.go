package gkvlitefile

import (
	"errors"
	"github.com/steveyen/gkvlite"
	"log"
	"os"
)

type Store struct {
	*gkvlite.Store
	channel chan bool
	file    *os.File
}

/*
 * storePool Is used to cache all the Store instances stopping the creation of
 * two stores for a single instance. Atleast atempting to..
 */
var storePool map[string]*Store
var defaultStore string

/*
 * Return the first Store in the pool.
 */
func GetDefaultStoreFile() (*Store, error) {
	if len(storePool) > 0 && defaultStore != "" {
		return GetStoreFile(defaultStore)
	} else {
		return nil, errors.New("Store File Pool is empty")
	}
}

func GetStoreFile(filename string) (*Store, error) {
	//Use our cache...
	store := storePool[filename]
	if store != nil {
		return store, nil
	}

	var file *os.File

	if _, err := os.Stat(filename); err != nil {
		if os.IsNotExist(err) {
			//File Doesn't Exists Error
			file, err = os.Create(filename)
			if err != nil {
				return nil, err
			}
		} else {
			//Some other unknown error
			return nil, err
		}
	} else {
		//File Exists
		//Compact The Datastore
		err := compactStoreFile(filename)
		if err != nil {
			return nil, err
		}

		file, err = os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
	}

	gkvliteStore, err := gkvlite.NewStore(file)
	if err != nil {
		return nil, err
	}

	wrapperStore := Store{}
	wrapperStore.Store = gkvliteStore
	wrapperStore.channel = make(chan bool)
	wrapperStore.file = file

	storePool[filename] = &wrapperStore
	if defaultStore == "" {
		defaultStore = filename
	}

	go wrapperStore.ListenAndFlush()

	return &wrapperStore, nil
}

func compactStoreFile(filename string) error {
	_, err := os.Stat(filename + ".tmp")

	if err != nil {
		if os.IsNotExist(err) {
			//File doesn't exists
			//This is what we expect :D
		} else {
			//Some other error:
			return err
		}
	} else {
		//File exists so needs deleteing
		err = os.Remove(filename + ".tmp")
		if err != nil {
			return err
		}
	}

	compactStoreFile, err := os.Create(filename + ".tmp")
	if err != nil {
		return err
	}

	//Open our local file.
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	//Create a GVKLite file.
	gkvliteStore, err := gkvlite.NewStore(file)
	if err != nil {
		return err
	}

	compactedStore, err := gkvliteStore.CopyTo(compactStoreFile, 0)
	if err != nil {
		return err
	}

	err = compactedStore.Flush()
	if err != nil {
		compactedStore.Close()
		return err
	}

	compactedStore.Close()
	compactStoreFile.Close()

	gkvliteStore.Close()
	file.Close()

	err = os.Rename(filename, filename+".bck")
	if err != nil {
		return err
	}

	err = os.Rename(filename+".tmp", filename)
	if err != nil {
		//Try and rename it back
		undoError := os.Rename(filename+".bck", filename)
		if undoError != nil {
			log.Fatal("Tried to Compact Database and it went bad, real bad")
		}
		return err
	}

	err = os.Remove(filename + ".bck")
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) MakePrivateCollection(compare gkvlite.KeyCompare) *Collection {
	collection := s.Store.MakePrivateCollection(compare)
	wrappedCollection := Collection{}
	wrappedCollection.Collection = collection
	wrappedCollection.Channel = s.channel
	return &wrappedCollection
}

func (s *Store) SetCollection(name string, compare gkvlite.KeyCompare) *Collection {
	collection := s.Store.SetCollection(name, compare)
	wrappedCollection := Collection{}
	wrappedCollection.Collection = collection
	wrappedCollection.Channel = s.channel
	return &wrappedCollection
}

func (s *Store) GetCollection(name string) *Collection {
	collection := s.Store.GetCollection(name)
	if collection == nil {
		return nil
	} else {
		wrappedCollection := Collection{}
		wrappedCollection.Collection = collection
		wrappedCollection.Channel = s.channel
		return &wrappedCollection
	}
}

/*
 * Name: Name of collection
 * Returned Bool: True if the collection has been created.
 * Returned Collection: The Collection Wrapper.
 */
func (s *Store) GetCollectionCreateEmpty(name string) (bool, *Collection) {
	collection := s.GetCollection(name)

	if collection == nil {
		collection = s.SetCollection(name, nil)
		return true, collection
	} else {
		return false, collection
	}
}

func (s *Store) Flush() error {
	err := s.Store.Flush()
	if err != nil {
		return err
	}
	return s.file.Sync()
}

func (s *Store) ListenAndFlush() error {
	for {
		<-s.channel
		err := s.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Close() error {
	err := s.Flush()
	if err != nil {
		return err
	}

	s.Store.Close()
	err = s.file.Close()

	s.file = nil

	delete(storePool, s.file.Name())
	return err
}

func init() {
	storePool = make(map[string]*Store)
}
