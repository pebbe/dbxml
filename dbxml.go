/*
A basic Go interface to Oracle Berkeley DB XML.

http://www.oracle.com/us/products/database/berkeley-db/xml/index.html
*/
package dbxml

//. Imports

/*
#cgo LDFLAGS: -ldbxml
#include <stdlib.h>
#include "c_dbxml.h"
*/
import "C"

import (
	"errors"
	"runtime"
	"sync"
	"unsafe"
)

//. Types

// A database connection.
type Db struct {
	opened bool
	db     C.c_dbxml
	lock   sync.Mutex
	next   uint64
	docss  map[uint64]*Docs
}

// An iterator over xml documents in the database.
type Docs struct {
	db      *Db
	id      uint64
	started bool
	opened  bool
	docs    C.c_dbxml_docs
	lock    sync.Mutex
}

//. Variables

var (
	errclosed = errors.New("Database is closed")
)

//. Open & Close

// Open a database.
//
// Call db.Close() to ensure all write operations to the database are finished, before terminating the program.
func Open(filename string) (*Db, error) {
	db := &Db{}
	cs := C.CString(filename)
	defer C.free(unsafe.Pointer(cs))
	db.db = C.c_dbxml_open(cs)
	if C.c_dbxml_error(db.db) != 0 {
		err := errors.New(C.GoString(C.c_dbxml_errstring(db.db)))
		C.c_dbxml_free(db.db)
		return db, err
	}
	db.docss = make(map[uint64]*Docs)
	db.opened = true
	runtime.SetFinalizer(db, (*Db).Close)
	return db, nil
}

// Close the database.
//
// This flushes all write operations to the database.
func (db *Db) Close() {
	db.lock.Lock()
	defer db.lock.Unlock()
	if db.opened {
		for id := uint64(0); id < db.next; id++ {
			if _, ok := db.docss[id]; ok {
				db.docss[id].Close()
			}
		}
		C.c_dbxml_free(db.db)
		db.opened = false
	}
}

//. Write

// Put an xml file from disc into the database.
func (db *Db) PutFile(filename string, replace bool) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if !db.opened {
		return errclosed
	}

	cs := C.CString(filename)
	defer C.free(unsafe.Pointer(cs))
	repl := C.int(0)
	if replace {
		repl = 1
	}
	if C.c_dbxml_put_file(db.db, cs, repl) == 0 {
		return errors.New(C.GoString(C.c_dbxml_errstring(db.db)))
	}
	return nil
}

// Put an xml document from memory into the database.
func (db *Db) PutXml(name string, data string, replace bool) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if !db.opened {
		return errclosed
	}

	csname := C.CString(name)
	defer C.free(unsafe.Pointer(csname))
	csdata := C.CString(data)
	defer C.free(unsafe.Pointer(csdata))
	repl := C.int(0)
	if replace {
		repl = 1
	}
	if C.c_dbxml_put_xml(db.db, csname, csdata, repl) == 0 {
		return errors.New(C.GoString(C.c_dbxml_errstring(db.db)))
	}
	return nil
}

// Merge a database from disc into this database.
func (db *Db) Merge(filename string, replace bool) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if !db.opened {
		return errclosed
	}

	cs := C.CString(filename)
	defer C.free(unsafe.Pointer(cs))
	repl := C.int(0)
	if replace {
		repl = 1
	}
	if C.c_dbxml_merge(db.db, cs, repl) == 0 {
		return errors.New(C.GoString(C.c_dbxml_errstring(db.db)))
	}
	return nil
}

// Remove an xml document from the database.
func (db *Db) Remove(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if !db.opened {
		return errclosed
	}

	cs := C.CString(name)
	defer C.free(unsafe.Pointer(cs))
	if C.c_dbxml_remove(db.db, cs) == 0 {
		return errors.New(C.GoString(C.c_dbxml_errstring(db.db)))
	}
	return nil
}

//. Read

// Get an xml document by name from the database.
func (db *Db) Get(name string) (string, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if !db.opened {
		return "", errclosed
	}

	cs := C.CString(name)
	defer C.free(unsafe.Pointer(cs))
	s := C.GoString(C.c_dbxml_get(db.db, cs))
	if C.c_dbxml_error(db.db) != 0 {
		return "", errors.New(s)
	}
	return s, nil
}

// Get the number of xml documents in the database.
func (db *Db) Size() (uint64, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	if !db.opened {
		return 0, errclosed
	}
	return uint64(C.c_dbxml_size(db.db)), nil
}

// Get all xml documents from the database.
//
// Example:
//
//      docs, err := db.All()
//      if err != nil {
//          fmt.Println(err)
//      } else {
//          for docs.Next() {
//              fmt.Println(docs.Name(), docs.Content())
//          }
//      }
func (db *Db) All() (*Docs, error) {
	docs := &Docs{}
	db.lock.Lock()
	defer db.lock.Unlock()

	if !db.opened {
		return docs, errclosed
	}
	docs.docs = C.c_dbxml_get_all(db.db)
	docs.db = db
	docs.id = db.next
	db.next++
	db.docss[docs.id] = docs
	runtime.SetFinalizer(docs, (*Docs).Close)
	docs.opened = true
	return docs, nil
}

// Get all xml documents that match the XPATH query from the database.
//
// Example:
//
//      docs, err := db.Query(xpath_query)
//      if err != nil {
//          fmt.Println(err)
//      } else {
//          for docs.Next() {
//              fmt.Println(docs.Name(), docs.Content())
//          }
//      }
func (db *Db) Query(query string) (*Docs, error) {
	docs := &Docs{}
	db.lock.Lock()
	defer db.lock.Unlock()

	if !db.opened {
		return docs, errclosed
	}
	cs := C.CString(query)
	defer C.free(unsafe.Pointer(cs))
	docs.docs = C.c_dbxml_get_query(db.db, cs)
	if C.c_dbxml_error(db.db) != 0 {
		err := errors.New(C.GoString(C.c_dbxml_errstring(db.db)))
		return docs, err
	}
	docs.db = db
	docs.id = db.next
	db.next++
	db.docss[docs.id] = docs
	runtime.SetFinalizer(docs, (*Docs).Close)
	docs.opened = true
	return docs, nil
}

// Iterate to the next xml document in the list, that was returned by db.All() or db.Query(query).
func (docs *Docs) Next() bool {
	docs.lock.Lock()
	defer docs.lock.Unlock()
	if !docs.opened {
		return false
	}
	docs.started = true
	if C.c_dbxml_docs_next(docs.docs) == 0 {
		docs.close()
		return false
	}
	return true
}

// Get name of current xml document after call to docs.Next().
func (docs *Docs) Name() string {
	docs.lock.Lock()
	defer docs.lock.Unlock()
	if !(docs.opened && docs.started) {
		return ""
	}
	return C.GoString(C.c_dbxml_docs_name(docs.docs))
}

// Get content of current xml document after call to docs.Next().
func (docs *Docs) Content() string {
	docs.lock.Lock()
	defer docs.lock.Unlock()
	if !(docs.opened && docs.started) {
		return ""
	}
	return C.GoString(C.c_dbxml_docs_content(docs.docs))
}

// Close iterator over xml documents in the database, that was returned by db.All() or db.Query(query).
//
// This is called automaticly if docs.Next() reaches false, but you can also call it inside a loop to exit it prematurely:
//
//      docs, _ := db.All()
//      for docs.Next() {
//          fmt.Println(docs.Name(), docs.Content())
//          if some_condition {
//              docs.Close()
//          }
//      }
func (docs *Docs) Close() {
	docs.lock.Lock()
	defer docs.lock.Unlock()
	docs.close()
}

func (docs *Docs) close() {
	if docs.opened {
		C.c_dbxml_docs_free(docs.docs)
		docs.opened = false
		delete(docs.db.docss, docs.id)
		docs.db = nil // remove reference so the garbage collector can do its work
	}
}
