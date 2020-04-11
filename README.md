# simplekv
[<img class="badge" tag="github.com/tcfw/simplekv" src="https://goreportcard.com/badge/github.com/tcfw/simplekv">](https://goreportcard.com/report/github.com/tcfw/minidns)

A simple persistent key-value store

---
**IMPORTANT NOTE:** This is not production ready, just an experiment

---

### Example Usages
```go
package main

import (
	"fmt"
	"github.com/tcfw/simplekv"
)

func main() {
	store, _ := simplekv.NewStore("my_test.db")

	//Add a key
	store.Add([]byte("test_key"), []byte("my value"))

	//Delete a key
	store.Delete([]byte("test_key"))

	//Update a key
	store.Add([]byte("update_key"), []byte("my value"))
	store.Update([]byte("update_key"), []byte("my other value"))

	//Iterate over all keys
	for kv := range store.Iter() {
		fmt.Printf("Key: %s, Value: %s", kv.Key, kv.Value)
	}

	//Clean up deleted records
	store.Clean()
}
```