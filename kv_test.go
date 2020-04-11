package simplekv

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRWHeader(t *testing.T) {
	store := &Store{
		count:   123456,
		rCount:  654321,
		txCount: 3,
		version: version,
		flags:   FlagRecordsCompressed,
		index:   map[uint64]*record{},
	}

	buf := bytes.NewBuffer(nil)

	store.writeHeader(buf)
	t.Logf("%x", buf.Bytes())

	nStore := &Store{}

	nStore.readHeader(buf)

	assert.Equal(t, store.version, nStore.version, "version mismatch")
	assert.Equal(t, store.flags, nStore.flags, "flags mismatch")
	assert.Equal(t, store.count, nStore.count, "total count mismatch")
	assert.Equal(t, store.rCount, nStore.rCount, "record count mismatch")
	assert.Equal(t, store.txCount, nStore.txCount, "transaction count")
}

func TestRWRecord(t *testing.T) {
	rec := &record{
		store: &Store{},
		off:   0,
		Key:   []byte{0x1},
		tx:    1,
	}

	buf := bytes.NewBuffer(nil)
	rec.write(buf, []byte("Hello!"))

	nrec := &record{
		store: &Store{},
	}
	err := nrec.read(buf)
	assert.NoError(t, err)

	assert.Equal(t, rec.Key, nrec.Key, "key mismatch")
	assert.Equal(t, rec.tx, nrec.tx, "tx mismatch")
}

func TestStoreRecord(t *testing.T) {
	buf := bytes.NewBuffer(nil)

	store := &Store{
		count:   1,
		rCount:  1,
		txCount: 1,
		index:   map[uint64]*record{},
		flags:   FlagRecordsCompressed,
	}
	rec := &record{
		store: store,
		Key:   []byte{0x1},
		tx:    1,
	}

	store.writeHeader(buf)
	rec.write(buf, []byte("Hello!"))

	nStore := &Store{}
	nStore.read(buf)

	assert.Len(t, nStore.recs, int(store.count))
	assert.Equal(t, rec.Key, nStore.recs[0].Key)
}

func TestRecordGet(t *testing.T) {
	buf, _ := ioutil.TempFile(os.TempDir(), "test_*")
	defer buf.Close()
	defer os.Remove(buf.Name())

	store := &Store{
		backend: buf,
		count:   1,
		rCount:  1,
		txCount: 1,
		index:   map[uint64]*record{},
		flags:   FlagRecordsCompressed,
	}
	rec := &record{
		store: store,
		Key:   []byte{0x1},
		tx:    1,
	}
	recVal := []byte("Hello!")
	store.writeHeader(buf)
	rec.write(buf, recVal)

	buf.Seek(0, io.SeekStart)

	err := store.read(buf)
	assert.NoError(t, err)

	val, err := store.Get([]byte{0x1})

	assert.NoError(t, err)
	assert.Equal(t, recVal, val)
}

func TestStoreAdd(t *testing.T) {
	buf, _ := ioutil.TempFile(os.TempDir(), "test_*")
	defer buf.Close()
	defer os.Remove(buf.Name())

	store := &Store{
		backend: buf,
		flags:   FlagRecordsCompressed,
		index:   map[uint64]*record{},
	}

	store.writeHeader(buf)

	key := []byte{0x1}
	val := []byte("Hello!")

	err := store.Add(key, val)

	assert.NoError(t, err)

	rVal, _ := store.Get(key)

	assert.Equal(t, val, rVal)
}

func TestStoreDel(t *testing.T) {
	buf, _ := ioutil.TempFile(os.TempDir(), "test_*")
	defer buf.Close()
	defer os.Remove(buf.Name())

	store := &Store{
		backend: buf,
		flags:   FlagRecordsCompressed,
		index:   map[uint64]*record{},
	}

	store.writeHeader(buf)

	key := []byte{0x1}
	val := []byte("Hello!")

	err := store.Add(key, val)
	assert.NoError(t, err)

	err = store.Delete(key)
	assert.NoError(t, err)

	b, err := store.Get(key)
	assert.Error(t, err)
	assert.Nil(t, b)

	//Reread whole DB
	store.read(buf)

	b, err = store.Get(key)
	assert.Error(t, err)
	assert.Nil(t, b)

	assert.Equal(t, uint64(0), store.count)
	assert.Equal(t, uint64(1), store.rCount)
	assert.Equal(t, uint64(2), store.txCount)
}

func TestStoreUpdate(t *testing.T) {
	buf, _ := ioutil.TempFile(os.TempDir(), "test_*")
	defer buf.Close()
	defer os.Remove(buf.Name())

	store := &Store{
		backend: buf,
		index:   map[uint64]*record{},
		flags:   FlagRecordsCompressed,
	}

	store.writeHeader(buf)

	key := []byte{0x1}
	val := []byte("Hello!")

	err := store.Add(key, val)
	assert.NoError(t, err)

	nVal := []byte("Goodbye")

	err = store.Update(key, nVal)
	assert.NoError(t, err)

	v, err := store.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, nVal, v)

	assert.Equal(t, uint64(1), store.count)
	assert.Equal(t, uint64(2), store.rCount)
	assert.Equal(t, uint64(2), store.txCount)
}

func TestStoreIter(t *testing.T) {
	buf, _ := ioutil.TempFile(os.TempDir(), "test_*")
	defer buf.Close()
	defer os.Remove(buf.Name())

	store := &Store{
		backend: buf,
		index:   map[uint64]*record{},
	}

	store.writeHeader(buf)

	store.Add([]byte{0x1}, []byte{0x1})
	store.Add([]byte{0x2}, []byte{0x1})

	ch := store.Iter()

	kv := <-ch
	assert.Equal(t, []byte{0x1}, kv.Key)
	assert.Equal(t, uint64(0), kv.Tx)

	kv = <-ch
	assert.Equal(t, []byte{0x2}, kv.Key)
	assert.Equal(t, uint64(1), kv.Tx)

	_, ok := <-ch
	assert.False(t, ok)
}

func TestNewStoreBackend(t *testing.T) {
	buf, _ := ioutil.TempFile(os.TempDir(), "test_*")
	defer buf.Close()
	defer os.Remove(buf.Name())

	store, err := NewStoreWithBackend(buf)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), store.count)
}

func TestStoreClean(t *testing.T) {
	buf, _ := ioutil.TempFile(os.TempDir(), "test_*")
	defer buf.Close()
	defer os.Remove(buf.Name())

	store, err := NewStoreWithBackend(buf)
	assert.NoError(t, err)

	//Add outdated tombstoned record
	r, _ := store.addRecTxTs([]byte{0x1}, []byte{0x1}, 0, time.Now().Add(-6*time.Minute).Unix())
	store.recs = append(store.recs, r)

	//Add tombstoned record but not outdated
	r, _ = store.addRecTxTs([]byte{0x2}, []byte{0x1}, 1, time.Now().Add(-4*time.Minute).Unix())
	store.recs = append(store.recs, r)

	//Add active record
	r, _ = store.addRecTxTs([]byte{0x2}, []byte{0x1}, 2, 0)
	store.recs = append(store.recs, r)

	err = store.Clean()
	assert.NoError(t, err)

	//count new records
	assert.Len(t, store.recs, 2)
}

func BenchmarkStoreAdd(b *testing.B) {
	buf, _ := ioutil.TempFile(os.TempDir(), "test_*")
	defer buf.Close()
	defer os.Remove(buf.Name())

	store := &Store{
		backend: buf,
		index:   map[uint64]*record{},
	}

	b.Run("add", func(b *testing.B) {
		k := make([]byte, 4)
		vals := []byte{0x1, 0x2, 0x3, 0x4}
		for i := 0; i < b.N; i++ {
			binary.LittleEndian.PutUint32(k, uint32(i))
			store.Add(k, vals)
		}
	})

	b.Run("del", func(b *testing.B) {
		k := make([]byte, 4)
		for i := 0; i < b.N; i++ {
			binary.LittleEndian.PutUint32(k, uint32(i))
			store.Delete(k)
		}
	})

	updateKey := []byte{0x99, 0x88, 0x77}
	store.Add(updateKey, []byte{0x1})

	b.Run("update", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			store.Update(updateKey, []byte{0x2})
		}
	})

	b.Run("clean", func(b *testing.B) {
		store.Clean()
	})
}

func BenchmarkFindAndSeek(b *testing.B) {
	tmpFile, _ := ioutil.TempFile(os.TempDir(), "test_*")
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	store, _ := NewStoreWithBackend(tmpFile)

	b.StopTimer()

	maxK := 50000
	val := make([]byte, 20480)
	rand.Read(val)

	b.Log("Adding test records...")

	for k := 0; k < maxK; k++ {
		kBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(kBytes, uint32(k))
		if err := store.Add(kBytes, val); err != nil {
			b.Fatalf("%s: %x", err, k)
		}
	}

	b.StartTimer()

	b.Run("rand-read", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			kBytes := make([]byte, 4)
			k := rand.Int31n(int32(maxK))
			binary.LittleEndian.PutUint32(kBytes, uint32(k))
			_, _ = store.Get(kBytes)
		}
	})
}
