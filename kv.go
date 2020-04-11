package simplekv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/cespare/xxhash"
)

const (
	newStoreDefaultPerms = 0644

	headerMagic = uint32(0x54574548)
	recordMagic = uint32(0x45485457)
	version     = uint16(1)

	maxTX = ^uint64(0)

	//FlagRecordsCompressed Records are compressed
	FlagRecordsCompressed = 0x1
)

type storeBackend interface {
	io.ReadCloser
	io.WriteSeeker
	io.ReaderAt
	io.WriterAt
}

//Store basic store
type Store struct {
	backend storeBackend

	mu sync.RWMutex

	version uint16
	flags   uint16
	count   uint64
	rCount  uint64
	txCount uint64

	recs []*record

	index map[uint64]*record
}

//NewStore creates or opens a store given a file filename
func NewStore(filename string) (*Store, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, newStoreDefaultPerms)
	if err != nil {
		return nil, err
	}

	return NewStoreWithBackend(f)
}

//NewStoreWithBackend creates or opens a store from a given backend (e.g. a file on disk)
func NewStoreWithBackend(be storeBackend) (*Store, error) {
	store := &Store{
		backend: be,
		index:   map[uint64]*record{},
	}

	if !store.headerExists() {
		if err := store.updateHeader(); err != nil {
			return nil, err
		}
	}

	if err := store.reread(); err != nil {
		return nil, err
	}

	return store, nil
}

//Close closes the backend src and cleans records in mem
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.recs = []*record{}
	s.index = map[uint64]*record{}

	return s.backend.Close()
}

//IterKeyVal holds key values for the iter func
type IterKeyVal struct {
	Key   []byte
	Value []byte
	Tx    uint64
}

//Iter iterates over all keys
func (s *Store) Iter() <-chan IterKeyVal {
	s.mu.RLock()
	defer s.mu.RUnlock()

	recCh := make(chan IterKeyVal)

	go func() {
		for _, rec := range s.recs {
			if rec.ts != 0 {
				continue
			}
			val, err := rec.value()
			if err != nil {
				close(recCh)
				return
			}

			iterkv := IterKeyVal{
				Key:   rec.Key,
				Value: val,
				Tx:    rec.tx,
			}

			recCh <- iterkv
		}
		close(recCh)
	}()

	return recCh
}

//Get retreives the value for a given key
func (s *Store) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if rec := s.recByKey(key, maxTX); rec != nil {
		return rec.value()
	}

	return nil, fmt.Errorf("key not found")
}

func (s *Store) recByKey(key []byte, mtx uint64) *record {
	if rec, ok := s.index[hashKey(key)]; ok {
		if bytes.Equal(rec.Key, key) && rec.ts == 0 && rec.tx <= mtx {
			return rec
		}
	}

	for _, rec := range s.recs {
		if bytes.Equal(rec.Key, key) && rec.ts == 0 && rec.tx <= mtx {
			return rec
		}
	}

	return nil
}

//HasKey checks if a key exists
func (s *Store) HasKey(key []byte) bool {
	b := s.recByKey(key, maxTX)
	return b != nil
}

//Add adds a new key with given value
func (s *Store) Add(key []byte, value []byte) error {
	if s.HasKey(key) {
		return fmt.Errorf("key already exists")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.addRec(key, value)

	s.count++
	s.rCount++
	s.txCount++

	return s.updateHeader()
}

//Delete dletes a key
func (s *Store) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := s.recByKey(key, maxTX)
	if r == nil {
		return fmt.Errorf("key not found")
	}

	ts := time.Now().Unix()

	s.delRec(r, ts)
	delete(s.index, hashKey(key))

	s.txCount++
	s.count--

	return s.updateHeader()
}

//Update updates an existing key
func (s *Store) Update(key []byte, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := s.recByKey(key, maxTX)
	if r == nil {
		return fmt.Errorf("key not found")
	}

	ts := time.Now().Unix()

	s.delRec(r, ts)
	s.addRec(key, value)

	s.txCount++
	s.rCount++

	return s.updateHeader()
}

//Clean removes old tombstoned records
func (s *Store) Clean() error {
	sBackend, ok := s.backend.(*os.File)
	if !ok {
		return fmt.Errorf("unsupported cleaning backend")
	}

	tmpBackend, err := ioutil.TempFile(os.TempDir(), "cleanup_simplekv_*")
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tmpStore := &Store{
		backend: tmpBackend,
	}

	//Write initial header
	if err := tmpStore.updateHeader(); err != nil {
		return err
	}

	cutoff := time.Now().Add(-5 * time.Minute).Unix()

	val := []byte{}

	for _, rec := range s.recs {
		if rec.ts >= cutoff || rec.ts == 0 {
			val, _ = rec.value()
			tmpStore.addRecTxTs(rec.Key, val[:rec.valLen], rec.tx, rec.ts)
		}
	}

	//Close off
	if err := tmpStore.updateHeader(); err != nil {
		return err
	}
	tmpStore.backend.Close()

	//swap backends
	s.backend.Close()
	filename := sBackend.Name()
	os.Rename(filename, filename+"_")

	os.Rename(tmpBackend.Name(), filename)
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, newStoreDefaultPerms)
	if err != nil {
		os.Rename(filename+"_", filename)
		backF, _ := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, newStoreDefaultPerms)
		s.backend = backF
	}

	s.backend = f
	s.reread()

	return os.Remove(filename + "_")
}

func (s *Store) addRec(key []byte, value []byte) error {
	rec, err := s.addRecTxTs(key, value, s.txCount, 0)
	if err != nil {
		return err
	}

	s.recs = append(s.recs, rec)
	s.index[hashKey(key)] = rec

	return nil
}

func (s *Store) addRecTxTs(key []byte, value []byte, tx uint64, ts int64) (*record, error) {
	off, err := s.backend.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	rec := &record{
		off:   off,
		store: s,
		tx:    tx,
		ts:    ts,
		Key:   key,
	}

	rec.write(s.backend, value)

	return rec, nil
}

func (s *Store) delRec(r *record, ts int64) error {
	//Validate pos
	if ok, err := s.validateRecAtOffset(r); !ok || err != nil {
		return fmt.Errorf("could not found key to delete at offset")
	}

	tsBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(tsBytes, uint64(ts))

	if _, err := s.backend.WriteAt(tsBytes, r.off+4); err != nil {
		return err
	}

	//Update inner rec
	r.ts = ts

	return nil
}

func (s *Store) validateRecAtOffset(r *record) (bool, error) {
	recH := make([]byte, r.valOff)
	if _, err := s.backend.ReadAt(recH, r.off); err != nil {
		return false, err
	}

	recHMagic := binary.LittleEndian.Uint32(recH[0:4])

	if recHMagic != recordMagic {
		return false, fmt.Errorf("invalid magic")
	}

	recHKeyLen := binary.LittleEndian.Uint16(recH[20:22])
	recHKey := recH[22 : 22+recHKeyLen]

	if !bytes.Equal(r.Key, recHKey) {
		return false, nil
	}

	return true, nil
}

func (s *Store) headerExists() bool {
	fMagic := make([]byte, 4)

	if _, err := s.backend.ReadAt(fMagic, 0); err != nil {
		return false
	}

	return binary.LittleEndian.Uint32(fMagic) == headerMagic
}

func (s *Store) updateHeader() error {
	b := bytes.NewBuffer(nil)
	s.writeHeader(b)

	_, err := s.backend.WriteAt(b.Bytes(), 0)

	return err
}

func (s *Store) writeHeader(w io.Writer) {
	order := binary.LittleEndian

	binary.Write(w, order, headerMagic)
	binary.Write(w, order, s.version)
	binary.Write(w, order, s.flags)
	binary.Write(w, order, s.count)
	binary.Write(w, order, s.rCount)
	binary.Write(w, order, s.txCount)
}

func (s *Store) read(r io.Reader) error {
	if err := s.readHeader(r); err != nil {
		return err
	}

	s.recs = []*record{}
	s.index = map[uint64]*record{}

	off := s.recordStart()

	for {
		rec := &record{
			store: s,
			off:   off,
		}
		err := rec.read(r)
		if err == io.EOF {
			break
		}
		s.recs = append(s.recs, rec)
	}

	if uint64(len(s.recs)) != s.count {
		return fmt.Errorf("mismatch record header count")
	}

	return nil
}

func (s *Store) reread() error {
	return s.read(s.backend)
}

func (s *Store) readHeader(r io.Reader) error {
	order := binary.LittleEndian

	var magic uint32
	var version, flags uint16
	var count, rCount, txCount uint64

	binary.Read(r, order, &magic)

	if magic != headerMagic {
		return fmt.Errorf("Magic mismatch")
	}

	binary.Read(r, order, &version)
	binary.Read(r, order, &flags)
	binary.Read(r, order, &count)
	binary.Read(r, order, &rCount)
	binary.Read(r, order, &txCount)

	s.version = version
	s.flags = flags
	s.count = count
	s.rCount = rCount
	s.txCount = txCount

	return nil
}

func (s *Store) recordStart() int64 {
	return 4 + 2 + 2 + 8 + 8 + 8 //magic + version + flags + count + rCount + txCount
}

func hashKey(k []byte) uint64 {
	return xxhash.Sum64(k)
}
