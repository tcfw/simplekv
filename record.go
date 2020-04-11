package simplekv

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
)

//record atomic-unit of a record
type record struct {
	store *Store

	off    int64
	valOff int64

	Key []byte
	ts  int64
	tx  uint64

	valLen uint64
	crc    uint32
}

func (rec *record) value() ([]byte, error) {
	val := make([]byte, rec.valLen)

	n, err := rec.store.backend.ReadAt(val, int64(rec.off+rec.valOff))
	if err != nil {
		return nil, err
	}
	if n != int(rec.valLen) {
		return nil, fmt.Errorf("unexpected value len")
	}

	if rec.store.flags&FlagRecordsCompressed != 0 {
		val = decompressBytes(val)
	}

	return val, nil
}

func (rec *record) len() int {
	return int(rec.valOff + int64(rec.valLen) + 4) //add CRC
}

func (rec *record) write(w io.Writer, value []byte) {
	order := binary.LittleEndian

	binary.Write(w, order, recordMagic)

	//Meta
	binary.Write(w, order, rec.ts)
	binary.Write(w, order, rec.tx)

	//Key
	binary.Write(w, order, uint16(len(rec.Key)))
	binary.Write(w, order, rec.Key)

	//Value
	val := value
	if rec.store.flags&FlagRecordsCompressed != 0 {
		val = compressBytes(val)
	}
	binary.Write(w, order, uint64(len(val)))
	binary.Write(w, order, val)

	//CRC
	binary.Write(w, order, crc32.ChecksumIEEE(val))

	rec.valOff = int64(4 + 2 + uint64(len(rec.Key)) + 8 + 8 + 8) //magic + kLen + key + ts + tx + upTsLen + upTs + valLen
	rec.valLen = uint64(len(val))
}

func compressBytes(v []byte) []byte {
	var b bytes.Buffer
	gz, _ := flate.NewWriter(&b, flate.DefaultCompression)
	if _, err := gz.Write(v); err != nil {
		panic(err)
	}
	if err := gz.Flush(); err != nil {
		panic(err)
	}
	if err := gz.Close(); err != nil {
		panic(err)
	}
	return b.Bytes()
}

func decompressBytes(v []byte) []byte {
	inB := bytes.NewReader(v)
	gz := flate.NewReader(inB)
	defer gz.Close()

	buf, _ := ioutil.ReadAll(gz)

	return buf
}

func (rec *record) read(r io.Reader) error {
	order := binary.LittleEndian

	var kLen uint16
	var magic, crc uint32
	var tx, valLen uint64
	var ts int64

	//EOF catcher
	if err := binary.Read(r, order, &magic); err != nil {
		return err
	}

	if magic != recordMagic {
		return fmt.Errorf("Magic mismatch")
	}

	//Meta
	binary.Read(r, order, &ts)
	binary.Read(r, order, &tx)

	//Key
	binary.Read(r, order, &kLen)
	key := make([]byte, kLen)
	binary.Read(r, order, key)

	//Value (read only for checksum)
	binary.Read(r, order, &valLen)
	value := make([]byte, valLen)
	binary.Read(r, order, value)

	binary.Read(r, order, &crc)
	if crc != crc32.ChecksumIEEE(value) {
		return fmt.Errorf("CRC failed")
	}

	rec.Key = key
	rec.ts = ts
	rec.tx = tx
	rec.crc = crc

	rec.valLen = valLen
	rec.valOff = int64(4 + 2 + uint64(kLen) + 8 + 8 + 8) //magic + kLen + key + ts + tx + upTsLen + upTs + valLen

	return nil
}
