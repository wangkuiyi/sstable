package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
)

// Reader loads the index of an SSTable file into the memory.  If the
// file doesn't have an index, it scans the file and builds the index.
// After that, we can query key and values of a key.
type Reader struct {
	io.ReadSeeker
	index map[string][]int64
}

func Open(r io.ReadSeeker) (*Reader, error) {
	ss := &Reader{
		ReadSeeker: r,
		index:      make(map[string][]int64)}
	if e := ss.loadIndex(); e != nil {
		return nil, e
	}
	return ss, nil
}

func must(e error) {
	if e != nil {
		log.Fatalf("Fatal due to %v", e)
	}
}

func (ss *Reader) hasIndex() (uint64, error) {
	fileLength, e := ss.Seek(0, io.SeekEnd)
	must(e)

	if fileLength < indexEndFlagSize {
		return 0, fmt.Errorf("Too short to have index")
	}

	_, e = ss.Seek(-indexEndFlagSize, io.SeekEnd)
	must(e)
	sp, e := readUint32(ss)
	if e != nil {
		return 0, fmt.Errorf("Failed to seek index-end flag: %v", e)
	}
	if sp != separator {
		return 0, fmt.Errorf("Cannot find index-end flag: %v", e)
	}

	offset, e := readUint64(ss)
	must(e)
	return offset, nil
}

func (ss *Reader) loadIndex() error {
	offset, e := ss.hasIndex()
	fmt.Printf("Offset is %v", offset)
	return e
}

func readUint32(r io.Reader) (uint32, error) {
	var bs [4]byte
	_, e := r.Read(bs[:])
	if e != nil {
		return 0, e
	}
	return binary.LittleEndian.Uint32(bs[:]), nil
}

func readUint64(r io.Reader) (uint64, error) {
	var bs [8]byte
	_, e := r.Read(bs[:])
	if e != nil {
		return 0, e
	}
	return binary.LittleEndian.Uint64(bs[:]), nil
}
