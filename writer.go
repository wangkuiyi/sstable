// Package sstable implements the reading and write of SSTable files.
//
// SSTable file format is defined here:
// https://www.igvita.com/2012/02/06/sstable-and-log-structured-storage-leveldb/
// In particular, a basic SSTable file contains a sequence of records,
// where each record consists of:
//
//  1. key-size in 4 bytes of little-endian uint32
//  2. key
//  3. value-size in 4 bytes of little-endian uint32
//  4. value
//
// An SSTable file can optionally has the index data structured
// appended to records:
//
//  1. a separate of 4 bytes of little-endian encoding of 0xffffffff
//  2. number of unique keys as 4 bytes of little-endian encoding
//  3. a sequence of key and offsets of values of that key:
//     1. key-size in 4 bytes of little-endian uint32
//     2. key
//     3. number of records of that key in 4 bytes of little-endian uint32
//     4. offsets in the SSTable file of all these records
//  4. index-end flag as
//     1. 4 bytes of little-endiean encoding of 0xffffffff
//     2. 8 bytes of the offset of the index.
//
// Note that the following are all valid SSTable files:
//
//  1. records
//  2. records + separator 0xffffffff
//  3. records + separator 0xffffffff + index + index-end 0xffffffff
//
package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	separator        uint32 = 0xffffffff
	indexEndFlagSize int64  = 12 // 0xffffff and the 8-byte offset
)

// Writer constructs an SSTable file.  It is not thread-safe.
type Writer struct {
	io.Writer
	index  map[string][]int64
	offset int64
}

func Create(w io.Writer) *Writer {
	return &Writer{
		Writer: w,
		index:  make(map[string][]int64),
		offset: 0}
}

func (ss *Writer) Put(key, value string) error {
	// Pack key-size, key, value-size, value into buf, so we can
	// write buf as a transaction into the SSTable file.
	var buf bytes.Buffer

	if e := writeUint32(&buf, len(key)); e != nil {
		return fmt.Errorf("Failed to write key size: %v", e)
	}

	if _, e := buf.Write([]byte(key)); e != nil {
		return fmt.Errorf("Failed to write key: %s", key)
	}

	if e := writeUint32(&buf, len(value)); e != nil {
		return fmt.Errorf("Failed to write value size: %v", e)
	}

	if _, e := buf.Write([]byte(value)); e != nil {
		return fmt.Errorf("Failed to write value: %v", e)
	}

	if _, e := ss.Write(buf.Bytes()); e != nil {
		return fmt.Errorf("Failed to write key-value pair (%s, %s) to SSTable: %v", key, value, e)
	}

	ss.index[key] = append(ss.index[key], ss.offset)
	ss.offset += int64(buf.Len())
	return nil
}

func (ss *Writer) WriteIndex() error {
	if e := writeUint32(ss, int(separator)); e != nil {
		return fmt.Errorf("Failed to write the separator: %v", e)
	}

	if e := writeUint32(ss, len(ss.index)); e != nil {
		return fmt.Errorf("Failed to write the number of unique keys: %v", e)
	}

	for k, s := range ss.index {
		if e := writeUint32(ss, len(k)); e != nil {
			return fmt.Errorf("Failed to write key size in index: %v", e)
		}
		if _, e := ss.Write([]byte(k)); e != nil {
			return fmt.Errorf("Failed to write key in index: %v", e)
		}
		if e := writeUint32(ss, len(s)); e != nil {
			return fmt.Errorf("Failed to write number of values of key %s: %v", k, e)
		}
		for _, o := range s {
			if e := writeUint64(ss, o); e != nil {
				return fmt.Errorf("Failed to write offset of key %s: %v", k, e)
			}
		}
	}

	if e := writeUint32(ss, int(separator)); e != nil {
		return fmt.Errorf("Failed to write index-end flag: %v", e)
	}
	if e := writeUint64(ss, ss.offset); e != nil {
		return fmt.Errorf("Failed to write index offset: %v", e)
	}

	// Note: No more content can be added once we wrote the index.
	ss.Writer = nil
	return nil
}

func writeUint32(w io.Writer, value int) error {
	var bs [4]byte
	binary.LittleEndian.PutUint32(bs[:], uint32(value))
	if _, e := w.Write(bs[:]); e != nil {
		return e
	}
	return nil
}

func writeUint64(w io.Writer, value int64) error {
	var bs [8]byte
	binary.LittleEndian.PutUint64(bs[:], uint64(value))
	if _, e := w.Write(bs[:]); e != nil {
		return e
	}
	return nil
}
