package sstable

import (
	"bytes"
	"fmt"
	"testing"
)

func TestSSTableCreation(t *testing.T) {
	var buf bytes.Buffer
	w := Create(&buf)
	fmt.Println(buf.Len())

	w.Put("", "")
	fmt.Println(buf.Len())
	fmt.Println(w.index)

	w.Put("", "")
	fmt.Println(buf.Len())
	fmt.Println(w.index)

	w.Put("apple", "pie")
	fmt.Println(buf.Len())
	fmt.Println(w.index)

	w.Put("apple", "imac")
	fmt.Println(buf.Len())
	fmt.Println(w.index)

	w.WriteIndex()
	fmt.Println(buf.Len())

	br := bytes.NewReader(buf.Bytes())
	r, e := Open(br)
	fmt.Println(r, e)
}
