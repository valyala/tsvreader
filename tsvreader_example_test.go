package tsvreader_test

import (
	"bytes"
	"fmt"
	"github.com/valyala/tsvreader"
)

func ExampleReader() {
	bs := bytes.NewBufferString(
		"foo\t42\n" +
			"bar\t123\n")

	r := tsvreader.New(bs)
	for r.Next() {
		col1 := r.String()
		col2 := r.Int()
		fmt.Printf("col1=%s, col2=%d\n", col1, col2)
	}
	if err := r.Error(); err != nil {
		fmt.Printf("unexpected error: %s", err)
	}

	// Output:
	// col1=foo, col2=42
	// col1=bar, col2=123
}

func ExampleReader_HasCols() {
	bs := bytes.NewBufferString(
		"foo\n" +
			"bar\tbaz\n" +
			"\n" +
			"a\tb\tc\n")

	r := tsvreader.New(bs)
	for r.Next() {
		for r.HasCols() {
			s := r.String()
			fmt.Printf("%q,", s)
		}
		fmt.Printf("\n")
	}
	if err := r.Error(); err != nil {
		fmt.Printf("unexpected error: %s", err)
	}

	// Output:
	// "foo",
	// "bar","baz",
	//
	// "a","b","c",
}

func ExampleReader_Next() {
	bs := bytes.NewBufferString("1\n2\n3\n42\n")

	r := tsvreader.New(bs)
	for r.Next() {
		n := r.Int()
		fmt.Printf("%d\n", n)
	}
	if err := r.Error(); err != nil {
		fmt.Printf("unexpected error: %s", err)
	}

	// Output:
	// 1
	// 2
	// 3
	// 42
}
