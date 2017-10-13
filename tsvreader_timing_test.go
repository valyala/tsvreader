package tsvreader

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func BenchmarkReaderBytes(b *testing.B) {
	for _, rows := range []int{100, 1e3, 1e4} {
		for _, cols := range []int{1, 10, 100} {
			name := fmt.Sprintf("%d_%d", rows, cols)
			b.Run(name, func(b *testing.B) {
				benchmarkReaderBytes(b, rows, cols)
			})
		}
	}
}

func benchmarkReaderBytes(b *testing.B, rows, cols int) {
	b.StopTimer()
	bb := createBytesTSV(rows, cols)
	br := bytes.NewReader(bb)
	r := New(br)
	b.StartTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchmarkReaderBytesSingleIter(b, r, rows, cols)
		br.Reset(bb)
		r.Reset(br)
	}
}

func benchmarkReaderBytesSingleIter(b *testing.B, r *Reader, rows, cols int) {
	for i := 0; i < rows; i++ {
		if !r.Next() {
			b.Fatalf("Reader.Next must return true on row #%d", i+1)
		}
		for j := 0; j < cols; j++ {
			bb := r.Bytes()
			if len(bb) == 0 {
				b.Fatalf("expecting non-empty bytes on row #%d, col #%d", i+1, j+1)
			}
		}
	}
}

func BenchmarkReaderInt(b *testing.B) {
	for _, rows := range []int{100, 1e3, 1e4} {
		for _, cols := range []int{1, 10, 100} {
			name := fmt.Sprintf("%d_%d", rows, cols)
			b.Run(name, func(b *testing.B) {
				benchmarkReaderInt(b, rows, cols)
			})
		}
	}
}

func benchmarkReaderInt(b *testing.B, rows, cols int) {
	b.StopTimer()
	bb := createIntTSV(rows, cols)
	br := bytes.NewReader(bb)
	r := New(br)
	b.StartTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchmarkReaderIntSingleIter(b, r, rows, cols)
		br.Reset(bb)
		r.Reset(br)
	}
}

func benchmarkReaderIntSingleIter(b *testing.B, r *Reader, rows, cols int) {
	for i := 0; i < rows; i++ {
		if !r.Next() {
			b.Fatalf("Reader.Next must return true on row #%d", i+1)
		}
		for j := 0; j < cols; j++ {
			n := r.Int()
			if n == 0 {
				b.Fatalf("expecting non-zero int on row #%d, col #%d", i+1, j+1)
			}
		}
	}
}

func BenchmarkReaderUint(b *testing.B) {
	for _, rows := range []int{100, 1e3, 1e4} {
		for _, cols := range []int{1, 10, 100} {
			name := fmt.Sprintf("%d_%d", rows, cols)
			b.Run(name, func(b *testing.B) {
				benchmarkReaderUint(b, rows, cols)
			})
		}
	}
}

func benchmarkReaderUint(b *testing.B, rows, cols int) {
	b.StopTimer()
	bb := createUintTSV(rows, cols)
	br := bytes.NewReader(bb)
	r := New(br)
	b.StartTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchmarkReaderUintSingleIter(b, r, rows, cols)
		br.Reset(bb)
		r.Reset(br)
	}
}

func benchmarkReaderUintSingleIter(b *testing.B, r *Reader, rows, cols int) {
	for i := 0; i < rows; i++ {
		if !r.Next() {
			b.Fatalf("Reader.Next must return true on row #%d", i+1)
		}
		for j := 0; j < cols; j++ {
			n := r.Uint()
			if n == 0 {
				b.Fatalf("expecting non-zero uint on row #%d, col #%d", i+1, j+1)
			}
		}
	}
}

func createBytesTSV(rows, cols int) []byte {
	var bb bytes.Buffer
	for i := 0; i < rows; i++ {
		var ss []string
		for j := 0; j < cols; j++ {
			s := fmt.Sprintf("cell %d %d", i, j)
			ss = append(ss, s)
		}
		fmt.Fprintf(&bb, "%s\n", strings.Join(ss, "\t"))
	}
	return bb.Bytes()
}

func createIntTSV(rows, cols int) []byte {
	return createUintTSV(rows, cols)
}

func createUintTSV(rows, cols int) []byte {
	var bb bytes.Buffer
	for i := 0; i < rows; i++ {
		var ss []string
		for j := 0; j < cols; j++ {
			s := fmt.Sprintf("%d", i*cols+j+1)
			ss = append(ss, s)
		}
		fmt.Fprintf(&bb, "%s\n", strings.Join(ss, "\t"))
	}
	return bb.Bytes()
}
