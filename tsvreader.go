package tsvreader

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"strconv"
	"unsafe"
)

// New returns new Reader that reads TSV data from r.
func New(r io.Reader) *Reader {
	return &Reader{
		br: bufio.NewReader(r),
	}
}

// Reader reads tab-separated data.
//
// Call New for creating new TSV reader.
//
// It is expected that columns are separated by tabs while rows
// are separated by newlines.
type Reader struct {
	br *bufio.Reader

	col int
	row int

	rowBuf  []byte
	b       []byte
	scratch []byte

	err error
}

// Reset resets the reader for reading from r.
func (tr *Reader) Reset(r io.Reader) {
	if tr.br == nil {
		tr.br = bufio.NewReader(r)
	} else {
		tr.br.Reset(r)
	}

	tr.col = 0
	tr.row = 0

	tr.rowBuf = nil
	tr.b = nil
	tr.scratch = tr.scratch[:0]

	tr.err = nil
}

// Error returns the last error.
func (tr *Reader) Error() error {
	if tr.err == io.EOF {
		return nil
	}
	return tr.err
}

// Next advances to the next row.
//
// Returns true if the next row does exist.
//
// Next must be called after reading all the columns on the previous row.
// Check Error after Next returns false.
func (tr *Reader) Next() bool {
	if tr.err != nil {
		return false
	}
	if tr.b != nil {
		tr.err = fmt.Errorf("row #%d %q contains unread columns: %q", tr.row, tr.rowBuf, tr.b)
		return false
	}

	tr.row++
	tr.col = 0
	tr.rowBuf = nil

	for {
		b, err := tr.br.ReadSlice('\n')
		if err == nil {
			if len(tr.scratch) > 0 {
				tr.scratch = append(tr.scratch, b...)
				b = tr.scratch
				tr.scratch = tr.scratch[:0]
			}
			tr.rowBuf = b[:len(b)-1]
			tr.b = tr.rowBuf
			return true
		}

		if err != bufio.ErrBufferFull {
			if err != io.EOF {
				tr.err = fmt.Errorf("cannot read row #%d: %s", tr.row, err)
			} else if len(b) == 0 && len(tr.scratch) == 0 {
				tr.err = io.EOF
			} else {
				tr.scratch = append(tr.scratch, b...)
				tr.err = fmt.Errorf("cannot find newline at the end of row #%d; row: %q", tr.row, tr.scratch)
			}
			return false
		}

		tr.scratch = append(tr.scratch, b...)
	}
}

// Int returns the next int column in the current row.
func (tr *Reader) Int() int {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `int`", err)
		return 0
	}

	n, err := strconv.Atoi(b2s(b))
	if err != nil {
		tr.setColError("cannot parse `int`", err)
		return 0
	}
	return n
}

// Uint returns the next uint column in the current row.
func (tr *Reader) Uint() uint {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `uint`", err)
		return 0
	}
	s := b2s(b)

	// Fast path - attempt to use Atoi
	n, err := strconv.Atoi(s)
	if err == nil && n >= 0 {
		return uint(n)
	}

	// Slow path - use ParseUint
	nu, err := strconv.ParseUint(s, 10, strconv.IntSize)
	if err != nil {
		tr.setColError("cannot parse `uint`", err)
		return 0
	}
	return uint(nu)
}

// Int32 returns the next int32 column in the current row.
func (tr *Reader) Int32() int32 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `int32`", err)
		return 0
	}
	s := b2s(b)

	// Fast path - attempt to use Atoi
	n, err := strconv.Atoi(s)
	if err == nil && n >= math.MinInt32 && n <= math.MaxInt32 {
		return int32(n)
	}

	// Slow path - use ParseInt
	n32, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		tr.setColError("cannot parse `int32`", err)
		return 0
	}
	return int32(n32)
}

// Uint32 returns the next uint32 column in the current row.
func (tr *Reader) Uint32() uint32 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `uint32`", err)
		return 0
	}
	s := b2s(b)

	// Fast path - attempt to use Atoi
	n, err := strconv.Atoi(s)
	if err == nil && n >= 0 && n <= math.MaxUint32 {
		return uint32(n)
	}

	// Slow path - use ParseUint
	n32, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		tr.setColError("cannot parse `uint32`", err)
		return 0
	}
	return uint32(n32)
}

// Int64 returns the next int64 column in the current row.
func (tr *Reader) Int64() int64 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `int64`", err)
		return 0
	}
	s := b2s(b)

	// Fast path - attempt to use Atoi
	n, err := strconv.Atoi(s)
	if err == nil && int64(n) >= math.MinInt64 && int64(n) <= math.MaxInt64 {
		return int64(n)
	}

	// Slow path - use ParseInt
	n64, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		tr.setColError("cannot parse `int64`", err)
		return 0
	}
	return n64
}

// Uint64 returns the next uint64 column in the current row.
func (tr *Reader) Uint64() uint64 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `uint64`", err)
		return 0
	}
	s := b2s(b)

	// Fast path - attempt to use Atoi
	n, err := strconv.Atoi(s)
	if err == nil && n >= 0 && uint64(n) <= math.MaxUint64 {
		return uint64(n)
	}

	// Slow path - use ParseUint
	n64, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		tr.setColError("cannot parse `uint64`", err)
		return 0
	}
	return n64
}

// Float32 returns the next float32 column in the current row.
func (tr *Reader) Float32() float32 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `float32`", err)
		return 0
	}
	s := b2s(b)

	f32, err := strconv.ParseFloat(s, 32)
	if err != nil {
		tr.setColError("cannot parse `float32`", err)
		return 0
	}
	return float32(f32)
}

// Float64 returns the next float64 column in the current row.
func (tr *Reader) Float64() float64 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `float64`", err)
		return 0
	}
	s := b2s(b)

	f64, err := strconv.ParseFloat(s, 64)
	if err != nil {
		tr.setColError("cannot parse `float64`", err)
		return 0
	}
	return f64
}

// Bytes returns the next bytes column in the current row.
//
// The returned value is valid until the next call to Reader.
func (tr *Reader) Bytes() []byte {
	if tr.err != nil {
		return nil
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `bytes`", err)
		return nil
	}
	return b
}

func (tr *Reader) nextCol() ([]byte, error) {
	if tr.b == nil {
		return nil, fmt.Errorf("no more columns")
	}

	tr.col++
	n := bytes.IndexByte(tr.b, '\t')
	if n < 0 {
		// last column
		b := tr.b
		tr.b = nil
		return b, nil
	}

	b := tr.b[:n]
	tr.b = tr.b[n+1:]
	return b, nil
}

func (tr *Reader) setColError(msg string, err error) {
	tr.err = fmt.Errorf("%s at row #%d, col #%d %q: %s", msg, tr.row, tr.col, tr.rowBuf, err)
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
