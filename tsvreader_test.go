package tsvreader

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func TestReaderHasCols(t *testing.T) {
	b := bytes.NewBufferString("foo\t\n\n")
	r := New(b)
	if r.HasCols() {
		t.Fatalf("HasCols must return false before calling Next")
	}

	if !r.Next() {
		t.Fatalf("Next must return true")
	}
	bb := r.Bytes()
	if string(bb) != "foo" {
		t.Fatalf("unexpected bytes: %q. Expecting %q", bb, "foo")
	}
	if !r.HasCols() {
		t.Fatalf("HasCols must return true")
	}
	bb = r.Bytes()
	if len(bb) > 0 {
		t.Fatalf("unexpected non-empty bytes: %q", bb)
	}
	if r.HasCols() {
		t.Fatalf("HasCols must return false")
	}

	// An empty row is treated as it has a single empty column
	if !r.Next() {
		t.Fatalf("Next must return true")
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
	if r.HasCols() {
		t.Fatalf("HasCols must return false")
	}

	// No more rows
	if r.Next() {
		t.Fatalf("Next must return false")
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
	if r.HasCols() {
		t.Fatalf("HasCols must return false")
	}
}

func TestReaderResetError(t *testing.T) {
	b := bytes.NewBufferString("foo\tbar\n\nbaz\n")
	r := New(b)
	if !r.Next() {
		t.Fatalf("Next must return true")
	}
	bb := r.Bytes()
	if string(bb) != "foo" {
		t.Fatalf("unexpected bytes: %q. Expecting %q", bb, "foo")
	}
	bb = r.Bytes()
	if string(bb) != "bar" {
		t.Fatalf("unexpected bytes: %q. Expecting %q", bb, "bar")
	}

	// Attempt to read the second (empty) row
	if !r.Next() {
		t.Fatalf("Next must return true")
	}
	bb = r.Bytes()
	if string(bb) != "" {
		t.Fatalf("unexpected non-empty bytes: %q", bb)
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
	bb = r.Bytes()
	if string(bb) != "" {
		t.Fatalf("unexpected non-empty bytes: %q", bb)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting non-zero error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "no more columns") {
		t.Fatalf("unexpected error: %s. Expecting %q", errS, "no more columns")
	}

	r.ResetError()
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}

	// Read the last error
	if !r.Next() {
		t.Fatalf("Next must return true")
	}
	bb = r.Bytes()
	if string(bb) != "baz" {
		t.Fatalf("unexpected bytes: %q. Expecting %q", bb, "baz")
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
	if r.Next() {
		t.Fatalf("Next must return false")
	}
}

func TestReaderEmpty(t *testing.T) {
	b := bytes.NewBufferString("")
	r := New(b)
	if r.Next() {
		t.Fatalf("Next must return false on empty data")
	}
	err := r.Error()
	if err != nil {
		t.Fatalf("unexpected error after reading empty data: %s", err)
	}

	// Make sure r.Next() returns false on subsequent calls.
	for i := 0; i < 10; i++ {
		if r.Next() {
			t.Fatalf("Next must return false at the end of data")
		}
		err = r.Error()
		if err != nil {
			t.Fatalf("unexpected error at the end of data: %s", err)
		}
	}
}

func TestReaderNoNext(t *testing.T) {
	b := bytes.NewBufferString("aaa\n")
	r := New(b)

	n := r.Int()
	if n != 0 {
		t.Fatalf("unexpected non-zero int: %d", n)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting non-nil error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "missing Next call") {
		t.Fatalf("unexpected error: %s. Must contains %q", errS, "missing Next call")
	}
}

func TestReaderEmptyCol(t *testing.T) {
	b := bytes.NewBufferString("\t\tfoobar\t\n")
	r := New(b)
	if !r.Next() {
		t.Fatalf("Next must return true")
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}

	for i := 0; i < 4; i++ {
		bb := r.Bytes()
		if i == 2 {
			if string(bb) != "foobar" {
				t.Fatalf("unexpected bytes on col #%d: %q. Expecting %q", i+1, bb, "foobar")
			}
		} else if len(bb) != 0 {
			t.Fatalf("unexpected non-empty bytes on col #%d: %q", i+1, bb)
		}
		if r.Error() != nil {
			t.Fatalf("unexpected error on col #%d: %s", i+1, r.Error())
		}
	}
}

func TestReaderNoNewline(t *testing.T) {
	testReaderNoNewline(t, "foobar")
	testReaderNoNewline(t, "foo\t")
	testReaderNoNewline(t, "\t")
	testReaderNoNewline(t, "\tfoo\t\tbar")
	testReaderNoNewline(t, "\tfoo")
	testReaderNoNewline(t, "\tfoo\t")
	testReaderNoNewline(t, "foo\tbar")
	testReaderNoNewline(t, "foo\x00bar")
	testReaderNoNewline(t, "\x00")
}

func testReaderNoNewline(t *testing.T, s string) {
	t.Helper()

	b := bytes.NewBufferString(s)
	r := New(b)
	if r.Next() {
		t.Fatalf("Next must return false when no newline; s: %q", s)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting error when no newline; s: %q", s)
	}
	errS := err.Error()
	if !strings.Contains(errS, "cannot find newline") {
		t.Fatalf("unexpected error: %s; must contain %q", s, "cannot find newline")
	}

	// Make sure r.Next() returns false on subsequent calls.
	for i := 0; i < 10; i++ {
		if r.Next() {
			t.Fatalf("Next must return false after error; s: %q", s)
		}
		err1 := r.Error()
		if err1 != err {
			t.Fatalf("unexpected error: %v. Expecting %s; s: %q", err1, err, s)
		}
	}
}

func TestReaderReset(t *testing.T) {
	var r Reader

	for i := 0; i < 10; i++ {
		s := fmt.Sprintf("foobar %d\n", i)
		b := bytes.NewBufferString(s)
		r.Reset(b)
		if !r.Next() {
			t.Fatalf("Next must return true for TSV %q", s)
		}
		if r.Error() != nil {
			t.Fatalf("unexpected error before reading TSV %q: %s", s, r.Error())
		}
		bb := r.Bytes()
		if string(bb) != s[:len(s)-1] {
			t.Fatalf("unexpected bytes: %q. Expecting %q", bb, s[:len(s)-1])
		}
		if r.Error() != nil {
			t.Fatalf("unexpected error after reading TSV %q: %s", s, r.Error())
		}
	}
}

func TestReaderSingleRowBytesCol(t *testing.T) {
	expectedS := "foobar"
	b := bytes.NewBufferString(fmt.Sprintf("%s\n", expectedS))
	r := New(b)
	if !r.Next() {
		t.Fatalf("Next must return true on the first line. err: %v", r.Error())
	}
	err := r.Error()
	if err != nil {
		t.Fatalf("unexpected error after reading the first line: %s", err)
	}

	bb := r.Bytes()
	if string(bb) != expectedS {
		t.Fatalf("unexpected bytes read: %q. Expecting %q", bb, expectedS)
	}
	err = r.Error()
	if err != nil {
		t.Fatalf("unexpected error after reading the first col: %s", err)
	}

	// Attempt to read the next col, which doesn't exist.
	if r.Next() {
		t.Fatalf("Next must return false on a single row")
	}
	err = r.Error()
	if err != nil {
		t.Fatalf("unexpected error at the end of data: %s", err)
	}

	// Make sure r.Next() returns false on subsequent calls.
	for i := 0; i < 10; i++ {
		if r.Next() {
			t.Fatalf("Next must return false at the end of data")
		}
		if err != nil {
			t.Fatalf("unexpected error at the end of data: %s", err)
		}
	}
}

func TestReaderSingleRowIntCol(t *testing.T) {
	expectedN := 12346
	b := bytes.NewBufferString(fmt.Sprintf("%d\n", expectedN))
	r := New(b)
	if !r.Next() {
		t.Fatalf("Next must return true on the first line. err: %v", r.Error())
	}
	err := r.Error()
	if err != nil {
		t.Fatalf("unexpected error after reading the first line: %s", err)
	}

	n := r.Int()
	if n != expectedN {
		t.Fatalf("unexpected int read: %d. Expecting %d", n, expectedN)
	}
	err = r.Error()
	if err != nil {
		t.Fatalf("unexpected error after reading the first col: %s", err)
	}

	// Attempt to read the next col, which doesn't exist.
	if r.Next() {
		t.Fatalf("Next must return false on a single row")
	}
	err = r.Error()
	if err != nil {
		t.Fatalf("unexpected error at the end of data: %s", err)
	}

	// Make sure r.Next() returns false on subsequent calls.
	for i := 0; i < 10; i++ {
		if r.Next() {
			t.Fatalf("Next must return false at the end of data")
		}
		if err != nil {
			t.Fatalf("unexpected error at the end of data: %s", err)
		}
	}
}

func TestReaderInvalidColType(t *testing.T) {
	b := bytes.NewBufferString("foobar\n")
	r := New(b)
	if !r.Next() {
		t.Fatalf("Next must return true on the first line. err: %v", r.Error())
	}
	err := r.Error()
	if err != nil {
		t.Fatalf("unexpected error after reading the first line: %s", err)
	}

	n := r.Int()
	if n != 0 {
		t.Fatalf("unexpected n: %d. Expecting 0", n)
	}
	err = r.Error()
	if err == nil {
		t.Fatalf("expecting non-nil error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "cannot parse") {
		t.Fatalf("unexpected error: %s. Must contain %q", err, "cannot parse")
	}
}

func TestReaderNoMoreCols(t *testing.T) {
	b := bytes.NewBufferString("aaa\n")
	r := New(b)
	if !r.Next() {
		t.Fatalf("Next must return true on the first line. err: %v", r.Error())
	}
	err := r.Error()
	if err != nil {
		t.Fatalf("unexpected error after reading the first line: %s", err)
	}

	bb := r.Bytes()
	if string(bb) != "aaa" {
		t.Fatalf("unexpected bytes value: %q. Expecting %q", bb, "aaa")
	}
	err = r.Error()
	if err != nil {
		t.Fatalf("unexpected error after reading the first col: %s", err)
	}

	// attempt to read more col
	for i := 0; i < 10; i++ {
		bb := r.Bytes()
		if len(bb) > 0 {
			t.Fatalf("unexpected non-empty bytes: %q", bb)
		}
		err = r.Error()
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
		errS := err.Error()
		if !strings.Contains(errS, "no more columns") {
			t.Fatalf("unexpected error: %s. Must contain %q", err, "no more columns")
		}

		n := r.Int()
		if n != 0 {
			t.Fatalf("unexpected non-zero int: %d", n)
		}
		err = r.Error()
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
		errS = err.Error()
		if !strings.Contains(errS, "no more columns") {
			t.Fatalf("unexpected error: %s. Must contain %q", err, "no more columns")
		}
	}

	// atempt to read more rows
	for i := 0; i < 10; i++ {
		if r.Next() {
			t.Fatalf("Next must return false")
		}
		err = r.Error()
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
		errS := err.Error()
		if !strings.Contains(errS, "no more columns") {
			t.Fatalf("unexpected error: %s. Must contain %q", err, "no more columns")
		}
	}
}

func TestReaderSingleRowMultiCols(t *testing.T) {
	b := bytes.NewBufferString("foobar\t-42\t3\tbaz\n")
	r := New(b)

	if !r.Next() {
		t.Fatalf("Next must return true on the first line. err: %v", r.Error())
	}
	err := r.Error()
	if err != nil {
		t.Fatalf("unexpected error after reading the first line: %s", err)
	}

	bb := r.Bytes()
	if string(bb) != "foobar" {
		t.Fatalf("unexpected bytes: %q. Expecting %q", bb, "foobar")
	}
	err = r.Error()
	if err != nil {
		t.Fatalf("unexpected error after reading the first col: %s", err)
	}

	n := r.Int()
	if n != -42 {
		t.Fatalf("unexpected int: %d. Expecting %d", n, -42)
	}
	err = r.Error()
	if err != nil {
		t.Fatalf("unexpected error after reading the second col: %s", err)
	}

	n = r.Int()
	if n != 3 {
		t.Fatalf("unexpected int: %d. Expecting %d", n, 3)
	}
	err = r.Error()
	if err != nil {
		t.Fatalf("unexpected error after reading the third col: %s", err)
	}

	bb = r.Bytes()
	if string(bb) != "baz" {
		t.Fatalf("unexpected bytes: %q. Expecting %q", bb, "baz")
	}
	err = r.Error()
	if err != nil {
		t.Fatalf("unexpected error after reading the fourth col: %s", err)
	}

	// Attempt to read more rows
	for i := 0; i < 10; i++ {
		if r.Next() {
			t.Fatalf("Next must return false")
		}
		err = r.Error()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}
}

func TestReaderUnreadColsSingle(t *testing.T) {
	b := bytes.NewBufferString("foo\tbar\n")
	r := New(b)
	if !r.Next() {
		t.Fatalf("Next must return true")
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}

	bb := r.Bytes()
	if string(bb) != "foo" {
		t.Fatalf("unexpected bytes: %q. Expecting %q", bb, "foo")
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}

	// Attempt to read next row while the current row isnt read till the end
	for i := 0; i < 10; i++ {
		if r.Next() {
			t.Fatalf("Next must return false, because the previous row has unread columns")
		}
		err := r.Error()
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
		errS := err.Error()
		if !strings.Contains(errS, "unread columns") {
			t.Fatalf("unexpected error: %s. Must contain %q", err, "unread columns")
		}
	}
}

func TestReaderUnreadColsAll(t *testing.T) {
	b := bytes.NewBufferString("foo\tbar\n")
	r := New(b)
	if !r.Next() {
		t.Fatalf("Next must return true")
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}

	// Attempt to read next row while the current row isnt read till the end
	for i := 0; i < 10; i++ {
		if r.Next() {
			t.Fatalf("Next must return false, because the previous row has unread columns")
		}
		err := r.Error()
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
		errS := err.Error()
		if !strings.Contains(errS, "unread columns") {
			t.Fatalf("unexpected error: %s. Must contain %q", err, "unread columns")
		}
	}
}

func TestReaderMultiRowsBytesCol(t *testing.T) {
	testReaderMultiRowsBytesCol(t, 2)
	testReaderMultiRowsBytesCol(t, 10)
	testReaderMultiRowsBytesCol(t, 100)
	testReaderMultiRowsBytesCol(t, 1000)
	testReaderMultiRowsBytesCol(t, 10000)
}

func testReaderMultiRowsBytesCol(t *testing.T, rows int) {
	t.Helper()

	var expected []string
	var ss []string
	for i := 0; i < rows; i++ {
		s := fmt.Sprintf("foo%d bar", rand.Int())
		expected = append(expected, s)
		ss = append(ss, fmt.Sprintf("%s\n", s))
	}

	b := bytes.NewBufferString(strings.Join(ss, ""))
	r := New(b)
	for i, expectedS := range expected {
		if !r.Next() {
			t.Fatalf("Next must return true when reading %q at row #%d", expectedS, i+1)
		}
		if r.Error() != nil {
			t.Fatalf("unexpected error when reading %q at row #%d: %s", expectedS, i+1, r.Error())
		}
		bb := r.Bytes()
		if string(bb) != expectedS {
			t.Fatalf("unexpected bytes at row #%d: %q. Expecting %q", i+1, bb, expectedS)
		}
		if r.Error() != nil {
			t.Fatalf("unexpected error after reading %q at row #%d: %s", expectedS, i+1, r.Error())
		}
	}

	if r.Next() {
		t.Fatalf("Next must return false")
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
}

func TestReaderMultiRowsIntCol(t *testing.T) {
	testReaderMultiRowsIntCol(t, 2)
	testReaderMultiRowsIntCol(t, 10)
	testReaderMultiRowsIntCol(t, 100)
	testReaderMultiRowsIntCol(t, 1000)
	testReaderMultiRowsIntCol(t, 10000)
}

func testReaderMultiRowsIntCol(t *testing.T, rows int) {
	t.Helper()

	var expected []int
	var ss []string
	for i := 0; i < rows; i++ {
		n := rand.Int()
		if rand.Intn(2) == 0 {
			n = -n
		}
		expected = append(expected, n)
		ss = append(ss, fmt.Sprintf("%d\n", n))
	}

	b := bytes.NewBufferString(strings.Join(ss, ""))
	r := New(b)
	for i, expectedN := range expected {
		if !r.Next() {
			t.Fatalf("Next must return true when reading %d at row #%d", expectedN, i+1)
		}
		if r.Error() != nil {
			t.Fatalf("unexpected error when reading %d at row #%d: %s", expectedN, i+1, r.Error())
		}
		n := r.Int()
		if n != expectedN {
			t.Fatalf("unexpected int at row #%d: %d. Expecting %d", i+1, n, expectedN)
		}
		if r.Error() != nil {
			t.Fatalf("unexpected error after reading %d at row #%d: %s", expectedN, i+1, r.Error())
		}
	}

	if r.Next() {
		t.Fatalf("Next must return false")
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
}

func TestReaderMultiRowsMultiCols(t *testing.T) {
	testReaderMultiRowsMultiCols(t, 2, 2)
	testReaderMultiRowsMultiCols(t, 10, 5)
	testReaderMultiRowsMultiCols(t, 100, 10)
	testReaderMultiRowsMultiCols(t, 1000, 100)
	testReaderMultiRowsMultiCols(t, 10000, 3)
	testReaderMultiRowsMultiCols(t, 3, 500)
}

func testReaderMultiRowsMultiCols(t *testing.T, rows int, cols int) {
	t.Helper()

	var expected [][]string
	var ss []string
	for i := 0; i < rows; i++ {
		var rowS []string
		for j := 0; j < cols; j++ {
			s := fmt.Sprintf("foobar%d", j+i*cols)
			rowS = append(rowS, s)
		}
		expected = append(expected, rowS)
		ss = append(ss, strings.Join(rowS, "\t")+"\n")
	}

	b := bytes.NewBufferString(strings.Join(ss, ""))
	r := New(b)
	testReaderMultiRowsCols(t, r, expected)
}

func TestReaderSlowSource(t *testing.T) {
	testReaderSlowSource(t, 1, 10000)
	testReaderSlowSource(t, 10, 1000)
	testReaderSlowSource(t, 100, 100)
	testReaderSlowSource(t, 1000, 10)
	testReaderSlowSource(t, 10000, 1)
}

func testReaderSlowSource(t *testing.T, rows, cols int) {
	t.Helper()

	var expected [][]string
	var ss []string
	for i := 0; i < rows; i++ {
		var rowS []string
		for j := 0; j < cols; j++ {
			s := fmt.Sprintf("foo тест %d", j+i*cols)
			rowS = append(rowS, s)
		}
		expected = append(expected, rowS)
		ss = append(ss, strings.Join(rowS, "\t")+"\n")
	}

	b := &slowSource{
		s: []byte(strings.Join(ss, "")),
	}
	r := New(b)
	testReaderMultiRowsCols(t, r, expected)
}

func testReaderMultiRowsCols(t *testing.T, r *Reader, expected [][]string) {
	t.Helper()

	for i, rowS := range expected {
		if !r.Next() {
			t.Fatalf("Next must return true when reading row #%d", i+1)
		}
		if r.Error() != nil {
			t.Fatalf("unexpected error when reading row #%d: %s", i+1, r.Error())
		}
		if r.row != i+1 {
			t.Fatalf("unexpected row number: %d. Expecting %d", r.row, i+1)
		}
		for j, expectedS := range rowS {
			bb := r.Bytes()
			if string(bb) != expectedS {
				t.Fatalf("unexpected bytes at col #%d, row #%d: %q. Expecting %q", j+1, i+1, bb, expectedS)
			}
			if r.Error() != nil {
				t.Fatalf("unexpected error after reading col #%d, row #%d: %s", j+1, i+1, r.Error())
			}
			if r.row != i+1 {
				t.Fatalf("unexpected row number: %d. Expecting %d", r.row, i+1)
			}
			if r.col != j+1 {
				t.Fatalf("unexpected col number on row #%d: %d. Expecting %d", i+1, r.col, j+1)
			}
		}
	}
}

// slowSource returns data by small chunks.
type slowSource struct {
	s []byte
}

func (ss *slowSource) Read(p []byte) (int, error) {
	if len(ss.s) == 0 {
		return 0, io.EOF
	}

	chunkSize := rand.Intn(10) + 1
	if chunkSize > len(ss.s) {
		chunkSize = len(ss.s)
	}
	n := copy(p, ss.s[:chunkSize])
	ss.s = ss.s[n:]
	return n, nil
}

func TestReaderUintSuccess(t *testing.T) {
	testReaderUintSuccess(t, (1<<strconv.IntSize)-1)
	testReaderUintSuccess(t, 0)
	testReaderUintSuccess(t, 10)
}

func testReaderUintSuccess(t *testing.T, expectedN uint) {
	t.Helper()

	b := bytes.NewBufferString(fmt.Sprintf("%d\n", expectedN))
	r := New(b)
	r.Next()
	n := r.Uint()
	if n != expectedN {
		t.Fatalf("unexpected uint: %d. Expecting %d", n, expectedN)
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
}

func TestReaderUintNegative(t *testing.T) {
	b := bytes.NewBufferString("-123\n")
	r := New(b)
	r.Next()
	n := r.Uint()
	if n != 0 {
		t.Fatalf("unexpected non-zero uint: %d", n)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting non-zero error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "invalid syntax") {
		t.Fatalf("unexpected error: %s. Must contain %q", err, "invalid syntax")
	}
}

func TestReaderInt32Success(t *testing.T) {
	testReaderInt32Success(t, math.MaxInt32)
	testReaderInt32Success(t, math.MinInt32)
	testReaderInt32Success(t, 0)
	testReaderInt32Success(t, 10)
	testReaderInt32Success(t, -10)
}

func testReaderInt32Success(t *testing.T, expectedN int32) {
	t.Helper()

	b := bytes.NewBufferString(fmt.Sprintf("%d\n", expectedN))
	r := New(b)
	r.Next()
	n := r.Int32()
	if n != expectedN {
		t.Fatalf("unexpected int32: %d. Expecting %d", n, expectedN)
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
}

func TestReaderInt32TooBig(t *testing.T) {
	testReaderInt32TooBig(t, fmt.Sprintf("123%d", math.MaxInt32))
	testReaderInt32TooBig(t, fmt.Sprintf("-123%d", math.MaxInt32))
}

func testReaderInt32TooBig(t *testing.T, s string) {
	t.Helper()

	b := bytes.NewBufferString(s + "\n")
	r := New(b)
	r.Next()
	n := r.Int32()
	if n != 0 {
		t.Fatalf("unexpected non-zero int32: %d", n)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting non-zero error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "out of range") {
		t.Fatalf("unexpected error: %s. Must contain %q", err, "out of range")
	}
}

func TestReaderUint32Success(t *testing.T) {
	testReaderUint32Success(t, math.MaxUint32)
	testReaderUint32Success(t, 0)
	testReaderUint32Success(t, 10)
}

func testReaderUint32Success(t *testing.T, expectedN uint32) {
	t.Helper()

	b := bytes.NewBufferString(fmt.Sprintf("%d\n", expectedN))
	r := New(b)
	r.Next()
	n := r.Uint32()
	if n != expectedN {
		t.Fatalf("unexpected uint32: %d. Expecting %d", n, expectedN)
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
}

func TestReaderUint32Negative(t *testing.T) {
	b := bytes.NewBufferString("-123\n")
	r := New(b)
	r.Next()
	n := r.Uint32()
	if n != 0 {
		t.Fatalf("unexpected non-zero uint32: %d", n)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting non-zero error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "invalid syntax") {
		t.Fatalf("unexpected error: %s. Must contain %q", err, "invalid syntax")
	}
}

func TestReaderUint32TooBig(t *testing.T) {
	b := bytes.NewBufferString(fmt.Sprintf("123%d\n", math.MaxUint32))
	r := New(b)
	r.Next()
	n := r.Uint32()
	if n != 0 {
		t.Fatalf("unexpected non-zero uint32: %d", n)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting non-zero error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "out of range") {
		t.Fatalf("unexpected error: %s. Must contain %q", err, "out of range")
	}
}

func TestReaderInt16Success(t *testing.T) {
	testReaderInt16Success(t, math.MaxInt16)
	testReaderInt16Success(t, math.MinInt16)
	testReaderInt16Success(t, 0)
	testReaderInt16Success(t, 10)
	testReaderInt16Success(t, -10)
}

func testReaderInt16Success(t *testing.T, expectedN int16) {
	t.Helper()

	b := bytes.NewBufferString(fmt.Sprintf("%d\n", expectedN))
	r := New(b)
	r.Next()
	n := r.Int16()
	if n != expectedN {
		t.Fatalf("unexpected int16: %d. Expecting %d", n, expectedN)
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
}

func TestReaderInt16TooBig(t *testing.T) {
	testReaderInt16TooBig(t, fmt.Sprintf("1%d", math.MaxInt16))
	testReaderInt16TooBig(t, fmt.Sprintf("-1%d", math.MaxInt16))
}

func testReaderInt16TooBig(t *testing.T, s string) {
	t.Helper()

	b := bytes.NewBufferString(s + "\n")
	r := New(b)
	r.Next()
	n := r.Int16()
	if n != 0 {
		t.Fatalf("unexpected non-zero int16: %d", n)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting non-zero error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "out of range") {
		t.Fatalf("unexpected error: %s. Must contain %q", err, "out of range")
	}
}

func TestReaderUint16Success(t *testing.T) {
	testReaderUint16Success(t, math.MaxUint16)
	testReaderUint16Success(t, 0)
	testReaderUint16Success(t, 10)
}

func testReaderUint16Success(t *testing.T, expectedN uint16) {
	t.Helper()

	b := bytes.NewBufferString(fmt.Sprintf("%d\n", expectedN))
	r := New(b)
	r.Next()
	n := r.Uint16()
	if n != expectedN {
		t.Fatalf("unexpected uint16: %d. Expecting %d", n, expectedN)
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
}

func TestReaderUint16Negative(t *testing.T) {
	b := bytes.NewBufferString("-123\n")
	r := New(b)
	r.Next()
	n := r.Uint16()
	if n != 0 {
		t.Fatalf("unexpected non-zero uint16: %d", n)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting non-zero error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "invalid syntax") {
		t.Fatalf("unexpected error: %s. Must contain %q", err, "invalid syntax")
	}
}

func TestReaderUint16TooBig(t *testing.T) {
	b := bytes.NewBufferString(fmt.Sprintf("123%d\n", math.MaxUint16))
	r := New(b)
	r.Next()
	n := r.Uint16()
	if n != 0 {
		t.Fatalf("unexpected non-zero uint16: %d", n)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting non-zero error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "out of range") {
		t.Fatalf("unexpected error: %s. Must contain %q", err, "out of range")
	}
}

func TestReaderInt8Success(t *testing.T) {
	testReaderInt8Success(t, math.MaxInt8)
	testReaderInt8Success(t, math.MinInt8)
	testReaderInt8Success(t, 0)
	testReaderInt8Success(t, 10)
	testReaderInt8Success(t, -10)
}

func testReaderInt8Success(t *testing.T, expectedN int8) {
	t.Helper()

	b := bytes.NewBufferString(fmt.Sprintf("%d\n", expectedN))
	r := New(b)
	r.Next()
	n := r.Int8()
	if n != expectedN {
		t.Fatalf("unexpected int8: %d. Expecting %d", n, expectedN)
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
}

func TestReaderInt8TooBig(t *testing.T) {
	testReaderInt8TooBig(t, fmt.Sprintf("1%d", math.MaxInt8))
	testReaderInt8TooBig(t, fmt.Sprintf("-1%d", math.MaxInt8))
}

func testReaderInt8TooBig(t *testing.T, s string) {
	t.Helper()

	b := bytes.NewBufferString(s + "\n")
	r := New(b)
	r.Next()
	n := r.Int8()
	if n != 0 {
		t.Fatalf("unexpected non-zero int8: %d", n)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting non-zero error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "out of range") {
		t.Fatalf("unexpected error: %s. Must contain %q", err, "out of range")
	}
}

func TestReaderUint8Success(t *testing.T) {
	testReaderUint8Success(t, math.MaxUint8)
	testReaderUint8Success(t, 0)
	testReaderUint8Success(t, 10)
}

func testReaderUint8Success(t *testing.T, expectedN uint8) {
	t.Helper()

	b := bytes.NewBufferString(fmt.Sprintf("%d\n", expectedN))
	r := New(b)
	r.Next()
	n := r.Uint8()
	if n != expectedN {
		t.Fatalf("unexpected uint8: %d. Expecting %d", n, expectedN)
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
}

func TestReaderUint8Negative(t *testing.T) {
	b := bytes.NewBufferString("-123\n")
	r := New(b)
	r.Next()
	n := r.Uint8()
	if n != 0 {
		t.Fatalf("unexpected non-zero uint8: %d", n)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting non-zero error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "invalid syntax") {
		t.Fatalf("unexpected error: %s. Must contain %q", err, "invalid syntax")
	}
}

func TestReaderUint8TooBig(t *testing.T) {
	b := bytes.NewBufferString(fmt.Sprintf("1%d\n", math.MaxUint8))
	r := New(b)
	r.Next()
	n := r.Uint8()
	if n != 0 {
		t.Fatalf("unexpected non-zero uint8: %d", n)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting non-zero error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "out of range") {
		t.Fatalf("unexpected error: %s. Must contain %q", err, "out of range")
	}
}

func TestReaderInt64Success(t *testing.T) {
	testReaderInt64Success(t, math.MaxInt64)
	testReaderInt64Success(t, math.MinInt64)
	testReaderInt64Success(t, 0)
	testReaderInt64Success(t, 10)
	testReaderInt64Success(t, -10)
}

func testReaderInt64Success(t *testing.T, expectedN int64) {
	t.Helper()

	b := bytes.NewBufferString(fmt.Sprintf("%d\n", expectedN))
	r := New(b)
	r.Next()
	n := r.Int64()
	if n != expectedN {
		t.Fatalf("unexpected int64: %d. Expecting %d", n, expectedN)
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
}

func TestReaderInt64TooBig(t *testing.T) {
	testReaderInt64TooBig(t, fmt.Sprintf("123%d", math.MaxInt64))
	testReaderInt64TooBig(t, fmt.Sprintf("-123%d", math.MaxInt64))
}

func testReaderInt64TooBig(t *testing.T, s string) {
	t.Helper()

	b := bytes.NewBufferString(s + "\n")
	r := New(b)
	r.Next()
	n := r.Int64()
	if n != 0 {
		t.Fatalf("unexpected non-zero int64: %d", n)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting non-zero error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "out of range") {
		t.Fatalf("unexpected error: %s. Must contain %q", err, "out of range")
	}
}

func TestReaderUint64Success(t *testing.T) {
	testReaderUint64Success(t, math.MaxUint64)
	testReaderUint64Success(t, 0)
	testReaderUint64Success(t, 10)
}

func testReaderUint64Success(t *testing.T, expectedN uint64) {
	t.Helper()

	b := bytes.NewBufferString(fmt.Sprintf("%d\n", expectedN))
	r := New(b)
	r.Next()
	n := r.Uint64()
	if n != expectedN {
		t.Fatalf("unexpected uint64: %d. Expecting %d", n, expectedN)
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
}

func TestReaderUint64Negative(t *testing.T) {
	b := bytes.NewBufferString("-123\n")
	r := New(b)
	r.Next()
	n := r.Uint64()
	if n != 0 {
		t.Fatalf("unexpected non-zero uint64: %d", n)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting non-zero error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "invalid syntax") {
		t.Fatalf("unexpected error: %s. Must contain %q", err, "invalid syntax")
	}
}

func TestReaderUint64TooBig(t *testing.T) {
	b := bytes.NewBufferString(fmt.Sprintf("123%d\n", uint64(math.MaxUint64)))
	r := New(b)
	r.Next()
	n := r.Uint64()
	if n != 0 {
		t.Fatalf("unexpected non-zero uint64: %d", n)
	}
	err := r.Error()
	if err == nil {
		t.Fatalf("expecting non-zero error")
	}
	errS := err.Error()
	if !strings.Contains(errS, "out of range") {
		t.Fatalf("unexpected error: %s. Must contain %q", err, "out of range")
	}
}

func TestReaderFloat32Success(t *testing.T) {
	testReaderFloat32Success(t, 0)
	testReaderFloat32Success(t, 123)
	testReaderFloat32Success(t, -123)
	testReaderFloat32Success(t, 0.123)
	testReaderFloat32Success(t, -1.2345)
	testReaderFloat32Success(t, 123e34)
	testReaderFloat64Success(t, math.NaN())
	testReaderFloat64Success(t, math.Inf(1))
	testReaderFloat64Success(t, math.Inf(-1))
}

func testReaderFloat32Success(t *testing.T, f float32) {
	t.Helper()

	s := fmt.Sprintf("%f\n", f)
	b := bytes.NewBufferString(s)
	r := New(b)
	r.Next()
	f32 := r.Float32()
	if f32 != f && !(math.IsNaN(float64(f32)) && math.IsNaN(float64(f))) {
		t.Fatalf("unexpected float32: %f. Expecting %f", f32, f)
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
}

func TestReaderFloat32Error(t *testing.T) {
	testReaderFloat32Error(t, "")
	testReaderFloat32Error(t, "foobar")
	testReaderFloat32Error(t, "123bsc")
	testReaderFloat32Error(t, "a23.34")
	testReaderFloat32Error(t, "2.34ess")
}

func testReaderFloat32Error(t *testing.T, s string) {
	t.Helper()

	b := bytes.NewBufferString(s + "\n")
	r := New(b)
	r.Next()
	f32 := r.Float32()
	if f32 != 0 {
		t.Fatalf("unexpected float32: %f. Expecting %f", f32, 0.0)
	}
	if r.Error() == nil {
		t.Fatalf("expecting non-nil error")
	}
	errS := r.Error().Error()
	if !strings.Contains(errS, "invalid syntax") {
		t.Fatalf("unexpected error: %s. Must contain %q", errS, "invalid syntax")
	}
}

func TestReaderFloat64Success(t *testing.T) {
	testReaderFloat64Success(t, 0)
	testReaderFloat64Success(t, 123)
	testReaderFloat64Success(t, -123)
	testReaderFloat64Success(t, 0.123)
	testReaderFloat64Success(t, -1.2345)
	testReaderFloat64Success(t, 123e34)
	testReaderFloat64Success(t, math.NaN())
	testReaderFloat64Success(t, math.Inf(1))
	testReaderFloat64Success(t, math.Inf(-1))
}

func testReaderFloat64Success(t *testing.T, f float64) {
	t.Helper()

	s := fmt.Sprintf("%f\n", f)
	b := bytes.NewBufferString(s)
	r := New(b)
	r.Next()
	f64 := r.Float64()
	if f64 != f && !(math.IsNaN(f64) && math.IsNaN(f)) {
		t.Fatalf("unexpected float64: %f. Expecting %f", f64, f)
	}
	if r.Error() != nil {
		t.Fatalf("unexpected error: %s", r.Error())
	}
}

func TestReaderFloat64Error(t *testing.T) {
	testReaderFloat64Error(t, "")
	testReaderFloat64Error(t, "foobar")
	testReaderFloat64Error(t, "123bsc")
	testReaderFloat64Error(t, "a23.34")
	testReaderFloat64Error(t, "2.34ess")
}

func testReaderFloat64Error(t *testing.T, s string) {
	t.Helper()

	b := bytes.NewBufferString(s + "\n")
	r := New(b)
	r.Next()
	f64 := r.Float64()
	if f64 != 0 {
		t.Fatalf("unexpected float64: %f. Expecting %f", f64, 0.0)
	}
	if r.Error() == nil {
		t.Fatalf("expecting non-nil error")
	}
	errS := r.Error().Error()
	if !strings.Contains(errS, "invalid syntax") {
		t.Fatalf("unexpected error: %s. Must contain %q", errS, "invalid syntax")
	}
}

func TestReaderDateSuccess(t *testing.T) {
	testReaderDateSuccess(t, "0000-00-00")
	testReaderDateSuccess(t, "1970-01-01")
	testReaderDateSuccess(t, "2017-10-13")
}

func testReaderDateSuccess(t *testing.T, date string) {
	t.Helper()

	b := bytes.NewBufferString(date + "\n")
	r := New(b)
	r.Next()
	dt := r.Date()
	if r.Error() != nil {
		t.Fatalf("unexpected error on date %q: %s", date, r.Error())
	}
	if dt.Hour() != 0 {
		t.Fatalf("unexpected non-zero hour: %d", dt.Hour())
	}
	if dt.Minute() != 0 {
		t.Fatalf("unexpected non-zero minute: %d", dt.Minute())
	}
	if dt.Second() != 0 {
		t.Fatalf("unexpected non-zero second: %d", dt.Second())
	}
	s := dt.Format("2006-01-02")
	if s != date && !(date == "0000-00-00" && dt.IsZero()) {
		t.Fatalf("unexpected date: %q. Expecting %q", s, date)
	}
}

func TestReaderDateFailure(t *testing.T) {
	testReaderDateFailure(t, "")
	testReaderDateFailure(t, "foobar")
	testReaderDateFailure(t, "2017")
	testReaderDateFailure(t, "2017-10")
	testReaderDateFailure(t, "2017-10-")
	testReaderDateFailure(t, "2017-10-1")
	testReaderDateFailure(t, "2017-10-aa")
	testReaderDateFailure(t, "2017-bb-aa")
	testReaderDateFailure(t, "20cc-1b-3a")
	testReaderDateFailure(t, "2017-10-10 ")
}

func testReaderDateFailure(t *testing.T, date string) {
	t.Helper()

	b := bytes.NewBufferString(date + "\n")
	r := New(b)
	r.Next()
	dt := r.Date()
	if !dt.IsZero() {
		t.Fatalf("unexpected non-zero date when parsing %q: %s", dt, date)
	}
	if r.Error() == nil {
		t.Fatalf("expecting non-nil error")
	}
	errS := r.Error().Error()
	if !strings.Contains(errS, "cannot parse `date`") {
		t.Fatalf("unexpected error: %s. Must contain %q", errS, "cannot parse `date`")
	}
}

func TestReaderDateTimeSuccess(t *testing.T) {
	testReaderDateTimeSuccess(t, "0000-00-00 00:00:00")
	testReaderDateTimeSuccess(t, "1970-01-01 12:34:56")
	testReaderDateTimeSuccess(t, "2017-10-13 23:59:59")
}

func testReaderDateTimeSuccess(t *testing.T, datetime string) {
	t.Helper()

	b := bytes.NewBufferString(datetime + "\n")
	r := New(b)
	r.Next()
	dt := r.DateTime()
	if r.Error() != nil {
		t.Fatalf("unexpected error on datetime %q: %s", datetime, r.Error())
	}
	s := dt.Format("2006-01-02 15:04:05")
	if s != datetime && !(datetime == "0000-00-00 00:00:00" && dt.IsZero()) {
		t.Fatalf("unexpected datetime: %q. Expecting %q", s, datetime)
	}
}

func TestReaderDateTimeFailure(t *testing.T) {
	testReaderDateTimeFailure(t, "")
	testReaderDateTimeFailure(t, "foobar")
	testReaderDateTimeFailure(t, "2017")
	testReaderDateTimeFailure(t, "2017-10")
	testReaderDateTimeFailure(t, "2017-10-")
	testReaderDateTimeFailure(t, "2017-10-1")
	testReaderDateTimeFailure(t, "2017-10-aa")
	testReaderDateTimeFailure(t, "2017-bb-aa")
	testReaderDateTimeFailure(t, "20cc-1b-3a")
	testReaderDateTimeFailure(t, "2017-01-10")
	testReaderDateTimeFailure(t, "2017-01-10 ")
	testReaderDateTimeFailure(t, "2017-01-10 10")
	testReaderDateTimeFailure(t, "2017-01-10 10:20")
	testReaderDateTimeFailure(t, "2017-01-10 10:20:3")
	testReaderDateTimeFailure(t, "2017-01-10T10:20:30")
	testReaderDateTimeFailure(t, "2017-01-10 10:20:30 ")
	testReaderDateTimeFailure(t, "2017-01-10 10:20:3c")
	testReaderDateTimeFailure(t, "2017-01-10 10:2s:3c")
	testReaderDateTimeFailure(t, "2017-01-10 1w:2s:3c")
	testReaderDateTimeFailure(t, "2017-01-10 1w:2s:3c s")
}

func testReaderDateTimeFailure(t *testing.T, datetime string) {
	t.Helper()

	b := bytes.NewBufferString(datetime + "\n")
	r := New(b)
	r.Next()
	dt := r.DateTime()
	if !dt.IsZero() {
		t.Fatalf("unexpected non-zero datetime when parsing %q: %s", dt, datetime)
	}
	if r.Error() == nil {
		t.Fatalf("expecting non-nil error")
	}
	errS := r.Error().Error()
	if !strings.Contains(errS, "cannot parse `datetime`") {
		t.Fatalf("unexpected error: %s. Must contain %q", errS, "cannot parse `datetime`")
	}
}

func TestReaderBytesUnescape(t *testing.T) {
	testReaderBytesUnescape(t, `\`, `\`)
	testReaderBytesUnescape(t, `\b\f\r\n\t\0\'\\`, "\b\f\r\n\t\x00'\\")
	testReaderBytesUnescape(t, `0\b11\f2\r3\n4\t5\06\'7\\8`, "0\b11\f2\r3\n4\t5\x006'7\\8")
	testReaderBytesUnescape(t, `\1\2\3\4\`, "1234\\")
}

func testReaderBytesUnescape(t *testing.T, before, after string) {
	t.Helper()

	b := bytes.NewBufferString(before + "\n")
	r := New(b)
	r.Next()
	bb := r.Bytes()
	if r.Error() != nil {
		t.Fatalf("unexpected error when parsing %q: %s", before, r.Error())
	}
	if string(bb) != after {
		t.Fatalf("unexpected unescaped result: %q. Expecting %q", bb, after)
	}
}

func TestReaderString(t *testing.T) {
	testReaderString(t, "", "")
	testReaderString(t, "a", "a")
	testReaderString(t, `a\nb`, "a\nb")
}

func testReaderString(t *testing.T, before, after string) {
	t.Helper()

	b := bytes.NewBufferString(before + "\n")
	r := New(b)
	r.Next()
	s := r.String()
	if r.Error() != nil {
		t.Fatalf("unexpected error when parsing %q: %s", before, r.Error())
	}
	if s != after {
		t.Fatalf("unexpected unescaped result: %q. Expecting %q", s, after)
	}
}
