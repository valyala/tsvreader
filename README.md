# tsvreader - fast reader for tab-separated data

## Features

* Optimized for speed. May read more than 20M rows per second on a single
  CPU core.
* Compatible with `TSV` format used in [ClickHouse](https://github.com/yandex/ClickHouse) responses.
* May read rows with variable number of columns using [Reader.HasCols](https://godoc.org/github.com/valyala/tsvreader#Reader.HasCols).
  This functionality allows reading [WITH TOTALS](http://clickhouse.readthedocs.io/en/latest/reference_en.html#WITH+TOTALS+modifier)
  row from `ClickHouse` responses.

## Documentation

See [these docs](https://godoc.org/github.com/valyala/tsvreader).
