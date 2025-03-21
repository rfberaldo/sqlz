# Benchmarks

Subpackage for benchmarks against [sqlx](https://github.com/jmoiron/sqlx/).

## How to run

Install [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat), then:

```bash
go test -benchmem -count=10 -bench . benchmark/sqlx > sqlx.txt
go test -benchmem -count=10 -bench . benchmark/sqlz > sqlz.txt

# rename sqlx to sqlz, otherwise benchstat won't compare
sed -i 's/sqlx/sqlz/g' sqlx.txt

benchstat sqlx.txt sqlz.txt > results.txt
```

See results in [results.txt](results.txt).
