package sqlogger_test

import (
	"log"
	"log/slog"

	"github.com/mattn/go-sqlite3"
	"github.com/rfberaldo/sqlz/sqlogger"
)

func ExampleOpen() {
	// [sqlogger.Open] is similar to [sql.Open], with two additional parameters:
	// [slog.Logger] and [sqlogger.Options]
	db, err := sqlogger.Open("sqlite3", ":memory:", slog.Default(), nil)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("CREATE TABLE user (id INT PRIMARY KEY, name TEXT")
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleOpen_options() {
	opts := &sqlogger.Options{
		// options...
	}

	// use sqlogger.Options as fourth parameter
	db, err := sqlogger.Open("sqlite3", ":memory:", slog.Default(), opts)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("CREATE TABLE user (id INT PRIMARY KEY, name TEXT")
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleNew() {
	// [sqlogger.New] receive the [driver.Driver] instead of the driver name
	db := sqlogger.New(&sqlite3.SQLiteDriver{}, ":memory:", slog.Default(), nil)

	_, err := db.Exec("CREATE TABLE user (id INT PRIMARY KEY, name TEXT")
	if err != nil {
		log.Fatal(err)
	}
}
