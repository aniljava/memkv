package main

import (
	"bytes"
	"testing"
)

func TestSet(t *testing.T) {
	db, _ := Open("test.db")

	db.Set("key", []byte("value"))
	db.Close()

	db1, _ := Open("test.db")
	value := db1.Get("key")
	if !bytes.Equal(value, []byte("value")) {
		t.Error("Does not match !")
	}
	db.Close()
}

func TestDelete(t *testing.T) {
	db, _ := Open("test.db")

	db.Set("key", []byte("value"))
	db.Remove("key")
	db.Close()

	db1, _ := Open("test.db")
	value := db1.Get("key")
	if !bytes.Equal(value, nil) {
		t.Error("Does not match !")
	}
	db.Close()
}

func TestNonExisiting(t *testing.T) {
	db, _ := Open("test.db")

	val := db.Get("NONEXISTING")
	if val != nil {
		t.Error("Expected nil")
	}
	db.Close()
}
