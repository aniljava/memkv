package memkv

import (
	"bytes"
	"os"
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
	os.Remove("test.db")
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
	os.Remove("test.db")
}

func TestNonExisiting(t *testing.T) {
	db, _ := Open("test.db")

	val := db.Get("NONEXISTING")
	if val != nil {
		t.Error("Expected nil")
	}
	db.Close()
	os.Remove("test.db")
}

func TestOptimizationStats(t *testing.T) {

	if true {
		return
	}

	db, _ := Open("test.db")

	for i := 0; i < SKIP_OPTIMIZATION_SIZE*2; i++ {

		if i%100000 == 0 {
			fmt.Println("WRITING", strconv.Itoa(i), "/", SKIP_OPTIMIZATION_SIZE*2, "STATS:", db.file_size, db.data_size, len(db.kv), time.Now())
		}
		err := db.Set(strconv.Itoa(i)+"key", []byte("DATA"))
		db.Remove(strconv.Itoa(i) + "key")
		if err != nil {
			fmt.Println(err)
		}

	}

	db.Close()
	os.Remove("test.db")
}
