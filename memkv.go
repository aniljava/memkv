package memkv

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
)

//TODO take care of file_size and data_size at optimize

type KV interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Remove(key string) error
	Exists(key string) (bool, error)
	io.Closer
}

type MemKV struct {
	kv              map[string][]byte
	file            *os.File
	writer_count    int
	write_lock      sync.Mutex
	open            bool
	file_size       int
	data_size       int
	optimize_factor int
	optimize_lock   sync.Mutex
}

func (kv *MemKV) optimizeIfNecessary() {
	//Fast fail
	if kv.data_size*kv.optimize_factor < kv.file_size {
		return
	}
	kv.optimize_lock.Lock()
	defer kv.optimize_lock.Unlock()

	// Safeguard against other concurrent optimize request
	if kv.data_size*kv.optimize_factor < kv.file_size {
		return
	}
	kv.Optimize()
}

func (kv *MemKV) SetOptimizeFactor(factor int) {
	kv.optimize_factor = factor
}

func (kv *MemKV) Close() error {
	//Already closed
	if !kv.open {
		return nil
	}
	kv.open = false //Stop further writes

	kv.write_lock.Lock()
	defer kv.write_lock.Unlock()

	for kv.writer_count != 0 {
		//Wait for all writes to complete
	}

	err := kv.file.Close()
	return err
}

func (kv *MemKV) Sync() error {
	if !kv.open {
		return errors.New("Could not sync closed file")
	}
	err := kv.file.Sync()
	return err
}

func Open(path string) (*MemKV, error) {

	if _, err := os.Stat(path); os.IsNotExist(err) {
		file, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		file.Close()
		return Open(path)
	}

	kv := MemKV{}
	kv.kv = make(map[string][]byte)
	kv.open = true
	kv.optimize_factor = 3

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	/** LOAD CONTENT OF FILE **/

	flag := []byte{0}
	keylength := []byte{0, 0}
	valuelength := []byte{0, 0, 0, 0}

	for {
		count, err := file.Read(flag)
		if count == 0 || err == io.EOF {
			break
		}

		switch flag[0] {
		case REMOVE:

			count, err := file.Read(keylength)
			if err != nil || count != 2 {
				return nil, err
			}

			key := make([]byte, bytes_to_int16(keylength))
			count, err = file.Read(key)

			if err != nil || count != int(bytes_to_int16(keylength)) {
				return nil, err
			}

			/** OPTIMIZATION ENTRY START **/
			old_value := kv.kv[string(key)]
			if old_value != nil {
				kv.data_size = kv.data_size - 1 - 2 - int(bytes_to_int16(keylength)) - int(len(old_value))
			}
			kv.file_size = kv.file_size + 1 + 2 + int(bytes_to_int16(keylength))
			/** OPTIMIZATION ENTRY END **/

			delete(kv.kv, string(key))
		case WRITE:
			//READ KEY
			count, err := file.Read(keylength)
			if err != nil || count != 2 {
				return nil, err
			}

			//READ VALUE
			count, err = file.Read(valuelength)
			if err != nil || count != 4 {
				return nil, err
			}

			key := make([]byte, bytes_to_int16(keylength))
			count, err = file.Read(key)
			if err != nil || count != int(bytes_to_int16(keylength)) {
				return nil, err
			}

			value := make([]byte, bytes_to_int32(valuelength))
			count, err = file.Read(value)

			if err != nil || count != int(bytes_to_int32(valuelength)) {
				return nil, err
			}

			kv.kv[string(key)] = value

			/** OPTIMIZATION ENTRY START **/
			kv.data_size = kv.data_size + 1 + 2 + 4 + int(bytes_to_int16(keylength)) + int(len(value))
			kv.file_size = kv.file_size + 1 + 2 + 4 + int(bytes_to_int16(keylength)) + int(len(value))
			/** OPTIMIZATION ENTRY END **/
		}

	}

	file.Close()

	file, _ = os.OpenFile(path, syscall.O_RDWR|syscall.O_APPEND, 0660)
	kv.file = file

	return &kv, nil
}

const WRITE = 1
const REMOVE = 0

func (kv *MemKV) Get(key string) []byte {
	value := kv.kv[key]
	return value
}

func (kv *MemKV) changeWriterCount(i int) {
	kv.writer_count = kv.writer_count + i
}

func (kv *MemKV) Set(key string, value []byte) error {

	kv.optimizeIfNecessary()

	kv.write_lock.Lock()
	kv.changeWriterCount(1)
	defer kv.changeWriterCount(-1)
	kv.write_lock.Unlock()

	existing := kv.kv[key]
	if existing != nil {
		if bytes.Equal(existing, value) {
			return nil
		}
		kv.data_size = kv.data_size - 1 - 2 - 4 - int(len(key)) - int(len(value))
	}

	kv.kv[key] = value

	keylength := len(key)
	valuelength := len(value)

	buffer := make([]byte, 1+6+keylength+valuelength)
	buffer[0] = WRITE
	int16_to_bytes(int16(keylength), buffer[1:3])
	int32_to_bytes(int32(valuelength), buffer[3:7])
	copy(buffer[7:7+keylength], key)
	copy(buffer[7+keylength:], value)

	_, err := kv.file.Write(buffer)
	if err != nil {
		return err
	}

	/** OPTIMIZATION ENTRY START **/
	kv.data_size = kv.data_size + 1 + 2 + 4 + int(keylength) + int(len(value))
	kv.file_size = kv.file_size + 1 + 2 + 4 + int(keylength) + int(len(value))
	/** OPTIMIZATION ENTRY END **/

	return nil
}

func (kv *MemKV) Optimize() error {
	kv.write_lock.Lock()
	defer kv.write_lock.Unlock()

	for kv.writer_count != 0 {
	}
	/*
		Iterate all keys, write new in
	*/
	path := kv.file.Name() + ".opt"

	tempdb, err := Open(path)
	if err != nil {
		return err
	}

	for k, v := range kv.kv {
		tempdb.Set(k, v)
	}

	tempdb.file.Close()
	kv.file.Close()
	syscall.Rename(path, kv.file.Name())

	//Update

	file, _ := os.OpenFile(kv.file.Name(), syscall.O_RDWR|syscall.O_APPEND, 0660)
	kv.file = file

	return nil
}

func (kv *MemKV) Remove(key string) error {

	kv.optimizeIfNecessary()

	value, exists := kv.kv[key]
	if !exists {
		return nil
	}

	kv.data_size = kv.data_size - 1 - 2 - 4 - int(len(key)) - int(len(value))
	kv.file_size = kv.file_size + 1 + 2 + int(len(key))

	kv.write_lock.Lock()
	kv.changeWriterCount(1)
	defer kv.changeWriterCount(-1)
	kv.write_lock.Unlock()

	keylength := len(key)
	buffer := make([]byte, 1+2+keylength)
	buffer[0] = REMOVE
	int16_to_bytes(int16(keylength), buffer[1:3])
	copy(buffer[3:3+keylength], key)

	_, err := kv.file.Write(buffer)
	if err != nil {
		return err
	}

	delete(kv.kv, key)

	return nil
}

func int16_to_bytes(a int16, buffer []byte) {
	buffer[0] = (byte)(a & 0xFF)
	buffer[1] = (byte)((a >> 8) & 0xFF)
}

func int32_to_bytes(a int32, buffer []byte) {
	buffer[0] = (byte)(a & 0xFF)
	buffer[1] = (byte)((a >> 8) & 0xFF)
	buffer[2] = (byte)((a >> 16) & 0xFF)
	buffer[3] = (byte)((a >> 24) & 0xFF)
}

func bytes_to_int32(b []byte) int32 {
	return int32(b[0]) + int32(b[1])<<8 + int32(b[2])<<16 + int32(b[3])<<24
}

func bytes_to_int16(b []byte) int16 {
	return int16(b[0]) + int16(b[1])<<8
}
