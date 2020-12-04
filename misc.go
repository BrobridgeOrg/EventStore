package eventstore

import (
	"encoding/binary"
	"unsafe"
)

func GetFixedSizeBytes(size int, str string) []byte {
	data := make([]byte, size)
	copy(data[:], str)
	return data
}

func Uint64ToBytes(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}

func BytesToUint64(data []byte) uint64 {
	return uint64(binary.BigEndian.Uint64(data))
}

func StrToBytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}
