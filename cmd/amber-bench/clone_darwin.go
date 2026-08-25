package main

import "golang.org/x/sys/unix"

// cloneFile makes dst a copy-on-write clone of src (APFS clonefile): instant,
// no extra blocks, and read back like any other file.
func cloneFile(src, dst string) error {
	return unix.Clonefile(src, dst, 0)
}
