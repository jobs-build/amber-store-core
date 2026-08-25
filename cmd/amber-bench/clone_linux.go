package main

import (
	"os"

	"golang.org/x/sys/unix"
)

// cloneFile makes dst a reflink of src (FICLONE: btrfs, xfs, bcachefs) and
// falls back to a plain copy where the filesystem cannot.
func cloneFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := unix.IoctlFileClone(int(out.Fd()), int(in.Fd())); err == nil {
		return out.Close()
	}
	out.Close()
	return copyFile(src, dst)
}
