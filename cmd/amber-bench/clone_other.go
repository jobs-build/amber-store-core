//go:build !darwin && !linux

package main

// cloneFile copies src to dst; this platform has no reflink primitive.
func cloneFile(src, dst string) error { return copyFile(src, dst) }
