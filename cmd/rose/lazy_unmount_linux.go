//go:build linux

package main

import "syscall"

func lazyUnmount(mountPoint string) error {
	return syscall.Unmount(mountPoint, syscall.MNT_DETACH)
}
