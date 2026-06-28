//go:build !linux

package main

import "fmt"

func lazyUnmount(mountPoint string) error {
	return fmt.Errorf("lazy unmount is only implemented on linux for %q", mountPoint)
}
