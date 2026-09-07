package storage

import "os"

// RemovePlogFiles removes a retired plog and its recovery evidence. The caller
// must first remove the authoritative placement and close all file users.
// Persisting data-file deletion before deleting the journal ensures an
// interrupted cleanup cannot leave a torn data file without its rollback copy.
// Missing files are tolerated so cleanup can resume after a crash.
func RemovePlogFiles(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := syncParent(path); err != nil {
		return err
	}
	for _, suffix := range []string{".undo", ".undo.tmp"} {
		if err := os.Remove(path + suffix); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return syncParent(path)
}
