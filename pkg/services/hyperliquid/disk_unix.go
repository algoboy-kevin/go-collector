//go:build unix

package hyperliquid

import "syscall"

// freeBytes returns the bytes available to this process on the filesystem holding
// path.
//
// Bavail, not Bfree: Bfree includes the blocks reserved for root, which an
// unprivileged process cannot use. Reporting Bfree would claim headroom that does not
// exist and let the recorder run into a full disk — the exact failure the guard is
// there to prevent.
func freeBytes(path string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	// Converted through uint64 because the field types differ between platforms
	// (Bavail is int64 on darwin and uint64 on linux; Bsize is uint32 and int64).
	return int64(uint64(st.Bavail) * uint64(st.Bsize)), nil
}
