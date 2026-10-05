//go:build !unix

package hyperliquid

import "errors"

// freeBytes is unavailable on this platform, so the disk guard cannot run.
//
// It fails loudly rather than returning a large number: silently disabling the guard
// would be indistinguishable from having headroom.
func freeBytes(string) (int64, error) {
	return 0, errors.New("hyperliquid: free-space check is not supported on this platform")
}
