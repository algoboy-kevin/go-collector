// Command gzmem measures the memory and CPU cost of the gzip writer that the
// Hyperliquid recorder will use, so the "does compression cost RAM?" question
// has a number instead of an opinion.
//
// It answers three things:
//
//  1. Fixed cost: live heap added by opening a gzip writer at BestSpeed
//     (the level go-collector's FeedRecorder already uses).
//  2. Flat cost: live heap after 10k vs 400k frames — if compression streams,
//     these are the same. Checkpoints are absolute GC'd HeapAlloc readings, not
//     differences, so a GC that frees more than it allocates cannot wrap an
//     unsigned subtraction into a nonsense 1.8e16 KB.
//  3. CPU cost: raw MB/s and ns/frame, from which the share of one core
//     follows for a given feed rate. Also reports allocation churn per frame,
//     which is what actually drives GC pressure.
//
// Throwaway dev helper: `go run ./.tmp/gzmem`
package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"time"
)

const (
	frames    = 400_000
	midFrames = 10_000
	ringSize  = 4096
	chanCount = 4 // l2Book, trades, bbo, activeAssetCtx
)

// gcHeap returns live heap bytes after a forced GC: what is actually retained,
// not garbage awaiting collection.
func gcHeap() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// allocTotal returns cumulative bytes ever allocated. Monotonic, so the delta
// across N frames is the per-frame allocation churn (GC pressure).
func allocTotal() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.TotalAlloc
}

// kb formats a signed difference between two unsigned heap readings.
func kb(a, b uint64) float64 { return float64(int64(a)-int64(b)) / 1024 }

// countWriter counts compressed bytes without retaining them, so a ratio
// measurement cannot itself grow the heap.
type countWriter struct{ n int64 }

func (c *countWriter) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}

// makeRing builds a set of *varying* frames. Repeating one identical buffer
// would compress absurdly well and make the CPU number meaningless; real books
// change a little on every push.
func makeRing(n int) [][]byte {
	r := rand.New(rand.NewSource(1))
	ring := make([][]byte, n)
	for i := range ring {
		bid := 100000 + r.Intn(200)
		ask := bid + 2 + r.Intn(40)
		s := fmt.Sprintf(`{"channel":"l2Book","data":{"coin":"BTC","time":%d,"levels":[[`,
			1767225600000+int64(r.Intn(1000)))
		for lvl := 0; lvl < 5; lvl++ {
			if lvl > 0 {
				s += ","
			}
			s += fmt.Sprintf(`{"px":"%d.%d","sz":"%d.%03d","n":%d}`,
				bid-lvl, r.Intn(10), r.Intn(9)+1, r.Intn(1000), r.Intn(20)+1)
		}
		s += "],["
		for lvl := 0; lvl < 5; lvl++ {
			if lvl > 0 {
				s += ","
			}
			s += fmt.Sprintf(`{"px":"%d.%d","sz":"%d.%03d","n":%d}`,
				ask+lvl, r.Intn(10), r.Intn(9)+1, r.Intn(1000), r.Intn(20)+1)
		}
		s += `}]]}}`
		ring[i] = []byte(s)
	}
	return ring
}

func main() {
	ring := makeRing(ringSize)
	var frameBytes int
	for _, f := range ring {
		frameBytes += len(f)
	}
	avg := frameBytes / len(ring)

	fmt.Printf("frame  : %d distinct l2Book frames, avg %d bytes\n", len(ring), avg)
	rawMB := float64(frames*avg) / (1 << 20)
	fmt.Printf("workload: %d frames = %.1f MB of raw input\n\n", frames, rawMB)

	levels := []struct {
		name string
		lvl  int
	}{
		{"BestSpeed (level 1)  <- FeedRecorder's choice", gzip.BestSpeed},
		{"DefaultCompression (6)", gzip.DefaultCompression},
		{"BestCompression (9)", gzip.BestCompression},
	}

	baseline := gcHeap()
	fmt.Printf("baseline live heap (ring built, no writer): %.1f KB\n\n",
		float64(baseline)/1024)

	// Ratio over the distinct frames ONLY. Compressing the full 400k-frame ring
	// would repeat a 4096-frame cycle many times and flatter the ratio well
	// beyond what a real capture sees.
	var sampleBytes int
	for _, f := range ring {
		sampleBytes += len(f)
	}
	fmt.Printf("compression ratio, %d distinct frames (%.2f MB raw):\n",
		len(ring), float64(sampleBytes)/(1<<20))
	for _, l := range levels {
		cw := &countWriter{}
		gz, err := gzip.NewWriterLevel(cw, l.lvl)
		if err != nil {
			panic(err)
		}
		for _, f := range ring {
			if _, err := gz.Write(f); err != nil {
				panic(err)
			}
		}
		if err := gz.Close(); err != nil {
			panic(err)
		}
		fmt.Printf("  %-42s %6.1f KB   %5.1f:1\n",
			l.name, float64(cw.n)/1024, float64(sampleBytes)/float64(cw.n))
	}
	fmt.Println()

	for _, l := range levels {
		h0 := gcHeap()
		t0 := allocTotal()

		// io.Discard: we are measuring the *compressor's* cost, not the size
		// of whatever buffer happens to be collecting its output.
		gz, err := gzip.NewWriterLevel(io.Discard, l.lvl)
		if err != nil {
			panic(err)
		}
		hOpen := gcHeap()

		start := time.Now()
		for i := 0; i < midFrames; i++ {
			if _, err := gz.Write(ring[i%len(ring)]); err != nil {
				panic(err)
			}
		}
		hMid := gcHeap()

		for i := midFrames; i < frames; i++ {
			if _, err := gz.Write(ring[i%len(ring)]); err != nil {
				panic(err)
			}
		}
		hEnd := gcHeap()
		tEnd := allocTotal()
		elapsed := time.Since(start)

		_ = gz.Close()

		mbps := rawMB / elapsed.Seconds()
		nsPerFrame := float64(elapsed.Nanoseconds()) / float64(frames)

		fmt.Printf("%s\n", l.name)
		fmt.Printf("  retained heap: on open %+.1f KB -> 10k frames %+.1f KB -> 400k frames %+.1f KB\n",
			kb(hOpen, h0), kb(hMid, h0), kb(hEnd, h0))
		fmt.Printf("  growth 10k -> 400k frames: %+.1f KB   <- ~0 means it streams\n",
			kb(hEnd, hMid))
		fmt.Printf("  alloc churn: %.0f bytes/frame\n", float64(tEnd-t0)/float64(frames))
		fmt.Printf("  speed: %.1f MB/s raw (%.0f ns/frame)\n", mbps, nsPerFrame)
		fmt.Printf("  at 4 GB/day raw: %.3f%% of one core\n\n", (0.0463/mbps)*100)
	}

	// Concurrent writers, as an absolute delta from a fresh baseline, so the
	// answer is "what 4 channels actually cost".
	hBase := gcHeap()
	writers := make([]*gzip.Writer, chanCount)
	for i := range writers {
		w, err := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
		if err != nil {
			panic(err)
		}
		writers[i] = w
	}
	hAll := gcHeap()
	fmt.Printf("concurrent writers at BestSpeed\n")
	fmt.Printf("  %d channels cost %+.1f KB total (%.1f KB each)\n",
		chanCount, kb(hAll, hBase), kb(hAll, hBase)/chanCount)

	// Keep everything alive to the end so no checkpoint measures
	// half-collected state.
	runtime.KeepAlive(ring)
	runtime.KeepAlive(writers)
}
