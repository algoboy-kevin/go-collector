package collector

import (
	"log/slog"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/algoboy-kevin/go-collector/pkg/libs"
)

// manifestFile is the epoch index written into every epoch directory.
const manifestFile = "manifest.json"

// epochIndex accumulates one EpochManifest per epoch a run touches, and writes
// them as {recordingDir}/{epoch}/manifest.json.
//
// An epoch's markets arrive from two directions: its own settlement event is
// claimed while the epoch is running, and the ladder families' events are
// claimed a day early as the preceding epoch's "+1" carry — but they are still
// filed under the epoch they settle into. Both paths write into this index, so
// the manifest written when an epoch ends is complete (CONTEXT.md §4/§6).
type epochIndex struct {
	anchor libs.EpochAnchor
	run    RunInfo

	mu     sync.Mutex
	epochs map[string]*EpochManifest
}

// newEpochIndex creates an empty index for a run.
func newEpochIndex(anchor libs.EpochAnchor, run RunInfo) *epochIndex {
	return &epochIndex{anchor: anchor, run: run, epochs: make(map[string]*EpochManifest)}
}

// Record files a fan-out's markets under the epoch they settle into, and notes
// the event as an offset carry of the epoch that is running. filter is the rung
// filter the claim was made under (nil for the non-ladder families).
func (ix *epochIndex) Record(runningEpoch string, t EventTarget, started []StartedMarket, filter *StrikeFilterInfo) {
	if len(started) == 0 {
		return
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()

	// The "+1" ladder event is recorded during one epoch but settles in the
	// next, so the epoch it was picked up in remembers the event slug.
	if t.OffsetDays > 0 && runningEpoch != "" && runningEpoch != t.EpochID {
		m := ix.manifestLocked(runningEpoch)
		m.OffsetEvents = appendUnique(m.OffsetEvents, t.Event.Slug)
	}

	entry := ix.seriesEntryLocked(t)
	if filter != nil {
		entry.StrikeFilter = filter
	}
	for _, st := range started {
		entry.Markets = appendMarket(entry.Markets, st.manifestMarket())
	}
}

// Epochs lists the epochs the index holds, sorted.
func (ix *epochIndex) Epochs() []string {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	out := make([]string, 0, len(ix.epochs))
	for id := range ix.epochs {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// writeEpoch writes {root}/{epochID}/manifest.json. No-op for an epoch with
// nothing recorded, so a rolled-over epoch with no markets leaves no empty
// directory behind.
//
// truncated lists markets the run ended before they settled; their recordings
// are partial and are flagged here so a reader does not have to open every
// market's metadata to find them.
func (ix *epochIndex) writeEpoch(root, epochID string, feeds map[string][]string, truncated map[string]bool) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()

	m, ok := ix.epochs[epochID]
	if !ok {
		return nil
	}
	if len(feeds) > 0 {
		if m.Feeds == nil {
			m.Feeds = make(map[string][]string, len(feeds))
		}
		for group, refs := range feeds {
			m.Feeds[group] = refs
		}
	}
	for i := range m.Series {
		for j := range m.Series[i].Markets {
			m.Series[i].Markets[j].Truncated = truncated[m.Series[i].Markets[j].MarketID]
		}
	}
	normalize(m)
	return writeJSONFile(filepath.Join(root, epochID, manifestFile), m)
}

// WriteAll writes the manifest of every epoch the run touched. Called at
// shutdown, so even a run killed mid-epoch leaves an index of what it claimed.
func (ix *epochIndex) WriteAll(root string, feeds func(epochID string) map[string][]string, truncated map[string]bool) error {
	var firstErr error
	for _, id := range ix.Epochs() {
		var refs map[string][]string
		if feeds != nil {
			refs = feeds(id)
		}
		if err := ix.writeEpoch(root, id, refs, truncated); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// manifestLocked returns the manifest for an epoch, creating it on first use.
// Caller holds ix.mu.
func (ix *epochIndex) manifestLocked(epochID string) *EpochManifest {
	if m, ok := ix.epochs[epochID]; ok {
		return m
	}
	start, end, err := libs.EpochBounds(epochID, ix.anchor)
	if err != nil {
		// Epoch ids always come from libs, so this cannot happen in practice;
		// keep the epoch in the index with zero bounds rather than dropping it.
		slog.Error("manifest: cannot resolve epoch bounds", "epoch", epochID, "err", err)
	}
	run := ix.run
	m := &EpochManifest{
		EpochID:    epochID,
		EpochStart: start.UnixMilli(),
		EpochEnd:   end.UnixMilli(),
		Anchor:     string(ix.anchor),
		Run:        &run,
		RecordedAt: time.Now().UnixMilli(),
	}
	ix.epochs[epochID] = m
	return m
}

// seriesEntryLocked finds or creates the series entry for a target. Entries are
// keyed by (series, settle instant, event): an epoch therefore holds one entry
// per hourly window for up/down, and one per dated event for a ladder — plus two
// entries for the single instant a DST transition day carries two events.
// Caller holds ix.mu.
func (ix *epochIndex) seriesEntryLocked(t EventTarget) *SeriesManifest {
	m := ix.manifestLocked(t.EpochID)
	key := seriesEntryKey(t.Spec.Name, t.SettleAt, t.Event.Slug)

	for i := range m.Series {
		if seriesEntryKey(m.Series[i].Series, time.UnixMilli(m.Series[i].SettleAt), m.Series[i].EventSlug) == key {
			return &m.Series[i]
		}
	}

	entry := SeriesManifest{
		Series:      t.Spec.Name,
		Family:      string(t.Spec.Key.Family),
		Interval:    libs.IntervalLabel(t.Spec.Key.Interval),
		Asset:       t.Spec.Key.Asset,
		GammaSeries: t.Spec.GammaSeries,
		// The event slug is always recorded, not just for ladders: it is what
		// tells two entries at the same settle instant apart on a DST transition
		// day, where the series genuinely carries two events.
		EventSlug: t.Event.Slug,
		SettleAt:  t.SettleAt.UnixMilli(),
	}
	if t.Spec.Ladder {
		entry.LadderSize = len(t.Event.Markets)
	}
	m.Series = append(m.Series, entry)
	return &m.Series[len(m.Series)-1]
}

func seriesEntryKey(series string, settleAt time.Time, eventSlug string) string {
	return series + "@" + strconv.FormatInt(settleAt.UnixMilli(), 10) + "@" + eventSlug
}

// appendMarket adds a market unless its id is already listed.
func appendMarket(markets []ManifestMarket, m ManifestMarket) []ManifestMarket {
	for _, existing := range markets {
		if existing.MarketID == m.MarketID {
			return markets
		}
	}
	return append(markets, m)
}

// appendUnique adds a string unless already present.
func appendUnique(list []string, s string) []string {
	for _, existing := range list {
		if existing == s {
			return list
		}
	}
	return append(list, s)
}

// normalize sorts a manifest so it reads in a stable order — a file that is
// rewritten every tick must not churn.
func normalize(m *EpochManifest) {
	sort.Strings(m.OffsetEvents)
	for group, refs := range m.Feeds {
		sorted := append([]string(nil), refs...)
		sort.Strings(sorted)
		m.Feeds[group] = sorted
	}
	sort.Slice(m.Series, func(i, j int) bool {
		a, b := m.Series[i], m.Series[j]
		if a.SettleAt != b.SettleAt {
			return a.SettleAt < b.SettleAt
		}
		if a.Series != b.Series {
			return a.Series < b.Series
		}
		return a.EventSlug < b.EventSlug
	})
	for i := range m.Series {
		sort.SliceStable(m.Series[i].Markets, func(x, y int) bool {
			a, b := m.Series[i].Markets[x], m.Series[i].Markets[y]
			if av, bv := rungSortValue(a), rungSortValue(b); av != bv {
				return av < bv
			}
			return a.MarketID < b.MarketID
		})
	}
}

// rungSortValue orders a manifest's markets the way a ladder reads: by strike,
// then by range bound, with non-ladder markets all equal.
func rungSortValue(m ManifestMarket) float64 {
	switch {
	case m.Strike != nil:
		return *m.Strike
	case m.RangeLow != nil && m.RangeHigh != nil:
		return (*m.RangeLow + *m.RangeHigh) / 2
	case m.RangeLow != nil:
		return *m.RangeLow
	case m.RangeHigh != nil:
		return *m.RangeHigh
	default:
		return 0
	}
}
