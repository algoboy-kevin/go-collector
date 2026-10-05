package hyperliquid

import "time"

// RunIDLayout is the run identifier's format: the run's start instant, UTC, to the
// second, e.g. "2026-10-05T084829Z".
//
// Deliberately the same shape as BucketLayout plus a zone marker and without the colon
// a clock time would invite: it reads as the same kind of thing as an hour bucket, so
// there is one mental model for both.
//
// Second resolution is enough to tell concurrent runs apart, but NOT sequential ones — a
// crash loop can restart inside the same second — so a run id is made unique against the
// manifests already in the root (see uniqueRunID), and ordering is taken from the
// recorded start time rather than from the name (see sortManifests).
const RunIDLayout = "2006-01-02T150405"

// RunID identifies one recorder process by when it started.
//
// It is stamped into every manifest, every `_meta` sentinel and every quarantine file
// name, so any artifact can be traced back to the run that produced it without reading
// another file.
func RunID(startedAt time.Time) string {
	return startedAt.UTC().Format(RunIDLayout) + "Z"
}
