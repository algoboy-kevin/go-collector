# Connector changes required by go-collector

> **Superseded — kept as history.** Everything here landed in
> `go-exchange-connector` **v0.6.0** and is now summarised in `CONTEXT.md` §8.1,
> which is the live record. This file is the request as it was written against
> v0.5.12.

Target repo: `github.com/algoboy-kevin/go-exchange-connector`
Current pin: **v0.5.12** → requested: **v0.6.0**
Consumer: `go-collector` (companion design doc: `CONTEXT.md` in that repo)

Three things are needed. Sections 1 and 2 are **blocking**; section 3 is optional.
Section 5 gives the interfaces go-collector will code against, so it can be built and tested
before v0.6.0 lands.

Context for why: the collector must (a) **discover** rolling events from Gamma series instead of
synthesizing market slugs, (b) record **ladder events** where 1 event contains N markets, and
(c) record Binance **5m klines** so market outcomes can be recomputed offline from the actual
settlement source.

---

## 1. Gamma series discovery (blocking)

### 1.1 Endpoints (all verified working 2026-10-02)

```
GET /series?slug=<seriesSlug>                      → [GammaSeries]   (1 element)
GET /events?series_id=<id>&closed=false&limit=50   → [GammaEvent]     (events + nested markets)
GET /events?slug=<eventSlug>                       → [GammaEvent]
```

* `GET /events?series_id=…` is the important one: it returns the **currently open and future**
  events of a series, each with its **nested `markets[]`**.
* **A `User-Agent` header is required.** Without it Gamma returns `403 Forbidden`. If the shared
  Gamma HTTP client does not already set one, it must.
* These endpoints are public — no CLOB auth headers.

### 1.2 New types (root `connector` package)

```go
// GammaSeries is a recurring Gamma market series.
type GammaSeries struct {
	ID         string       `json:"id"`
	Ticker     string       `json:"ticker"`
	Slug       string       `json:"slug"`
	Title      string       `json:"title"`
	SeriesType string       `json:"seriesType"` // "single"
	Recurrence string       `json:"recurrence"` // "5m" | "15m" | "hourly" | "4h" | "daily" | "weekly" | "monthly"
	Active     bool         `json:"active"`
	Closed     bool         `json:"closed"`
	Archived   bool         `json:"archived"`
	Events     []GammaEvent `json:"events,omitempty"`
}

// GammaEvent is one dated event; it may contain several strike/range markets.
type GammaEvent struct {
	ID           string        `json:"id"`
	Ticker       string        `json:"ticker"` // == the market/event slug used by GetMarket
	Slug         string        `json:"slug"`
	Title        string        `json:"title"`
	Description  string        `json:"description"`
	SeriesSlug   string        `json:"seriesSlug"`
	StartDate    time.Time     `json:"startDate"` // trading window open (NOT the settle instant)
	EndDate      time.Time     `json:"endDate"`   // trading window close (== settle instant for our families)
	Closed       bool          `json:"closed"`
	Active       bool          `json:"active"`
	NegRisk      bool          `json:"negRisk"`
	Tags         []GammaTag    `json:"tags,omitempty"`
	Markets      []GammaMarket `json:"markets"`
}

type GammaTag struct {
	ID   string `json:"id"`
	Slug string `json:"slug"`
}

// GammaMarket is one market inside an event (one rung of a ladder).
// Wire format only — map into connector.Market where convenient.
type GammaMarket struct {
	ID              string `json:"id"`
	Slug            string `json:"slug"`
	Question        string `json:"question"`
	ConditionID     string `json:"conditionId"`
	GroupItemTitle  string `json:"groupItemTitle"`  // the strike, e.g. "80,000"
	Outcomes        string `json:"outcomes"`        // JSON-encoded array, e.g. ["Yes","No"]
	ClobTokenIDs    string `json:"clobTokenIds"`    // JSON-encoded array, index-aligned with Outcomes
	Description     string `json:"description"`     // contains the machine-readable resolution rule
	StartDate       string `json:"startDate"`
	EndDate         string `json:"endDate"`
	Closed          bool   `json:"closed"`
	Active          bool   `json:"active"`
	NegRisk         bool   `json:"negRisk"`
	NegRiskMarketID string `json:"negRiskMarketID"`
}

// EventQuery filters ListSeriesEvents.
type EventQuery struct {
	SeriesID  string // required
	Closed    *bool  // nil = server default; use false for "open + future"
	Limit     int    // default 100, max 500
	Offset    int
	Order     string // e.g. "endDate"
	Ascending bool
}
```

Notes:

* `outcomes` and `clobTokenIds` arrive as **JSON-encoded strings**, not arrays. Provide helpers:
  `func (m GammaMarket) OutcomeList() ([]string, error)` and
  `func (m GammaMarket) TokenIDList() ([]string, error)` (index-aligned: for a Yes/No market,
  index 0 = YES, 1 = NO).
* Keep `GammaMarket` separate from `connector.Market` — `Market` is the CLOB-shaped type used by
  `GetMarket`; do not change its existing field semantics.

### 1.3 New methods on `PolymarketConnector`

```go
// GetSeries fetches series by slug. Returns an empty slice (nil error) when not found.
func (p *PolymarketConnector) GetSeries(ctx context.Context, slug string) ([]connector.GammaSeries, error)

// ListSeriesEvents lists the events of a series, newest-window-last per Order.
// Paginates internally until Limit is reached or the server returns a short page.
func (p *PolymarketConnector) ListSeriesEvents(ctx context.Context, q connector.EventQuery) ([]connector.GammaEvent, error)

// GetEvent fetches a single event (with nested markets) by slug or ticker.
func (p *PolymarketConnector) GetEvent(ctx context.Context, slug string) (*connector.GammaEvent, error)
```

Behaviour requirements:

1. **Caching.** `slug → series ID` is stable; cache with a TTL (suggest 1h) and an explicit
   `InvalidateSeries(slug string)` hook. `ListSeriesEvents` must not be cached (it is live state).
2. **Pagination.** Respect `Limit`; stop on a short page or on a page whose events are all `Closed`
   when `Closed == false`. Never return duplicates across pages.
3. **Errors.** Empty result is `(nil, nil)`-ish, not an error. Surface non-2xx as a typed error
   carrying status + URL. Honour `ctx` cancellation.
4. **Concurrency-safe** — the collector calls this from a scheduler goroutine while the WS clients
   are running.

### 1.4 Extend the market metadata the collector can see

`connector.Market` (used by `GetMarket`) currently exposes only
`ID, Slug, Question, ConditionID, YesAssetID, NoAssetID, Outcomes, TickSize, IsResolved, Resolution`.

The collector needs these additional fields, either on `Market` or reachable via `GammaMarket`:

| field | why |
|---|---|
| `GroupItemTitle` | the **strike** label ("80,000") — identifies the ladder rung; without it a recorded ladder cannot be labelled |
| `NegRisk`, `NegRiskMarketID` | Price Range ladders are neg-risk events |
| `EventSlug` / `EventTicker` | groups the 11 markets of one ladder event |
| `Description` | the **machine-readable resolution rule** (settlement anchor + source) |
| `StartDate`, `EndDate` | trading window vs settle instant |

Minimum ask: expose them on `GammaMarket` (1.2) and let the collector go through
`GetEvent`/`ListSeriesEvents`. Extending `connector.Market` is optional.

Verified example to test against — `GET /events?slug=bitcoin-above-on-october-3-2026`:

* `seriesSlug = btc-multi-strikes-weekly`, `startDate = 2026-09-26T16:00:15Z`, `endDate = 2026-10-03T16:00:00Z`
* **11 markets**, `groupItemTitle` = `74,000 … 94,000`, each `outcomes = ["Yes","No"]`, 2 CLOB tokens
* `GET /events?series_id=45&closed=false` → **7** open dated events (oct-2 … oct-8)

---

## 2. Binance kline stream (blocking)

The settlement rule for 5 of 6 BTC families is *"the final Close (or High) of the Binance
BTC/USDT 1-minute candle at 12:00 ET"*. go-collector records **5m** klines as an integrity
cross-check for the trade stream, and derives the 1m settle candle from `aggTrade`.

### 2.1 New event type

Follow the existing conventions exactly (`SeqID` + `ReceivedAt` first, decimal prices as `string`,
`Market` = `"spot"`/`"perp"`):

```go
// BinanceKlineEvent is a kline/candlestick update from Binance's kline stream.
type BinanceKlineEvent struct {
	SeqID       int64     `json:"seq_id"`       // monotonic sequence for ordered DES replay
	ReceivedAt  time.Time `json:"received_at"`  // local arrival timestamp
	Symbol      string    `json:"symbol"`       // e.g. "BTCUSDT"
	Market      string    `json:"market"`       // "spot" or "perp"
	Interval    string    `json:"interval"`     // "5m"
	OpenTime    time.Time `json:"open_time"`    // kline open time (exchange)
	CloseTime   time.Time `json:"close_time"`   // kline close time (exchange)
	Open        string    `json:"open"`         // decimal string
	High        string    `json:"high"`
	Low         string    `json:"low"`
	Close       string    `json:"close"`
	Volume      string    `json:"volume"`        // base asset volume
	QuoteVolume string    `json:"quote_volume"`
	TradeCount  int64     `json:"trade_count"`
	IsFinal     bool      `json:"is_final"`      // Binance "x": candle closed
	Timestamp   time.Time `json:"timestamp"`     // exchange event time
}
```

### 2.2 Subscribe API

```go
// SubscribeKlines subscribes to kline updates for the given symbols and interval.
// interval is one of "1m","5m","15m","1h","4h","1d" (reject others).
func (w *WSBinance) SubscribeKlines(ctx context.Context, mkt MarketType, symbols []string, interval string) error

// UnsubscribeKlines stops kline subscriptions for the given symbols/interval.
func (w *WSBinance) UnsubscribeKlines(ctx context.Context, mkt MarketType, symbols []string, interval string) error
```

* Stream name: `<symbol>@kline_5m` (lowercase symbol), combined-stream form
  `wss://stream.binance.com:9443/stream?streams=btcusdt@kline_5m` for spot; mirror whatever the
  existing `SubscribeTrades` / `SubscribePartialDepth` implementation does for perp.
* Validate `interval` against an allow-list and return an error for anything else — do **not**
  pass the string through to the URL unchecked.
* Route through the same dispatcher (`SetDispatcher`) as the other Binance events, and reuse the
  existing SeqID counter so ordering stays globally consistent.

### 2.3 Deliver every kline update; do **not** filter to final

Binance pushes kline updates continuously (~1/s) and flags the last one with `x: true`.
**The connector should emit all of them** and let the consumer filter on `IsFinal`. go-collector
persists only `IsFinal` messages, but keeping the connector dumb leaves intra-candle updates
available to other consumers.

---

## 3. `variantDuration` allow-list (optional)

`variantDuration()` in `pkg/polymarket/crypto_price.go` accepts only
`fiveminute | fifteen | hourly | daily`, so `GetCryptoPrice` rejects `4h` and weekly variants
**locally**.

Important caveat discovered while testing: the upstream
`/api/crypto/price-history` is **lenient** — an unknown `variant` does not error, it silently
falls back to a default candle granularity (`variant=4h` and `variant=hourly` return 60s points;
`daily` returns 300s points). So a wrong variant string produces **silently wrong granularity**
rather than an error. Keep the strict local allow-list.

Add `"4h"` (and `"weekly"` if you want it) only if something still calls `GetCryptoPrice` for those
families — go-collector is **dropping** its `fetchWindowPrices` call, so this is low priority.

---

## 4. Versioning & integration

* Requested release: **v0.6.0** — purely additive (new types + methods + one new event type), no
  changes to existing exported signatures.
* go-collector will then `go get github.com/algoboy-kevin/go-exchange-connector@v0.6.0`.
* During development either pin a pseudo-version or add
  `replace github.com/algoboy-kevin/go-exchange-connector => ../go-exchange-connector`.
* No changes needed to the crypto-price, RTDS, Chainlink or CLOB auth APIs.

---

## 5. Interfaces go-collector will code against

So that go-collector is not blocked on v0.6.0, it defines these locally and ships a minimal
stand-in implementation (a small Gamma HTTP client) behind them. When v0.6.0 lands, the
`*polymarket.PolymarketConnector` implementation is swapped in — no other code changes.

```go
// SeriesDiscovery is the subset of the connector go-collector needs for scheduling.
type SeriesDiscovery interface {
	GetSeries(ctx context.Context, slug string) ([]connector.GammaSeries, error)
	ListSeriesEvents(ctx context.Context, q connector.EventQuery) ([]connector.GammaEvent, error)
}

// KlineSubscriber is the subset needed for the settlement cross-check.
type KlineSubscriber interface {
	SubscribeKlines(ctx context.Context, mkt binance.MarketType, symbols []string, interval string) error
}
```

Please keep the names, signatures and JSON field names in §1 and §2 exactly as specified — the
swap-in then becomes a one-line change on our side.

---

## 6. Test checklist

Gamma:

- [ ] `GetSeries("btc-up-or-down-daily")` → 1 series, `recurrence == "daily"`.
- [ ] `GetSeries("does-not-exist")` → empty result, **nil error**.
- [ ] `ListSeriesEvents({SeriesID:"45", Closed:false})` → ≥7 events; each has 11 markets with
      **distinct** `GroupItemTitle` and exactly 2 CLOB token IDs.
- [ ] `OutcomeList()` / `TokenIDList()` are index-aligned and handle the JSON-string encoding.
- [ ] 403 regression: request without a `User-Agent` fails, with one succeeds.
- [ ] Pagination returns no duplicates and respects `Limit`.
- [ ] `ctx` cancellation aborts the request.

Klines:

- [ ] `SubscribeKlines(spot, ["BTCUSDT"], "5m")` → `IsFinal` messages arrive at 5m boundaries.
- [ ] `OpenTime` sits exactly on the 5m grid; `CloseTime - OpenTime == 5m - 1ms`.
- [ ] OHLCV parse as decimal strings (no float precision loss).
- [ ] Invalid interval (`"7m"`) → error, no subscription.
- [ ] `UnsubscribeKlines` stops delivery.
- [ ] Events reach the dispatcher with a monotonically increasing `SeqID` shared with the other
      Binance events.

---

## 7. Open questions

1. Should `ListSeriesEvents` default `Closed` to `false` when `EventQuery.Closed == nil`?
   (go-collector always sets it explicitly, so either is fine.)
2. Is a `User-Agent` already set on the shared Gamma HTTP client? If not, that is a one-line fix
   but it is a hard 403 otherwise.
3. Are you happy for `GammaEvent`/`GammaSeries` to live in the root `connector` package alongside
   `Market`, or would you rather they sit under a `pkg/gamma` sub-package? go-collector only needs
   the interface, so either works.
