package main

import (
	"fmt"
	"strings"

	"github.com/algoboy-kevin/go-collector/pkg/services/hyperliquid"
)

// parseCoins splits the -coins value into bare venue symbols, preserving order. It is
// syntax only: entries are trimmed of ASCII spaces and an empty entry is an error. Whether
// a coin exists is the venue's call (BuildSubscriptions and the startup universe check).
func parseCoins(s string) ([]string, error) {
	parts := strings.Split(s, ",")
	coins := make([]string, 0, len(parts))
	for i, p := range parts {
		p = strings.Trim(p, " ")
		if p == "" {
			return nil, fmt.Errorf("-coins: entry %d is empty in %q (want e.g. BTC,ETH,xyz:JP225)", i+1, s)
		}
		coins = append(coins, p)
	}
	return coins, nil
}

// applyCoins replaces cfg.Coins with the -coins value. An empty flag leaves the config's
// coin list untouched; a non-empty one replaces it entirely.
func applyCoins(cfg *hyperliquid.Config, coinsFlag string) error {
	if coinsFlag == "" {
		return nil
	}
	coins, err := parseCoins(coinsFlag)
	if err != nil {
		return err
	}
	cfg.Coins = coins
	return nil
}
