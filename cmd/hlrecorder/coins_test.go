package main

import (
	"reflect"
	"testing"

	"github.com/algoboy-kevin/go-collector/pkg/services/hyperliquid"
)

func TestApplyCoins(t *testing.T) {
	cfgCoins := []string{"SOL", "HYPE"}
	tests := []struct {
		name    string
		flag    string
		want    []string
		wantErr bool
	}{
		{"empty flag keeps config", "", cfgCoins, false},
		{"single", "BTC", []string{"BTC"}, false},
		{"trims whitespace", " BTC , ETH ", []string{"BTC", "ETH"}, false},
		{"preserves order", "ETH,BTC,AAA", []string{"ETH", "BTC", "AAA"}, false},
		{"dex prefixed passthrough", "BTC,ETH,xyz:JP225,vntl:OPENAI", []string{"BTC", "ETH", "xyz:JP225", "vntl:OPENAI"}, false},
		{"replaces not appends", "BTC", []string{"BTC"}, false},
		{"empty middle entry", "BTC,,ETH", cfgCoins, true},
		{"trailing comma", "BTC,", cfgCoins, true},
		{"leading comma", ",BTC", cfgCoins, true},
		{"whitespace-only entry", "BTC, ,ETH", cfgCoins, true},
		{"all-whitespace flag", " ", cfgCoins, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &hyperliquid.Config{Coins: append([]string(nil), cfgCoins...)}
			err := applyCoins(cfg, tc.flag)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if !reflect.DeepEqual(cfg.Coins, tc.want) {
				t.Errorf("cfg.Coins = %v, want %v", cfg.Coins, tc.want)
			}
		})
	}
}
