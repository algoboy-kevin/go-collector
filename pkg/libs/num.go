package libs

import (
	"math"
	"strconv"
)

// ParseFloat converts a string to float64. Returns 0 on failure.
func ParseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

// Round rounds f to the given number of decimal places.
func Round(f float64, decimals int) float64 {
	pow := math.Pow(10, float64(decimals))
	return math.Round(f*pow) / pow
}

// R3 rounds to 3 decimal places.
func R3(x float64) float64 { return Round(x, 3) }
