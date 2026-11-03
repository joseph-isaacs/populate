// Dumps Go math/rand outputs used by TSBS, for validating the Rust GoRand port.
// Usage: go run ./validate/gorand > tests/golden/gorand.txt
package main

import (
	"fmt"
	"math"
	"math/rand"
)

func main() {
	seeds := []int64{0, 1, 2, 123, -5, 108, 4294967296, math.MaxInt64, math.MinInt64, 2147483647}
	for _, s := range seeds {
		r := rand.New(rand.NewSource(s))
		fmt.Printf("seed %d\n", s)
		for i := 0; i < 20; i++ {
			fmt.Printf("int63 %d\n", r.Int63())
		}
		for i := 0; i < 20; i++ {
			fmt.Printf("float64 %x\n", math.Float64bits(r.Float64()))
		}
		for _, n := range []int{1, 2, 3, 4, 6, 9, 10, 100, 1000, 100000, 1 << 31, 3 << 33} {
			fmt.Printf("intn %d %d\n", n, r.Intn(n))
		}
		for _, n := range []int64{1, 2, 3, 20, 240, 20000, 1 << 40, 3 << 40} {
			fmt.Printf("int63n %d %d\n", n, r.Int63n(n))
		}
		for i := 0; i < 600; i++ {
			fmt.Printf("norm %x\n", math.Float64bits(r.NormFloat64()))
		}
		fmt.Printf("uint32 %d\n", r.Uint32())
	}
}
