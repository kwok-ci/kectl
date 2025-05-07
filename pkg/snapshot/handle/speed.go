/*
Copyright 2024 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package handle

import (
	"math"
)

// Speed represents the speed of a handle.
type Speed float64

const (
	speedBase   = 0.0001
	speedOffset = 100000
)

// Up increases the speed value in a logarithmic manner, ensuring it never goes below the base speed.
// It scales the speed by speedOffset, calculates an appropriate step size based on the number of digits,
// increments the value, and then scales back down to return the new speed.
func (s Speed) Up() Speed {
	if s < speedBase {
		return speedBase
	}

	n := float64(s)
	n *= speedOffset
	step := math.Pow(10, float64(digitCount(int64(math.Round(n)))-2))
	step = math.Max(step, 10)
	n += step
	n = math.Round(n)
	n /= speedOffset
	return Speed(n)
}

// Down decreases the speed value in a logarithmic manner, ensuring it never goes below 0.
// It scales the speed by speedOffset, calculates an appropriate step size based on the number of digits,
// decrements the value, and then scales back down to return the new speed.
// If the speed is already at or below the base speed, it returns 0.
func (s Speed) Down() Speed {
	if s <= speedBase {
		return 0
	}

	n := float64(s)
	n *= speedOffset
	step := math.Pow(10, float64(digitCount(int64(math.Round(n)-speedBase))-2))
	step = math.Max(step, 10)
	n -= step
	n = math.Round(n)
	n /= speedOffset
	return Speed(n)
}

// digitCount calculates the number of digits in number.
func digitCount(i int64) int64 {
	if i <= 0 {
		return 0
	}
	return int64(math.Log10(float64(i))) + 1
}
