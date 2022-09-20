package fuzz

import (
	"testing"
	// "fmt"
)

/*
as per Fuzz test requirement, function name is in "FuzzXxx" format
takes only one input, a *testing.F var
*/
func FuzzTest(f *testing.F) {
	/*
	optional seed corpus addition
	need match the fuzzing arguments' types in exact order
	*/
	// f.Add(200, 100, 300, 100, 20, 200, 500, 1000)

	/*
	fuzz target
	first argument is *testing.T var
	arguments that follow are data that need to be randomized
	for fuzz test use
	*/
	f.Fuzz(func(t *testing.T, MaxInitCount int, ChangeCount int, QueryCount int, IterCount int, IterDistance int, MaxDelCount int, MinLen int, MaxLen int) {
		var testConfig = FuzzConfig{
			MaxInitCount,
			ChangeCount, 
			QueryCount,  
			IterCount,   
			IterDistance,
			MaxDelCount, 
			MinLen,      
			MaxLen,    
		}

		runTest(testConfig)
	})
}
