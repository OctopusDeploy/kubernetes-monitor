package utilities

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestConcurrentMap_Set_AddsValueForKey(t *testing.T) {
	cm := NewConcurrentMap[string, int]()

	cm.Set("Test", 1)
	value, ok := cm.Get("Test")

	if !ok {
		t.Errorf("Expected to find value for key 'Test', but got none")
	}

	if value != 1 {
		t.Errorf("Expected value 1, but got %d", value)
	}
}

func TestConcurrentMap_Set_ReplacesValueForExistingKey(t *testing.T) {
	cm := NewConcurrentMap[string, int]()

	cm.Set("Test", 1)
	value, ok := cm.Get("Test")

	if !ok {
		t.Errorf("Expected to find value for key 'Test', but got none")
	}

	if value != 1 {
		t.Errorf("Expected value 1, but got %d", value)
	}

	cm.Set("Test", 2)
	value, ok = cm.Get("Test")
	if !ok {
		t.Errorf("Expected to find value for key 'Test', but got none")
	}

	if value != 2 {
		t.Errorf("Expected value 2, but got %d", value)
	}
}

func TestConcurrentMap_ConcurrentAccessDoesNotThrow(t *testing.T) {
	cm := NewConcurrentMap[int, int]()
	totalNumber := 10000

	go func() {
		for {
			cm.Get(0)
		}
	}()

	for i := 0; i < totalNumber; i++ {
		go cm.Set(i, i)
		go cm.Get(i)
	}

	// Wait for all keys to be added
	for len(cm.GetAll()) < totalNumber {
	}

	if len(cm.GetAll()) != totalNumber {
		t.Errorf("Expected %d keys, but got %d", totalNumber, len(cm.GetAll()))
	}
}

func TestConcurrentMap_ReplaceAll_ReplacesAllEntries(t *testing.T) {
	cm := NewConcurrentMap[string, int]()
	cm.Set("Test", 1)
	replacement := map[string]int{
		"Replaced": 1,
	}

	cm.ReplaceAll(replacement)

	original, ok := cm.Get("Test")
	if ok || original != 0 {
		t.Errorf("Expected to not find value for key 'Test', but got %d", original)
	}

	replacedValue, ok := cm.Get("Replaced")
	if !ok {
		t.Errorf("Expected to find value for key 'Replaced', but got none")
	}
	if replacedValue != 1 {
		t.Errorf("Expected value 1, but got %d", replacedValue)
	}
}

func TestConcurrentMap_Get_ReturnsValueForSpecifiedKey(t *testing.T) {
	cm := NewConcurrentMap[string, int]()
	cm.Set("Test", 1)

	value, ok := cm.Get("Test")
	if !ok {
		t.Errorf("Expected to find value for key 'Test', but got none")
	}

	if value != 1 {
		t.Errorf("Expected value 1, but got %d", value)
	}

	_, ok = cm.Get("NonExistent")
	if ok {
		t.Errorf("Expected to not find value for key 'NonExistent', but got one")
	}
}

func TestConcurrentMap_Get_ReturnsZeroValueForSpecifiedKey_WhenKeyDoesNotExist(t *testing.T) {
	intCm := NewConcurrentMap[string, int]()
	stringCm := NewConcurrentMap[string, string]()
	boolCm := NewConcurrentMap[string, bool]()
	pointerCm := NewConcurrentMap[string, *int]()

	intValue, _ := intCm.Get("NonExistent")
	if intValue != 0 {
		t.Errorf("Expected zero value for key 'NonExistent', but got a non-zero value")
	}

	stringValue, _ := stringCm.Get("NonExistent")
	if stringValue != "" {
		t.Errorf("Expected zero value for key 'NonExistent', but got a non-zero value")
	}

	boolValue, _ := boolCm.Get("NonExistent")
	if boolValue != false {
		t.Errorf("Expected zero value for key 'NonExistent', but got a non-zero value")
	}

	pointerValue, _ := pointerCm.Get("NonExistent")
	if pointerValue != nil {
		t.Errorf("Expected zero value for key 'NonExistent', but got a non-nil pointer")
	}
}

func TestConcurrentMap_GetAll_ReturnsSliceOfAllValues(t *testing.T) {
	cm := NewConcurrentMap[string, int]()
	cm.Set("Test1", 1)
	cm.Set("Test2", 2)

	values := cm.GetAll()
	expected := []int{1, 2}
	if diff := cmp.Diff(expected, values, cmpopts.SortSlices(SortIntSlice)); diff != "" {
		t.Error(diff)
	}
}

func TestConcurrentMap_Remove_SpecifiedKey(t *testing.T) {
	cm := NewConcurrentMap[string, int]()
	cm.Set("Test", 1)
	cm.Remove("Test")
	_, ok := cm.Get("Test")
	if ok {
		t.Errorf("Expected to not find value for key 'Test', but got one")
	}
}

func TestConcurrentMap_Exists_ChecksExistenceForSpecifiedKey(t *testing.T) {
	cm := NewConcurrentMap[string, int]()
	cm.Set("Test", 1)

	if !cm.Exists("Test") {
		t.Errorf("Expected to find value for key 'Test', but got none")
	}
	if cm.Exists("NonExistent") {
		t.Errorf("Expected to not find value for key 'NonExistent', but got one")
	}
}

func TestConcurrentMap_Iterate_InvokesCallbackFunctionForAllValues(t *testing.T) {
	cm := NewConcurrentMap[string, int]()
	cm.Set("Test1", 1)
	cm.Set("Test2", 2)
	var keys []string
	var values []int

	cm.Iterate(func(key string, value int) bool {
		keys = append(keys, key)
		values = append(values, value)
		return true
	})

	expectedKeys := []string{"Test1", "Test2"}
	expectedValues := []int{1, 2}
	if diff := cmp.Diff(expectedKeys, keys, cmpopts.SortSlices(SortStringSlice)); diff != "" {
		t.Error(diff)
	}

	if diff := cmp.Diff(expectedValues, values, cmpopts.SortSlices(SortIntSlice)); diff != "" {
		t.Error(diff)
	}
}

func TestConcurrentMap_Iterate_StopsIterationOnFalseReturn(t *testing.T) {
	cm := NewConcurrentMap[string, int]()
	cm.Set("Test1", 1)
	cm.Set("Test2", 2)

	iterationCount := 0

	cm.Iterate(func(key string, value int) bool {
		iterationCount++
		return false
	})

	if iterationCount != 1 {
		t.Errorf("Expected callback to be invoked once, but it was invoked %d times", iterationCount)
	}
}

func TestConcurrentMap_GetAsMap_ReturnsMap(t *testing.T) {
	cm := NewConcurrentMap[string, int]()
	cm.Set("Test1", 1)
	cm.Set("Test2", 2)

	expected := map[string]int{
		"Test1": 1,
		"Test2": 2,
	}
	actual := cm.GetAsMap()
	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Error(diff)
	}
}

func SortIntSlice(a, b int) bool {
	return a < b
}

func SortStringSlice(a, b string) bool {
	return a < b
}
