/*
Copyright 2018 The Kubernetes Authors.

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

package cloudprovider

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResourceLimiterGetResources(t *testing.T) {
	limiter := NewResourceLimiter(map[string]int64{"a": 1, "b": 2}, map[string]int64{"b": 2, "c": 2})
	expected := limiter.GetResources()
	actual := []string{"a", "b", "c"}
	assert.Equal(t, len(actual), len(expected))
	assert.Subset(t, actual, expected)
}
