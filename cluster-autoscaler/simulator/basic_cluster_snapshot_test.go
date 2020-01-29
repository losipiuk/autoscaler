/*
Copyright 2020 The Kubernetes Authors.

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

package simulator

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	schedulerlisters "k8s.io/kubernetes/pkg/scheduler/listers"

	apiv1 "k8s.io/api/core/v1"
)

func createTestNodesWithPrefix(name string, n int) []*apiv1.Node {
	nodes := make([]*apiv1.Node, n, n)
	for i := 0; i < n; i++ {
		nodes[i] = BuildTestNode(fmt.Sprintf("%s-%d", name, i), 2000, 2000000)
		SetNodeReadyState(nodes[i], true, time.Time{})
	}
	return nodes
}

func createTestNodes(n int) []*apiv1.Node {
	return createTestNodesWithPrefix("n", n)
}

func createTestPods(n int) []*apiv1.Pod {
	pods := make([]*apiv1.Pod, n, n)
	for i := 0; i < n; i++ {
		pods[i] = BuildTestPod(fmt.Sprintf("p-%d", i), 1000, 2000000)
	}
	return pods
}

func assignPodsToNodes(pods []*apiv1.Pod, nodes []*apiv1.Node) {
	j := 0
	for i := 0; i < len(pods); i++ {
		if j >= len(nodes) {
			j = 0
		}
		pods[i].Spec.NodeName = nodes[j].Name
		j++
	}
}

func BenchmarkAddNodes(b *testing.B) {
	testCases := []int{1, 10, 100, 1000, 5000, 15000, 100000}
	snapshots := map[string]forkingSnapshotFactory{
		"basic": func() forkingSnapshot { return NewBasicClusterSnapshot() },
		"delta": func() forkingSnapshot { return NewDeltaClusterSnapshot() },
	}

	for snapshotName, snapshotFactory := range snapshots {
		for _, tc := range testCases {
			nodes := createTestNodes(tc)
			clusterSnapshot := snapshotFactory()
			b.ResetTimer()
			b.Run(fmt.Sprintf("%s: AddNode() %d", snapshotName, tc), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					clusterSnapshot.Clear()
					b.StartTimer()
					for _, node := range nodes {
						err := clusterSnapshot.AddNode(node)
						if err != nil {
							assert.NoError(b, err)
						}
					}
				}
			})
		}
	}
	for snapshotName, snapshotFactory := range snapshots {
		for _, tc := range testCases {
			nodes := createTestNodes(tc)
			clusterSnapshot := snapshotFactory()
			b.ResetTimer()
			b.Run(fmt.Sprintf("%s: AddNodes() %d", snapshotName, tc), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					clusterSnapshot.Clear()
					b.StartTimer()
					err := clusterSnapshot.AddNodes(nodes)
					if err != nil {
						assert.NoError(b, err)
					}
				}
			})
		}
	}
}

func BenchmarkListNodeInfos(b *testing.B) {
	testCases := []int{1, 10, 100, 1000, 5000, 15000, 100000}
	snapshots := map[string]forkingSnapshotFactory{
		"basic": func() forkingSnapshot { return NewBasicClusterSnapshot() },
		"delta": func() forkingSnapshot { return NewDeltaClusterSnapshot() },
	}

	for snapshotName, snapshotFactory := range snapshots {
		for _, tc := range testCases {
			nodes := createTestNodes(tc)
			clusterSnapshot := snapshotFactory()
			_ = clusterSnapshot.AddNodes(nodes)
			b.ResetTimer()
			b.Run(fmt.Sprintf("%s: List() %d", snapshotName, tc), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					lister, _ := clusterSnapshot.GetSchedulerLister()
					_, _ = lister.NodeInfos().List()
				}
			})
		}
	}
}

func BenchmarkAddPods(b *testing.B) {
	testCases := []int{1, 10, 100, 1000, 5000, 15000}
	snapshots := map[string]forkingSnapshotFactory{
		"basic": func() forkingSnapshot { return NewBasicClusterSnapshot() },
		"delta": func() forkingSnapshot { return NewDeltaClusterSnapshot() },
	}

	for snapshotName, snapshotFactory := range snapshots {
		for _, tc := range testCases {
			clusterSnapshot := snapshotFactory()
			nodes := createTestNodes(tc)
			err := clusterSnapshot.AddNodes(nodes)
			assert.NoError(b, err)
			pods := createTestPods(tc * 30)
			assignPodsToNodes(pods, nodes)
			b.ResetTimer()
			b.Run(fmt.Sprintf("%s: AddPod() 30*%d", snapshotName, tc), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					err = clusterSnapshot.Clear()
					if err != nil {
						assert.NoError(b, err)
					}

					err = clusterSnapshot.AddNodes(nodes)
					if err != nil {
						assert.NoError(b, err)
					}
					/*
						// uncomment to test effect of pod caching
							_, err = clusterSnapshot.GetAllPods()
							if err != nil {
								assert.NoError(b, err)
							}
					*/
					b.StartTimer()
					for _, pod := range pods {
						err = clusterSnapshot.AddPod(pod, pod.Spec.NodeName)
						if err != nil {
							assert.NoError(b, err)
						}
					}
				}
			})
		}
	}
}

func BenchmarkForkAddRevert(b *testing.B) {
	nodeTestCases := []int{1, 10, 100, 1000, 5000, 15000, 100000}
	podTestCases := []int{0, 1, 30}
	snapshots := map[string]forkingSnapshotFactory{
		"basic": func() forkingSnapshot { return NewBasicClusterSnapshot() },
		"delta": func() forkingSnapshot { return NewDeltaClusterSnapshot() },
	}

	for snapshotName, snapshotFactory := range snapshots {
		for _, ntc := range nodeTestCases {
			nodes := createTestNodes(ntc)
			for _, ptc := range podTestCases {
				pods := createTestPods(ntc * ptc)
				assignPodsToNodes(pods, nodes)
				clusterSnapshot := snapshotFactory()
				err := clusterSnapshot.AddNodes(nodes)
				assert.NoError(b, err)
				for _, pod := range pods {
					err = clusterSnapshot.AddPod(pod, pod.Spec.NodeName)
					assert.NoError(b, err)
				}
				tmpNode := BuildTestNode("tmp", 2000, 2000000)
				b.ResetTimer()
				b.Run(fmt.Sprintf("%s: ForkAddRevert (%d nodes, %d pods)", snapshotName, ntc, ptc), func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						err = clusterSnapshot.Fork()
						if err != nil {
							assert.NoError(b, err)
						}
						err = clusterSnapshot.AddNode(tmpNode)
						if err != nil {
							assert.NoError(b, err)
						}
						err = clusterSnapshot.Revert()
						if err != nil {
							assert.NoError(b, err)
						}
					}
				})
			}
		}
	}
}

type forkingSnapshotFactory func() forkingSnapshot

type forkingSnapshot interface {
	Fork() error
	Revert() error
	AddNode(*apiv1.Node) error
	AddNodes([]*apiv1.Node) error
	AddPod(*apiv1.Pod, string) error
	GetAllNodes() ([]*apiv1.Node, error)
	GetAllPods() ([]*apiv1.Pod, error)
	Clear() error
	GetSchedulerLister() (schedulerlisters.SharedLister, error)
}

func TestFork(t *testing.T) {
	nodeCount := 3
	podCount := 90

	nodes := createTestNodes(nodeCount)
	extraNodes := createTestNodesWithPrefix("tmp", 2)
	pods := createTestPods(podCount)
	assignPodsToNodes(pods, nodes)

	snapshots := map[string]forkingSnapshotFactory{
		"basic": func() forkingSnapshot { return NewBasicClusterSnapshot() },
		"delta": func() forkingSnapshot { return NewDeltaClusterSnapshot() },
	}

	for name, snapshotFactory := range snapshots {
		t.Run(fmt.Sprintf("%s: fork should not affect base data: adding nodes", name),
			func(t *testing.T) {
				clusterSnapshot := snapshotFactory()
				_ = clusterSnapshot.AddNodes(nodes)
				_ = clusterSnapshot.Fork()
				for _, node := range extraNodes {
					_ = clusterSnapshot.AddNode(node)
				}
				forkNodes, err := clusterSnapshot.GetAllNodes()
				assert.NoError(t, err)
				assert.Equal(t, nodeCount+len(extraNodes), len(forkNodes))
				_ = clusterSnapshot.Revert()
				baseNodes, err := clusterSnapshot.GetAllNodes()
				assert.NoError(t, err)
				assert.Equal(t, nodeCount, len(baseNodes))
			})
		t.Run(fmt.Sprintf("%s: fork should not affect base data: adding pods", name),
			func(t *testing.T) {
				clusterSnapshot := snapshotFactory()
				_ = clusterSnapshot.AddNodes(nodes)
				_ = clusterSnapshot.Fork()
				for _, pod := range pods {
					_ = clusterSnapshot.AddPod(pod, pod.Spec.NodeName)
				}
				forkPods, err := clusterSnapshot.GetAllPods()
				assert.NoError(t, err)
				assert.Equal(t, len(pods), len(forkPods))
				_ = clusterSnapshot.Revert()
				basePods, err := clusterSnapshot.GetAllPods()
				assert.NoError(t, err)
				assert.Equal(t, 0, len(basePods))
			})
	}

}
