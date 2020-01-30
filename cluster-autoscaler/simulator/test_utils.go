package simulator

import (
	"testing"

	apiv1 "k8s.io/api/core/v1"

	"github.com/stretchr/testify/assert"
)

// InitializeClusterSnapshot clears cluster snapshot and then initializes it with given set of nodes and pods.
// Both Spec.NodeName and Status.NominatedNodeName are used when simulating scheduling pods.
func InitializeClusterSnapshot(
	t *testing.T,
	snapshot ClusterSnapshot,
	nodes []*apiv1.Node,
	pods []*apiv1.Pod) {
	initializeClusterSnapshot(t, snapshot, nodes, pods, true)
}

// InitializeClusterSnapshotNoError behaves like InitializeClusterSnapshot but does not report error from AddPod
func InitializeClusterSnapshotNoError(
	t *testing.T,
	snapshot ClusterSnapshot,
	nodes []*apiv1.Node,
	pods []*apiv1.Pod) {
	initializeClusterSnapshot(t, snapshot, nodes, pods, false)
}

func initializeClusterSnapshot(
	t *testing.T,
	snapshot ClusterSnapshot,
	nodes []*apiv1.Node,
	pods []*apiv1.Pod,
	reportErrors bool) {

	var err error

	snapshot.Clear()

	for _, node := range nodes {
		err = snapshot.AddNode(node)
		assert.NoError(t, err, "error while adding node %s", node.Name)
	}

	for _, pod := range pods {
		if pod.Spec.NodeName != "" {
			err = snapshot.AddPod(pod, pod.Spec.NodeName)
			if reportErrors {
				assert.NoError(t, err, "error while adding pod %s/%s to node %s", pod.Namespace, pod.Name, pod.Spec.NodeName)
			}
		} else if pod.Status.NominatedNodeName != "" {
			err = snapshot.AddPod(pod, pod.Status.NominatedNodeName)
			if reportErrors {
				assert.NoError(t, err, "error while adding pod %s/%s to nominated node %s", pod.Namespace, pod.Name, pod.Status.NominatedNodeName)
			}
		} else {
			assert.Fail(t, "pod %s/%s does not have Spec.NodeName nor Status.NominatedNodeName set", pod.Namespace, pod.Name)
		}
	}
}
