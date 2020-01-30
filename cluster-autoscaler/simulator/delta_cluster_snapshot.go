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

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	schedulerlisters "k8s.io/kubernetes/pkg/scheduler/listers"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

// DeltaClusterSnapshot is implementation of ClusterSnapshot optimized for typical Cluster Autoscaler usage - (fork, add stuff, revert) repeated many times per loop.
//
// Complexity of some notable operations:
//	fork - O(1)
//	revert - O(1)
//	commit - O(n)
//	list all pods (no filtering) - O(n), cached
//	list all pods (with filtering) - O(n)
//	list node infos - O(n), cached
//	get all nodes - O(n)
//
// Watch out for:
//	node deletions, pod additions & deletions - invalidates cache of current snapshot
//		(when forked affects delta, but not base.)
//	pod affinity - causes scheduler framework to list pods with non-empty selector,
//		so basic caching doesn't help.
//
type DeltaClusterSnapshot struct {
	data *internalDeltaSnapshotData
}

type internalDeltaSnapshotDataNodeLister internalDeltaSnapshotData
type internalDeltaSnapshotDataPodLister internalDeltaSnapshotData

type internalDeltaSnapshotData struct {
	baseData *internalDeltaSnapshotData

	nodeInfoMap      map[string]*schedulernodeinfo.NodeInfo
	deletedNodeInfos map[string]bool

	nodeInfoList         []*schedulernodeinfo.NodeInfo
	podList              []*apiv1.Pod
	rawPods              [][]*apiv1.Pod
	rawPodsTotal         int
	havePodsWithAffinity []*schedulernodeinfo.NodeInfo
}

func (data *internalDeltaSnapshotData) getNodeInfo(name string) (*schedulernodeinfo.NodeInfo, error) {
	if data == nil {
		return nil, fmt.Errorf("node not found")
	}
	if nodeInfo, found := data.nodeInfoMap[name]; found {
		return nodeInfo, nil
	}
	if data.deletedNodeInfos[name] {
		return nil, fmt.Errorf("node not found")
	}
	return data.baseData.getNodeInfo(name)
}

func (data *internalDeltaSnapshotData) getNodeInfoList() []*schedulernodeinfo.NodeInfo {
	if data == nil {
		return nil
	}
	if data.nodeInfoList == nil {
		data.nodeInfoList = data.buildNodeInfoList()
	}
	return data.nodeInfoList
}

// Contains costly copying throughout the struct chain. Use wisely.
func (data *internalDeltaSnapshotData) buildNodeInfoList() []*schedulernodeinfo.NodeInfo {
	baseList := data.baseData.getNodeInfoList()
	totalLen := len(baseList) + len(data.nodeInfoMap)
	var nodeInfoList []*schedulernodeinfo.NodeInfo

	if len(data.deletedNodeInfos) > 0 {
		nodeInfoList = make([]*schedulernodeinfo.NodeInfo, 0, totalLen+100)
		for _, bni := range baseList {
			if data.deletedNodeInfos[bni.Node().Name] {
				continue
			}
			nodeInfoList = append(nodeInfoList, bni)
		}
	} else {
		nodeInfoList = make([]*schedulernodeinfo.NodeInfo, len(baseList), totalLen+100)
		copy(nodeInfoList, baseList)
	}

	for _, dni := range data.nodeInfoMap {
		nodeInfoList = append(nodeInfoList, dni)
	}

	return nodeInfoList
}

func (data *internalDeltaSnapshotDataNodeLister) List() ([]*schedulernodeinfo.NodeInfo, error) {
	return (*internalDeltaSnapshotData)(data).getNodeInfoList(), nil
}

func (data *internalDeltaSnapshotDataNodeLister) HavePodsWithAffinityList() ([]*schedulernodeinfo.NodeInfo, error) {
	if data.havePodsWithAffinity != nil {
		return data.havePodsWithAffinity, nil
	}

	nodeInfoList := (*internalDeltaSnapshotData)(data).getNodeInfoList()
	havePodsWithAffinityList := make([]*schedulernodeinfo.NodeInfo, 0, len(nodeInfoList))
	for _, node := range nodeInfoList {
		if len(node.PodsWithAffinity()) > 0 {
			havePodsWithAffinityList = append(havePodsWithAffinityList, node)
		}
	}
	data.havePodsWithAffinity = havePodsWithAffinityList
	return data.havePodsWithAffinity, nil
}

func (data *internalDeltaSnapshotDataNodeLister) Get(nodeName string) (*schedulernodeinfo.NodeInfo, error) {
	return (*internalDeltaSnapshotData)(data).getNodeInfo(nodeName)
}

func (data *internalDeltaSnapshotDataPodLister) List(selector labels.Selector) ([]*apiv1.Pod, error) {
	if data.podList == nil {
		data.podList = (*internalDeltaSnapshotData)(data).buildPodList()
	}

	if selector.Empty() {
		// no restrictions, yay
		return data.podList, nil
	}

	selectedPods := make([]*apiv1.Pod, 0, len(data.podList))
	for _, pod := range data.podList {
		if selector.Matches(labels.Set(pod.Labels)) {
			selectedPods = append(selectedPods, pod)
		}
	}
	return selectedPods, nil
}

func (data *internalDeltaSnapshotDataPodLister) FilteredList(podFilter schedulerlisters.PodFilter, selector labels.Selector) ([]*apiv1.Pod, error) {
	if data.podList == nil {
		data.podList = (*internalDeltaSnapshotData)(data).buildPodList()
	}

	selectedPods := make([]*apiv1.Pod, 0, len(data.podList))
	for _, pod := range data.podList {
		if podFilter(pod) && selector.Matches(labels.Set(pod.Labels)) {
			selectedPods = append(selectedPods, pod)
		}
	}
	return selectedPods, nil
}

func (data *internalDeltaSnapshotData) Pods() schedulerlisters.PodLister {
	return (*internalDeltaSnapshotDataPodLister)(data)
}

func (data *internalDeltaSnapshotData) NodeInfos() schedulerlisters.NodeInfoLister {
	return (*internalDeltaSnapshotDataNodeLister)(data)
}

// NewEmptySnapshot initializes a Snapshot struct and returns it.
func newInternalDeltaSnapshotData() *internalDeltaSnapshotData {
	return &internalDeltaSnapshotData{
		nodeInfoMap:      make(map[string]*schedulernodeinfo.NodeInfo),
		deletedNodeInfos: make(map[string]bool),
	}
}

// Convenience method to avoid writing loop for adding nodes.
func (data *internalDeltaSnapshotData) addNodes(nodes []*apiv1.Node) error {
	for _, node := range nodes {
		if err := data.addNode(node); err != nil {
			return err
		}
	}
	return nil
}

func (data *internalDeltaSnapshotData) addNode(node *apiv1.Node) error {
	nodeInfo := schedulernodeinfo.NewNodeInfo()
	if err := nodeInfo.SetNode(node); err != nil {
		return fmt.Errorf("cannot set node in NodeInfo: %v", err)
	}
	return data.addNodeInfo(nodeInfo)
}

func (data *internalDeltaSnapshotData) addNodeInfo(nodeInfo *schedulernodeinfo.NodeInfo) error {
	if _, found := data.nodeInfoMap[nodeInfo.Node().Name]; found {
		return fmt.Errorf("node %s already in snapshot", nodeInfo.Node().Name)
	}
	data.nodeInfoMap[nodeInfo.Node().Name] = nodeInfo
	if data.nodeInfoList != nil {
		data.nodeInfoList = append(data.nodeInfoList, nodeInfo)
	}
	return nil
}

func (data *internalDeltaSnapshotData) clearCaches() {
	data.nodeInfoList = nil
	data.clearPodCaches()
}

func (data *internalDeltaSnapshotData) clearPodCaches() {
	data.podList = nil
	data.rawPods = nil
	data.rawPodsTotal = 0
	data.havePodsWithAffinity = nil
}

func (data *internalDeltaSnapshotData) updateNode(node *schedulernodeinfo.NodeInfo) error {
	if _, found := data.nodeInfoMap[node.Node().Name]; found {
		data.removeNode(node.Node().Name)
	}

	return data.addNodeInfo(node)
}

func (data *internalDeltaSnapshotData) removeNode(nodeName string) error {
	_, found := data.nodeInfoMap[nodeName]
	if found {
		// If node was added or modified within this delta, delete this change.
		delete(data.nodeInfoMap, nodeName)
	}

	ni, err := data.baseData.getNodeInfo(nodeName)
	if err == nil && ni != nil {
		// If node was found in the underlying data, mark it as deleted in delta.
		data.deletedNodeInfos[nodeName] = true
	}

	if err != nil && !found {
		// Node not found in the chain.
		return err
	}

	// Maybe consider deleting from the lists instead. Maybe not.
	data.clearCaches()
	return nil
}

func (data *internalDeltaSnapshotData) addPod(pod *apiv1.Pod, nodeName string) error {
	if _, found := data.nodeInfoMap[nodeName]; !found {
		if ni, err := data.baseData.getNodeInfo(nodeName); err != nil {
			return err
		} else {
			data.nodeInfoMap[nodeName] = ni.Clone()
		}
	}

	data.nodeInfoMap[nodeName].AddPod(pod)
	if data.podList != nil || data.havePodsWithAffinity != nil {
		data.clearPodCaches()
	}
	return nil
}

func (data *internalDeltaSnapshotData) getNodeForPod(namespace, name string) (string, error) {
	if data == nil {
		return "", fmt.Errorf("pod %s/%s not in snapshot", namespace, name)
	}

	for nodeName, nodeInfo := range data.nodeInfoMap {
		for _, pod := range nodeInfo.Pods() {
			if pod.Name == name && pod.Namespace == namespace {
				return nodeName, nil
			}
		}
	}

	return data.baseData.getNodeForPod(namespace, name)
}

func (data *internalDeltaSnapshotData) removePod(namespace string, name string) error {
	nodeName, err := data.getNodeForPod(namespace, name)
	if err != nil {
		return err
	}

	if _, found := data.nodeInfoMap[nodeName]; !found {
		if ni, err := data.baseData.getNodeInfo(nodeName); err != nil {
			return err
		} else {
			data.nodeInfoMap[nodeName] = ni.Clone()
		}
	}

	nodeInfo, found := data.nodeInfoMap[nodeName]
	if !found {
		return fmt.Errorf("internal error: node not found")
	}

	preAffinityPods := len(nodeInfo.PodsWithAffinity())
	for _, pod := range nodeInfo.Pods() {
		if pod.Namespace == namespace && pod.Name == name {
			if err := nodeInfo.RemovePod(pod); err != nil {
				return fmt.Errorf("cannot remove pod; %v", err)
			}
			break
		}
	}

	// Maybe consider deleting from the list in the future. Maybe not.
	postAffinityPods := len(nodeInfo.PodsWithAffinity())
	if preAffinityPods == 1 && postAffinityPods == 0 {
		data.havePodsWithAffinity = nil
	}
	if data.podList != nil || data.rawPods != nil {
		data.clearPodCaches()
	}

	return nil
}

func (data *internalDeltaSnapshotData) getRawPods() ([][]*apiv1.Pod, int) {
	if data == nil {
		return [][]*apiv1.Pod{}, 0
	}
	if data.rawPods != nil {
		return data.rawPods, data.rawPodsTotal
	}
	basePods, baseCount := data.baseData.getRawPods()
	i := len(basePods)
	total := baseCount
	listCount := len(data.nodeInfoMap) + len(basePods)
	pods := make([][]*apiv1.Pod, listCount, listCount)
	copy(pods, basePods)
	for _, node := range data.nodeInfoMap {
		pods[i] = node.Pods()
		total += len(pods[i])
		i++
	}

	data.rawPods = pods
	data.rawPodsTotal = total
	return pods, total
}

func (data *internalDeltaSnapshotData) buildPodList() []*apiv1.Pod {
	pods, total := data.getRawPods()
	// Squash!
	podList := make([]*apiv1.Pod, total, total+1000)
	j := 0
	for i := 0; i < len(pods); i++ {
		copy(podList[j:], pods[i])
		j += len(pods[i])
	}
	return podList
}

func (data *internalDeltaSnapshotData) getAllPods() ([]*apiv1.Pod, error) {
	if data.podList == nil {
		data.podList = data.buildPodList()
	}
	return data.podList, nil

}

func (data *internalDeltaSnapshotData) getAllNodes() ([]*apiv1.Node, error) {
	nodeInfos := data.getNodeInfoList()
	nodes := make([]*apiv1.Node, len(nodeInfos), len(nodeInfos))
	for i, nodeInfo := range nodeInfos {
		nodes[i] = nodeInfo.Node()
	}
	return nodes, nil
}

func (data *internalDeltaSnapshotData) fork() *internalDeltaSnapshotData {
	forkedData := newInternalDeltaSnapshotData()
	forkedData.baseData = data
	return forkedData
}

func (data *internalDeltaSnapshotData) commit() *internalDeltaSnapshotData {
	for _, node := range data.nodeInfoMap {
		data.baseData.updateNode(node)
	}
	for node, _ := range data.deletedNodeInfos {
		data.baseData.removeNode(node)
	}
	return data.baseData
}

// NewDeltaClusterSnapshot creates instances of DeltaClusterSnapshot.
func NewDeltaClusterSnapshot() *DeltaClusterSnapshot {
	snapshot := &DeltaClusterSnapshot{}
	_ = snapshot.Clear()
	return snapshot
}

// AddNode adds node to the snapshot.
func (snapshot *DeltaClusterSnapshot) AddNode(node *apiv1.Node) error {
	return snapshot.data.addNode(node)
}

// AddNodes adds nodes in batch to the snapshot.
func (snapshot *DeltaClusterSnapshot) AddNodes(nodes []*apiv1.Node) error {
	return snapshot.data.addNodes(nodes)
}

// AddNodeWithPods adds a node and set of pods to be scheduled to this node to the snapshot.
func (snapshot *DeltaClusterSnapshot) AddNodeWithPods(node *apiv1.Node, pods []*apiv1.Pod) error {
	if err := snapshot.AddNode(node); err != nil {
		return err
	}
	for _, pod := range pods {
		if err := snapshot.AddPod(pod, node.Name); err != nil {
			return err
		}
	}
	return nil
}

// RemoveNode removes nodes (and pods scheduled to it) from the snapshot.
func (snapshot *DeltaClusterSnapshot) RemoveNode(nodeName string) error {
	return snapshot.data.removeNode(nodeName)
}

// AddPod adds pod to the snapshot and schedules it to given node.
func (snapshot *DeltaClusterSnapshot) AddPod(pod *apiv1.Pod, nodeName string) error {
	return snapshot.data.addPod(pod, nodeName)
}

// RemovePod removes pod from the snapshot.
func (snapshot *DeltaClusterSnapshot) RemovePod(namespace string, podName string) error {
	return snapshot.data.removePod(namespace, podName)
}

// GetAllPods returns list of all the pods in snapshot
func (snapshot *DeltaClusterSnapshot) GetAllPods() ([]*apiv1.Pod, error) {
	return snapshot.data.getAllPods()
}

// GetAllNodes returns list of all the nodes in snapshot
func (snapshot *DeltaClusterSnapshot) GetAllNodes() ([]*apiv1.Node, error) {
	return snapshot.data.getAllNodes()
}

// Time: O(1)
// Fork creates a fork of snapshot state. All modifications can later be reverted to moment of forking via Revert()
// Forking already forked snapshot is not allowed and will result with an error.
func (snapshot *DeltaClusterSnapshot) Fork() error {
	if snapshot.data.baseData != nil {
		return fmt.Errorf("snapshot already forked")
	}
	snapshot.data = snapshot.data.fork()
	return nil
}

// Time: O(1)
// Revert reverts snapshot state to moment of forking.
func (snapshot *DeltaClusterSnapshot) Revert() error {
	if snapshot.data.baseData == nil {
		return fmt.Errorf("snapshot not forked")
	}
	snapshot.data = snapshot.data.baseData
	return nil

}

// Time: O(n), where n = size of delta (number of nodes added, modified or deleted since forking)
// Commit commits changes done after forking.
func (snapshot *DeltaClusterSnapshot) Commit() error {
	snapshot.data = snapshot.data.commit()
	return nil
}

// Time: O(1)
// Clear reset cluster snapshot to empty, unforked state
func (snapshot *DeltaClusterSnapshot) Clear() error {
	snapshot.data = newInternalDeltaSnapshotData()
	return nil
}

// GetSchedulerLister exposes snapshot state as scheduler's SharedLister.
func (snapshot *DeltaClusterSnapshot) GetSchedulerLister() (schedulerlisters.SharedLister, error) {
	return snapshot.data, nil
}
