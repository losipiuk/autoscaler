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

package gce

import (
	"reflect"
	"sync"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"

	gce "google.golang.org/api/compute/v1"
	"k8s.io/klog"
)

// MachineTypeKey is used to identify MachineType.
type MachineTypeKey struct {
	Zone        string
	MachineType string
}

// GceCache is used for caching cluster resources state.
//
// It is needed to:
// - keep track of autoscaled MIGs in the cluster,
// - keep track of instances and which MIG they belong to,
// - limit repetitive GCE API calls.
//
// Cached resources:
// 1) MIG configuration,
// 2) instance->MIG mapping,
// 3) resource limits (self-imposed quotas),
// 4) machine types.
//
// How it works:
// - migs (1), resource limits (3) and machine types (4) are only stored in this cache,
// not updated by it.
// - instanceRefToMigRef (2) is based on registered migs (1). For each mig, its instances
// are fetched from GCE API using gceService.
// - instanceRefToMigRef (2) is NOT updated automatically when migs field (1) is updated. Calling
// RegenerateInstancesCache is required to sync it with registered migs.
type GceCache struct {
	cacheMutex sync.Mutex

	// Cache content.
	migs                map[GceRef]Mig
	instanceRefToMigRef map[GceRef]GceRef
	resourceLimiter     *cloudprovider.ResourceLimiter
	machinesCache       map[MachineTypeKey]*gce.MachineType
	migTargetSizeCache  map[GceRef]int64
	migBaseNameCache    map[GceRef]string
}

// NewGceCache creates empty GceCache.
func NewGceCache(gceService AutoscalingGceClient) GceCache {
	return GceCache{
		migs:                map[GceRef]Mig{},
		instanceRefToMigRef: map[GceRef]GceRef{},
		machinesCache:       map[MachineTypeKey]*gce.MachineType{},
		migTargetSizeCache:  map[GceRef]int64{},
		migBaseNameCache:    map[GceRef]string{},
	}
}

//  Methods locking on migsMutex.

// RegisterMig returns true if the node group wasn't in cache before, or its config was updated.
func (gc *GceCache) RegisterMig(newMig Mig) bool {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()

	oldMig, found := gc.migs[newMig.GceRef()]
	if found {
		if !reflect.DeepEqual(oldMig, newMig) {
			gc.migs[newMig.GceRef()] = newMig
			klog.V(4).Infof("Updated Mig %s", newMig.GceRef().String())
			return true
		}
		return false
	}

	klog.V(1).Infof("Registering %s", newMig.GceRef().String())
	gc.migs[newMig.GceRef()] = newMig
	return true
}

// UnregisterMig returns true if the node group has been removed, and false if it was already missing from cache.
func (gc *GceCache) UnregisterMig(toBeRemoved Mig) bool {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()

	_, found := gc.migs[toBeRemoved.GceRef()]
	if found {
		klog.V(1).Infof("Unregistered Mig %s", toBeRemoved.GceRef().String())
		delete(gc.migs, toBeRemoved.GceRef())
		gc.invalidateMigInstancesNoLock(toBeRemoved.GceRef())
		return true
	}
	return false
}

// GetMigs returns a copy of migs list.
func (gc *GceCache) GetMigs() []Mig {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()

	migs := make([]Mig, 0, len(gc.migs))
	for _, mig := range gc.migs {
		migs = append(migs, mig)
	}
	return migs
}

// GetMig returns registered mig for given ref
func (gc *GceCache) GetMig(migRef GceRef) (mig Mig, found bool) {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()
	mig, found = gc.migs[migRef]
	return
}

// GetMigRefForInstance returns Mig to which the given instance belongs.
func (gc *GceCache) GetMigRefForInstance(instanceRef GceRef) (migRef GceRef, found bool) {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()
	migRef, found = gc.instanceRefToMigRef[instanceRef]
	return
}

// SetMigInstances sets instance->mig mappings for one mig
func (gc *GceCache) SetMigInstances(migRef GceRef, instanceRefs []GceRef) {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()
	gc.invalidateMigInstancesNoLock(migRef)
	for _, instanceRef := range instanceRefs {
		gc.instanceRefToMigRef[instanceRef] = migRef
	}
}

// InvalidateMigInstances invalidate instance->mig mappings for one mig
func (gc *GceCache) InvalidateMigInstances(migRef GceRef) {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()
	gc.invalidateMigInstancesNoLock(migRef)
}

func (gc *GceCache) invalidateMigInstancesNoLock(migRef GceRef) {
	for instanceRef, instanceMigRef := range gc.instanceRefToMigRef {
		if migRef == instanceMigRef {
			delete(gc.instanceRefToMigRef, instanceRef)
		}
	}
}

// InvalidateMigInstancesAll invalidate all instance->mig mappings
func (gc *GceCache) InvalidateMigInstancesAll() {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()
	gc.instanceRefToMigRef = make(map[GceRef]GceRef)
}

// SetResourceLimiter sets resource limiter.
func (gc *GceCache) SetResourceLimiter(resourceLimiter *cloudprovider.ResourceLimiter) {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()

	gc.resourceLimiter = resourceLimiter
}

// GetResourceLimiter returns resource limiter.
func (gc *GceCache) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()

	return gc.resourceLimiter, nil
}

// GetMigTargetSize returns the cached targetSize for a GceRef
func (gc *GceCache) GetMigTargetSize(ref GceRef) (int64, bool) {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()

	size, found := gc.migTargetSizeCache[ref]
	if found {
		klog.V(5).Infof("target size cache hit for %s", ref)
	}
	return size, found
}

// SetMigTargetSize sets targetSize for a GceRef
func (gc *GceCache) SetMigTargetSize(ref GceRef, size int64) {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()

	gc.migTargetSizeCache[ref] = size
}

// InvalidateMigTargetSize clears the target size cache
func (gc *GceCache) InvalidateMigTargetSize(ref GceRef) {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()

	if _, found := gc.migTargetSizeCache[ref]; found {
		klog.V(5).Infof("target size cache invalidated for %s", ref)
		delete(gc.migTargetSizeCache, ref)
	}
}

// InvalidateAllMigTargetSizes clears the target size cache
func (gc *GceCache) InvalidateAllMigTargetSizes() {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()

	klog.V(5).Infof("target size cache invalidated")
	gc.migTargetSizeCache = map[GceRef]int64{}
}

// GetMachineFromCache retrieves machine type from cache under lock.
func (gc *GceCache) GetMachineFromCache(machineType string, zone string) *gce.MachineType {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()

	return gc.machinesCache[MachineTypeKey{zone, machineType}]
}

// AddMachineToCache adds machine to cache under lock.
func (gc *GceCache) AddMachineToCache(machineType string, zone string, machine *gce.MachineType) {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()

	gc.machinesCache[MachineTypeKey{zone, machineType}] = machine
}

// SetMachinesCache sets the machines cache under lock.
func (gc *GceCache) SetMachinesCache(machinesCache map[MachineTypeKey]*gce.MachineType) {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()

	gc.machinesCache = machinesCache
}

// SetMigBasename sets basename for given mig in cache
func (gc *GceCache) SetMigBasename(migRef GceRef, basename string) {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()
	gc.migBaseNameCache[migRef] = basename
}

// GetMigBasename get basename for given mig from cache.
func (gc *GceCache) GetMigBasename(migRef GceRef) (basename string, found bool) {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()
	basename, found = gc.migBaseNameCache[migRef]
	return
}

// InvalidateMigBasename invalidates basename entry for given mig.
func (gc *GceCache) InvalidateMigBasename(migRef GceRef) {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()
	delete(gc.migBaseNameCache, migRef)
}

// InvalidateAllMigBasenames invalidates all basename entries.
func (gc *GceCache) InvalidateAllMigBasenames() {
	gc.cacheMutex.Lock()
	defer gc.cacheMutex.Unlock()
	gc.migBaseNameCache = make(map[GceRef]string)
}
