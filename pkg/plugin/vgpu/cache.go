/*
Copyright 2023 The Volcano Authors.

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

package vgpu

import (
	"sync"

	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	"volcano.sh/k8s-device-plugin/pkg/plugin/vgpu/config"
)

type DeviceCache struct {
	*GpuDeviceManager

	cache     []*Device
	stopCh    chan interface{}
	unhealthy chan *Device
	notifyCh  map[string]chan *Device
	mutex     sync.Mutex
}

// 通过调用底层NVML驱动，维护设别相关信息
func NewDeviceCache() *DeviceCache {
	skipMigEnabledGPUs := true
	if config.Mode == "mig" {
		skipMigEnabledGPUs = false
	}
	return &DeviceCache{
		GpuDeviceManager: NewGpuDeviceManager(skipMigEnabledGPUs),
		stopCh:           make(chan interface{}),
		unhealthy:        make(chan *Device),
		notifyCh:         make(map[string]chan *Device),
	}
}

func (d *DeviceCache) AddNotifyChannel(name string, ch chan *Device) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.notifyCh[name] = ch
}

func (d *DeviceCache) RemoveNotifyChannel(name string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	delete(d.notifyCh, name)
}

func (d *DeviceCache) Start() {
	// 通过NVML调用底层驱动获取当前节点的设备信息
	d.cache = d.Devices()
	// TODO 这里因该是在做健康检测，如果设备的健康状态发生变化了，此时需要通过ListAndWatch接口通知Kubelet
	go d.CheckHealth(d.stopCh, d.cache, d.unhealthy)
	go d.notify()
}

func (d *DeviceCache) Stop() {
	close(d.stopCh)
}

func (d *DeviceCache) GetCache() []*Device {
	return d.cache
}

func (d *DeviceCache) notify() {
	for {
		select {
		case <-d.stopCh:
			return
		case dev := <-d.unhealthy:
			// 如果一个设备不健康了，那么需要通知device-plugin，让Kubelet实时感知设备的变化
			dev.Health = pluginapi.Unhealthy
			d.mutex.Lock()
			for _, ch := range d.notifyCh {
				ch <- dev
			}
			d.mutex.Unlock()
		}
	}
}
