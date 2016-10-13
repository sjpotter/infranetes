package common

import (
	"sync"

	"fmt"
	kubeapi "k8s.io/kubernetes/pkg/kubelet/api/v1alpha1/runtime"
)

type PodData struct {
	Id          *string
	Metadata    *kubeapi.PodSandboxMetadata
	Annotations map[string]string
	CreatedAt   int64
	Ip          string
	Labels      map[string]string
	Linux       *kubeapi.LinuxPodSandboxConfig
	StateLock   sync.Mutex
	Client      *Client
	PodState    kubeapi.PodSandBoxState
}

func (p *PodData) StopPod() error {
	p.StateLock.Lock()
	p.PodState = kubeapi.PodSandBoxState_NOTREADY
	p.StateLock.Unlock()

	return nil
}

func (p *PodData) RemovePod() error {
	p.Client.Close()

	return nil
}

func (p *PodData) PodStatus() *kubeapi.PodSandboxStatus {
	network := &kubeapi.PodSandboxNetworkStatus{
		Ip: &p.Ip,
	}

	net := "host"
	linux := &kubeapi.LinuxPodSandboxStatus{
		Namespaces: &kubeapi.Namespace{
			Network: &net,
			Options: p.Linux.NamespaceOptions,
		},
	}

	status := &kubeapi.PodSandboxStatus{
		Id:          p.Id,
		CreatedAt:   &p.CreatedAt,
		Metadata:    p.Metadata,
		Network:     network,
		Linux:       linux,
		Labels:      p.Labels,
		Annotations: p.Annotations,
		State:       &p.PodState,
	}

	return status
}

func (p *PodData) Filter(filter *kubeapi.PodSandboxFilter) (bool, string) {
	if filter != nil {
		if filter.GetId() != "" && filter.GetId() != *p.Id {
			return true, fmt.Sprintf("doesn't match %v", filter.GetId())
		}

		if filter.GetState() != p.PodState {
			return true, fmt.Sprintf("want %v and got %v", filter.GetState(), p.PodState)
		}

		for key, filterVal := range filter.GetLabelSelector() {
			if podVal, ok := p.Labels[key]; !ok {
				return true, fmt.Sprintf("didn't find key %v in local labels: %+v", key, p.Labels)
			} else {
				if podVal != filterVal {
					return true, fmt.Sprintf("key value's didn't match %v and %v", filterVal, podVal)
				}
			}
		}
	}

	return false, ""
}

func (p *PodData) GetSandbox() *kubeapi.PodSandbox {
	return &kubeapi.PodSandbox{
		CreatedAt:   &p.CreatedAt,
		Id:          p.Id,
		Metadata:    p.Metadata,
		Labels:      p.Labels,
		Annotations: p.Annotations,
		State:       &p.PodState,
	}
}
