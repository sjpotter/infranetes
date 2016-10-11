package virtualbox

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/sjpotter/infranetes/pkg/infranetes/provider"

	kubeapi "k8s.io/kubernetes/pkg/kubelet/api/v1alpha1/runtime"

	lvm "github.com/apcera/libretto/virtualmachine"
	"github.com/apcera/libretto/virtualmachine/virtualbox"
	"github.com/golang/glog"
	"github.com/sjpotter/infranetes/pkg/infranetes/provider/common"
	"sync"
)

type podData struct {
	common.PodData
	vm virtualbox.VM
}

type vboxProvider struct {
	netDevice string
	vmSrc     string
	vmMap     map[string]*podData
	vmMapLock sync.Mutex
}

func init() {
	provider.PodProviders.RegisterProvider("virtualbox", NewVBoxProvider)
}

type vboxConfig struct {
	NetDevice string
	VMSrc     string
}

func NewVBoxProvider() (provider.PodProvider, error) {
	var conf vboxConfig

	file, err := ioutil.ReadFile("virtualbox.json")
	if err != nil {
		return nil, fmt.Errorf("File error: %v\n", err)
	}

	json.Unmarshal(file, &conf)

	return &vboxProvider{
		netDevice: conf.NetDevice,
		vmSrc:     conf.VMSrc,
		vmMap:     make(map[string]*podData),
	}, nil
}

func (v *vboxProvider) getPodData(id string) (*podData, error) {
	v.vmMapLock.Lock()
	defer v.vmMapLock.Unlock()

	podData, ok := v.vmMap[id]
	if !ok {
		return nil, fmt.Errorf("Invalid PodSandboxId (%v)", id)
	}
	return podData, nil
}

func (v *vboxProvider) RunPodSandbox(req *kubeapi.RunPodSandboxRequest) (*kubeapi.RunPodSandboxResponse, error) {
	config := virtualbox.Config{
		NICs: []virtualbox.NIC{
			{Idx: 1, Backing: virtualbox.Bridged, BackingDevice: v.netDevice},
		},
	}

	vm := virtualbox.VM{Src: v.vmSrc,
		Config: config,
	}

	if err := vm.Provision(); err != nil {
		return nil, fmt.Errorf("Failed to Provision: %v", err)
	}

	ips, err := vm.GetIPs()
	if err != nil {
		return nil, fmt.Errorf("CreatePodSandbox: error in GetIPs(): %v", err)
	}

	ip := ips[0].String()

	client, err := common.CreateClient(ip)
	if err != nil {
		return nil, fmt.Errorf("CreatePodSandbox: error in createClient(): %v", err)
	}

	name := vm.GetName()

	v.vmMapLock.Lock()
	defer v.vmMapLock.Unlock()

	v.vmMap[name] = &podData{
		PodData: common.PodData{
			Id:          &name,
			Metadata:    req.Config.Metadata,
			Annotations: req.Config.Annotations,
			CreatedAt:   time.Now().Unix(),
			Ip:          ip,
			Labels:      req.Config.Labels,
			Linux:       req.Config.Linux,
			Client:      client,
			PodState:    kubeapi.PodSandBoxState_READY,
		},

		vm: vm,
	}

	resp := &kubeapi.RunPodSandboxResponse{
		PodSandboxId: &name,
	}

	return resp, nil
}

func (v *vboxProvider) StopPodSandbox(req *kubeapi.StopPodSandboxRequest) (*kubeapi.StopPodSandboxResponse, error) {
	podData, err := v.getPodData(req.GetPodSandboxId())
	if err != nil {
		return nil, fmt.Errorf("StopPodSandbox: %v", err)
	}

	err = podData.StopPod()

	resp := &kubeapi.StopPodSandboxResponse{}
	return resp, nil
}

func (v *vboxProvider) RemovePodSandbox(req *kubeapi.RemovePodSandboxRequest) (*kubeapi.RemovePodSandboxResponse, error) {
	podData, err := v.getPodData(req.GetPodSandboxId())
	if err != nil {
		return nil, fmt.Errorf("RemovePodSandbox: %v", err)
	}

	if err := podData.vm.Destroy(); err != nil {
		return nil, fmt.Errorf("RemovePodSandbox: %v", err)
	}

	err = podData.RemovePod()

	v.vmMapLock.Lock()
	delete(v.vmMap, req.GetPodSandboxId())
	v.vmMapLock.Unlock()

	resp := &kubeapi.RemovePodSandboxResponse{}
	return resp, nil
}

func (v *vboxProvider) PodSandboxStatus(req *kubeapi.PodSandboxStatusRequest) (*kubeapi.PodSandboxStatusResponse, error) {
	podData, err := v.getPodData(req.GetPodSandboxId())
	if err != nil {
		return nil, fmt.Errorf("PodSandboxStatus: %v", err)
	}

	podData.StateLock.Lock()
	defer podData.StateLock.Unlock()

	status, err := podData.PodStatus()
	if err != nil {
		return nil, fmt.Errorf("PodSandboxStatus: %v", err)
	}

	vmState, err := podData.vm.GetState()
	if err != nil {
		return nil, fmt.Errorf("PodSandboxStatus: error in GetState(): %v", err)
	}

	state := podData.PodState
	if vmState != lvm.VMRunning {
		state = kubeapi.PodSandBoxState_NOTREADY
	}

	status.State = &state

	resp := &kubeapi.PodSandboxStatusResponse{
		Status: status,
	}

	return resp, nil
}

func (v *vboxProvider) ListPodSandbox(req *kubeapi.ListPodSandboxRequest) (*kubeapi.ListPodSandboxResponse, error) {
	sandboxes := []*kubeapi.PodSandbox{}

	v.vmMapLock.Lock()
	defer v.vmMapLock.Unlock()

	glog.V(1).Infof("ListPodSandbox: len of vmMap = %v", len(v.vmMap))

	for id, podData := range v.vmMap {
		glog.V(1).Infof("ListPodSandbox:v podData for %v = %+v", id, podData)
		vmState, err := podData.vm.GetState()
		if err != nil {
			continue
		}

		podState := podData.PodState
		if vmState != lvm.VMRunning {
			podState = kubeapi.PodSandBoxState_NOTREADY
		}

		if req.Filter != nil {
			if req.Filter.GetId() != "" && req.Filter.GetId() != id {
				glog.V(1).Infof("ListPodSandbox: filtering out %v because doesn't match %v", id, req.Filter.GetId())
				continue
			}

			if req.Filter.GetState() != podState {
				glog.V(1).Infof("ListPodSandbox: filtering out %v because want %v and got %v", id, req.Filter.GetState(), podState)
				continue
			}

			filterLabels := req.Filter.GetLabelSelector()

			if filter, msg := podData.FilterByLabels(filterLabels); filter {
				glog.V(1).Infof("ListPodSandbox: filtering out %v on labels as %v", id, msg)
				continue
			}
		}

		sandbox := podData.GetSandbox()
		sandbox.State = &podState

		glog.V(1).Infof("ListPodSandbox Appending a sandbox for %v to sandboxes", id)

		sandboxes = append(sandboxes, sandbox)
	}

	glog.V(1).Infof("ListPodSandbox: len of sandboxes returning = %v", len(sandboxes))

	resp := &kubeapi.ListPodSandboxResponse{
		Items: sandboxes,
	}

	return resp, nil
}

func (v *vboxProvider) GetIP(podName string) (string, error) {
	podData, err := v.getPodData(podName)

	if err != nil {
		return "", fmt.Errorf("%v unknown pod name", podName)
	}

	return podData.Ip, nil
}

func (v *vboxProvider) GetClient(podName string) (*common.Client, error) {
	podData, err := v.getPodData(podName)

	if err != nil {
		return nil, fmt.Errorf("%v unknown pod name", podName)
	}

	return podData.Client, nil
}

func (v *vboxProvider) GetVMList() []string {
	v.vmMapLock.Lock()
	defer v.vmMapLock.Unlock()

	ret := []string{}
	for name := range v.vmMap {
		ret = append(ret, name)
	}

	return ret
}
