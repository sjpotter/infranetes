package aws

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sjpotter/infranetes/pkg/infranetes/provider"
	"github.com/sjpotter/infranetes/pkg/infranetes/provider/common"

	"github.com/apcera/libretto/ssh"
	lvm "github.com/apcera/libretto/virtualmachine"
	"github.com/apcera/libretto/virtualmachine/aws"
	"github.com/golang/glog"

	kubeapi "k8s.io/kubernetes/pkg/kubelet/api/v1alpha1/runtime"
)

type podData struct {
	common.PodData
	vm                 *aws.VM
	vmState            string
	vmStateLastChecked time.Time
}

type awsProvider struct {
	vmMap     map[string]*podData
	vmMapLock sync.RWMutex
	config    *awsConfig
}

func init() {
	provider.PodProviders.RegisterProvider("aws", NewAWSProvider)
}

type awsConfig struct {
	Ami           string
	Region        string
	SecurityGroup string
	Vpc           string
	Subnet        string
	SshKey        string
}

func NewAWSProvider() (provider.PodProvider, error) {
	var conf awsConfig

	file, err := ioutil.ReadFile("aws.json")
	if err != nil {
		return nil, fmt.Errorf("File error: %v\n", err)
	}

	json.Unmarshal(file, &conf)

	glog.Infof("Validating AWS Credentials")

	if err := aws.ValidCredentials("use-west-2"); err != nil {
		glog.Infof("Failed to Validated AWS Credentials")
		return nil, fmt.Errorf("failed to validate credentials: %v\n", err)
	}

	glog.Infof("Validated AWS Credentials")

	return &awsProvider{
		vmMap:  make(map[string]*podData),
		config: &conf,
	}, nil
}

/* Must be at least holding the vmmap RLock */
func (v *awsProvider) getPodData(id string) (*podData, error) {
	podData, ok := v.vmMap[id]
	if !ok {
		return nil, fmt.Errorf("Invalid PodSandboxId (%v)", id)
	}
	return podData, nil
}

func (v *awsProvider) RunPodSandbox(req *kubeapi.RunPodSandboxRequest) (*kubeapi.RunPodSandboxResponse, error) {
	rawKey, err := ioutil.ReadFile(v.config.SshKey)
	if err != nil {
		return nil, fmt.Errorf("failed to read key: %v\n", err)
	}

	awsName := req.Config.Metadata.GetNamespace() + ":" + req.Config.Metadata.GetName()
	vm := &aws.VM{
		Name:         awsName,
		AMI:          v.config.Ami,
		InstanceType: "t2.micro",
		//		InstanceType: "m4.large",
		SSHCreds: ssh.Credentials{
			SSHUser:       "ubuntu",
			SSHPrivateKey: string(rawKey),
		},
		Volumes: []aws.EBSVolume{
			{
				DeviceName: "/dev/sda1",
			},
		},
		Region:        v.config.Region,
		KeyPair:       strings.TrimSuffix(filepath.Base(v.config.SshKey), filepath.Ext(v.config.SshKey)),
		SecurityGroup: v.config.SecurityGroup,
		VPC:           v.config.Vpc,
		Subnet:        v.config.Subnet,
	}

	if err := vm.Provision(); err != nil {
		return nil, fmt.Errorf("failed to provision vm: %v\n", err)
	}

	ips, err := vm.GetIPs()
	if err != nil {
		return nil, fmt.Errorf("CreatePodSandbox: error in GetIPs(): %v", err)
	}

	ip := ips[0].String()

	name := vm.InstanceID

	client, err := common.CreateClient(ip)
	if err != nil {
		return nil, fmt.Errorf("CreatePodSandbox: error in createClient(): %v", err)
	}
	if client == nil {
		glog.Infof("WARNING WARNING WARNING: returned a nil GRPC client")
		glog.Infof("WARNING WARNING WARNING: returned a nil GRPC client")
		glog.Infof("WARNING WARNING WARNING: returned a nil GRPC client")
		vm.Destroy()
		return nil, fmt.Errorf("returned a nil GRPC client")
	}

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
		vm:                 vm,
		vmState:            lvm.VMRunning,
		vmStateLastChecked: time.Now(),
	}

	resp := &kubeapi.RunPodSandboxResponse{
		PodSandboxId: &name,
	}

	return resp, nil
}

func (v *awsProvider) StopPodSandbox(req *kubeapi.StopPodSandboxRequest) (*kubeapi.StopPodSandboxResponse, error) {
	v.vmMapLock.RLock()
	defer v.vmMapLock.RUnlock()

	podData, err := v.getPodData(req.GetPodSandboxId())
	if err != nil {
		return nil, fmt.Errorf("StopPodSandbox: %v", err)
	}

	podData.StateLock.Lock()
	defer podData.StateLock.Unlock()

	err = podData.StopPod()

	resp := &kubeapi.StopPodSandboxResponse{}
	return resp, err
}

func (v *awsProvider) RemovePodSandbox(req *kubeapi.RemovePodSandboxRequest) (*kubeapi.RemovePodSandboxResponse, error) {
	v.vmMapLock.Lock()
	defer v.vmMapLock.Unlock()

	podData, err := v.getPodData(req.GetPodSandboxId())
	if err != nil {
		return nil, fmt.Errorf("RemovePodSandbox: %v", err)
	}

	podData.StateLock.Lock()
	defer podData.StateLock.Unlock()

	if err := podData.vm.Destroy(); err != nil {
		return nil, fmt.Errorf("RemovePodSandbox: %v", err)
	}

	err = podData.RemovePod()

	delete(v.vmMap, req.GetPodSandboxId())

	resp := &kubeapi.RemovePodSandboxResponse{}
	return resp, err
}

func updateVMState(podData *podData) string {
	ret := podData.vmState

	if time.Now().After(podData.vmStateLastChecked.Add(30 * time.Second)) {
		vmState, err := podData.vm.GetState()
		if err == nil {
			podData.vmState = vmState
			podData.vmStateLastChecked = time.Now()
			ret = podData.vmState
		}
	}

	return ret
}

func (v *awsProvider) PodSandboxStatus(req *kubeapi.PodSandboxStatusRequest) (*kubeapi.PodSandboxStatusResponse, error) {
	v.vmMapLock.RLock()
	defer v.vmMapLock.RUnlock()

	podData, err := v.getPodData(req.GetPodSandboxId())
	if err != nil {
		return nil, fmt.Errorf("PodSandboxStatus: %v", err)
	}

	podData.StateLock.Lock()
	defer podData.StateLock.Unlock()

	vmState := updateVMState(podData)
	if vmState != lvm.VMRunning {
		podData.PodState = kubeapi.PodSandBoxState_NOTREADY
	}

	status := podData.PodStatus()

	resp := &kubeapi.PodSandboxStatusResponse{
		Status: status,
	}

	return resp, nil
}

func (v *awsProvider) ListPodSandbox(req *kubeapi.ListPodSandboxRequest) (*kubeapi.ListPodSandboxResponse, error) {
	v.vmMapLock.RLock()
	defer v.vmMapLock.RUnlock()

	sandboxes := []*kubeapi.PodSandbox{}

	glog.V(1).Infof("ListPodSandbox: len of vmMap = %v", len(v.vmMap))

	for id, podData := range v.vmMap {
		// podData lock is taken and released in filter
		if sandbox, ok := filter(podData, req.Filter); ok {
			glog.V(1).Infof("ListPodSandbox Appending a sandbox for %v to sandboxes", id)
			sandboxes = append(sandboxes, sandbox)
		}
	}

	glog.V(1).Infof("ListPodSandbox: len of sandboxes returning = %v", len(sandboxes))

	resp := &kubeapi.ListPodSandboxResponse{
		Items: sandboxes,
	}

	return resp, nil
}

func filter(podData *podData, reqFilter *kubeapi.PodSandboxFilter) (*kubeapi.PodSandbox, bool) {
	podData.StateLock.Lock()
	defer podData.StateLock.Unlock()

	glog.V(1).Infof("filter: podData for %v = %+v", *podData.Id, podData)

	vmState := updateVMState(podData)
	if vmState != lvm.VMRunning {
		podData.PodState = kubeapi.PodSandBoxState_NOTREADY
	}

	if filter, msg := podData.Filter(reqFilter); filter {
		glog.V(1).Infof("filter: filtering out %v on labels as %v", *podData.Id, msg)
		return nil, false
	}

	sandbox := podData.GetSandbox()

	return sandbox, true
}

func (v *awsProvider) GetClient(podName string) (*common.Client, error) {
	v.vmMapLock.RLock()
	defer v.vmMapLock.RUnlock()

	return v.GetClientLocked(podName)
}

func (v *awsProvider) GetClientLocked(podName string) (*common.Client, error) {
	podData, err := v.getPodData(podName)

	if err != nil {
		return nil, fmt.Errorf("%v unknown pod name", podName)
	}

	return podData.Client, nil
}

func (v *awsProvider) GetVMList() []string {
	ret := []string{}

	for name := range v.vmMap {
		ret = append(ret, name)
	}

	return ret
}

func (v *awsProvider) RLockMap() {
	v.vmMapLock.RLock()
}

func (v *awsProvider) RUnlockMap() {
	v.vmMapLock.RUnlock()
}
