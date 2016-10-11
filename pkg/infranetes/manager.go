package infranetes

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"

	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	kubeapi "k8s.io/kubernetes/pkg/kubelet/api/v1alpha1/runtime"

	"github.com/sjpotter/infranetes/pkg/infranetes/provider"
	"github.com/sjpotter/infranetes/pkg/infranetes/provider/common"
)

var (
	runtimeAPIVersion = "0.1.0"
)

type Manager struct {
	server       *grpc.Server
	podProvider  provider.PodProvider
	contProvider provider.ImageProvider

	vmMap     map[string]*common.PodData
	vmMapLock sync.RWMutex
}

func NewInfranetesManager(podProvider provider.PodProvider, contProvider provider.ImageProvider) (*Manager, error) {
	manager := &Manager{
		server:       grpc.NewServer(),
		podProvider:  podProvider,
		contProvider: contProvider,
		vmMap:        make(map[string]*common.PodData),
	}

	manager.registerServer()

	return manager, nil
}

func (s *Manager) Serve(addr string) error {
	glog.V(1).Infof("Start infranetes at %s", addr)

	if err := syscall.Unlink(addr); err != nil && !os.IsNotExist(err) {
		return err
	}

	lis, err := net.Listen("unix", addr)

	if err != nil {
		glog.Fatalf("Failed to listen %s: %v", addr, err)
		return err
	}

	defer lis.Close()
	return s.server.Serve(lis)
}

func (s *Manager) registerServer() {
	kubeapi.RegisterRuntimeServiceServer(s.server, s)
	kubeapi.RegisterImageServiceServer(s.server, s)
}

func (s *Manager) Version(ctx context.Context, req *kubeapi.VersionRequest) (*kubeapi.VersionResponse, error) {
	runtimeName := "infranetes"

	resp := &kubeapi.VersionResponse{
		RuntimeApiVersion: &runtimeAPIVersion,
		RuntimeName:       &runtimeName,
		RuntimeVersion:    &runtimeAPIVersion,
		Version:           &runtimeAPIVersion,
	}

	return resp, nil
}

func (m *Manager) RunPodSandbox(ctx context.Context, req *kubeapi.RunPodSandboxRequest) (*kubeapi.RunPodSandboxResponse, error) {
	cookie := rand.Int()
	glog.Infof("%d: RunPodSandbox: req = %+v", cookie, req)

	resp, err := m.createSandbox(req)

	glog.Infof("%d: RunPodSandbox: resp = %+v, err = %v", cookie, resp, err)

	return resp, err
}

func (m *Manager) StopPodSandbox(ctx context.Context, req *kubeapi.StopPodSandboxRequest) (*kubeapi.StopPodSandboxResponse, error) {
	cookie := rand.Int()
	glog.Infof("%d: StopPodSandbox: req = %+v", cookie, req)

	resp, err := m.stopSandbox(req)

	glog.Infof("%d: StopPodSandbox: resp = %+v, err = %v", cookie, resp, err)

	return resp, err
}

func (m *Manager) RemovePodSandbox(ctx context.Context, req *kubeapi.RemovePodSandboxRequest) (*kubeapi.RemovePodSandboxResponse, error) {
	cookie := rand.Int()
	glog.Infof("%d: RemovePodSandbox: req = %+v", cookie, req)

	err := m.removePodSandbox(req)

	resp := &kubeapi.RemovePodSandboxResponse{}

	glog.Infof("%d: RemovePodSandbox: resp = %+v, err = %v", cookie, resp, err)

	return resp, err
}

func (m *Manager) PodSandboxStatus(ctx context.Context, req *kubeapi.PodSandboxStatusRequest) (*kubeapi.PodSandboxStatusResponse, error) {
	cookie := rand.Int()
	glog.Infof("%d: PodSandboxStatus: req = %+v", cookie, req)

	resp, err := m.podSandboxStatus(req)

	glog.Infof("%d: PodSandboxStatus: resp = %+v, err = %v", cookie, resp, err)

	return resp, err
}

func (m *Manager) ListPodSandbox(ctx context.Context, req *kubeapi.ListPodSandboxRequest) (*kubeapi.ListPodSandboxResponse, error) {
	cookie := rand.Int()
	glog.V(1).Infof("%d: ListPodSandbox: req = %+v", cookie, req)

	resp, err := m.listPodSandbox(req)

	glog.V(1).Infof("%d: ListPodSandbox: resp = %+v, err = %v", cookie, resp, nil)

	return resp, err
}

func (m *Manager) CreateContainer(ctx context.Context, req *kubeapi.CreateContainerRequest) (*kubeapi.CreateContainerResponse, error) {
	cookie := rand.Int()
	glog.Infof("%d: CreateContainer: req = %+v", cookie, req)

	podId := req.GetPodSandboxId()

	client, err := m.getClient(podId)
	if err != nil {
		glog.Infof("%d: CreateContainer: failed to get client for sandbox %v", podId)
		return nil, fmt.Errorf("Failed to get client for sandbox %v: %v", podId, err)
	}

	resp, err := client.CreateContainer(req)

	glog.Infof("%d: CreateContainer: resp = %+v, err = %v", cookie, resp, err)

	return resp, err
}

func (m *Manager) StartContainer(ctx context.Context, req *kubeapi.StartContainerRequest) (*kubeapi.StartContainerResponse, error) {
	cookie := rand.Int()
	glog.Infof("%d: StartContainer: req = %+v", cookie, req)

	splits := strings.Split(req.GetContainerId(), ":")
	podId := splits[0]

	client, err := m.getClient(podId)
	if err != nil {
		glog.Infof("%d: StartContainer: failed to get client for sandbox %v", cookie, podId)
		return nil, fmt.Errorf("Failed to get client for sandbox %v: %v", podId, err)
	}

	resp, err := client.StartContainer(req)

	glog.Infof("%d: StartContainer: resp = %+v, err = %v", cookie, resp, err)

	return resp, err
}

func (m *Manager) StopContainer(ctx context.Context, req *kubeapi.StopContainerRequest) (*kubeapi.StopContainerResponse, error) {
	cookie := rand.Int()
	glog.Infof("%d: StopContainer: req = %+v", cookie, req)

	splits := strings.Split(req.GetContainerId(), ":")
	podId := splits[0]

	client, err := m.getClient(podId)
	if err != nil {
		glog.Infof("%d: StopContainer: failed to get client for sandbox %v", cookie, podId)
		return nil, fmt.Errorf("Failed to get client for sandbox %v: %v", podId, err)
	}

	resp, err := client.StopContainer(req)

	glog.Infof("%d: StopContainer: resp = %+v, err = %v", cookie, resp, err)

	return resp, err
}

func (m *Manager) RemoveContainer(ctx context.Context, req *kubeapi.RemoveContainerRequest) (*kubeapi.RemoveContainerResponse, error) {
	cookie := rand.Int()
	glog.Infof("%d: RemoveContainer: req = %+v", cookie, req)

	splits := strings.Split(req.GetContainerId(), ":")
	podId := splits[0]

	client, err := m.getClient(podId)
	if err != nil {
		glog.Infof("%d: RemoveContainer: failed to get client for sandbox %v", cookie, podId)
		return nil, fmt.Errorf("Failed to get client for sandbox %v: %v", podId, err)
	}

	resp, err := client.RemoveContainer(req)

	glog.Infof("%d: RemoveContainer: resp = %+v, err = %v", cookie, resp, err)

	return resp, err
}

func (m *Manager) ListContainers(ctx context.Context, req *kubeapi.ListContainersRequest) (*kubeapi.ListContainersResponse, error) {
	cookie := rand.Int()
	glog.V(1).Infof("%d: ListContainers: req = %+v", cookie, req)

	resp, err := m.listContainers(req)

	glog.V(1).Infof("%d: ListContainers: resp = %+v, err = %v", cookie, resp, err)

	return resp, err
}

func (m *Manager) ContainerStatus(ctx context.Context, req *kubeapi.ContainerStatusRequest) (*kubeapi.ContainerStatusResponse, error) {
	cookie := rand.Int()
	glog.Infof("%d: ContainerStatus: req = %+v", cookie, req)

	splits := strings.Split(req.GetContainerId(), ":")
	podId := splits[0]

	client, err := m.getClient(podId)
	if err != nil {
		glog.Infof("%d: ContainerStatus: failed to get client for sandbox %v", cookie, podId)
		return nil, fmt.Errorf("failed to get client for sandbox %v", podId)
	}

	resp, err := client.ContainerStatus(req)

	glog.Infof("%d: ContainerStatus: resp = %+v, err = %v", cookie, resp, err)

	return resp, err
}

func (m *Manager) Exec(stream kubeapi.RuntimeService_ExecServer) error {
	glog.Infof("Exec: Enter")

	err := errors.New("Unimplemented")

	glog.Infof("Exec: err = %v", err)

	return err
}

func (m *Manager) ListImages(ctx context.Context, req *kubeapi.ListImagesRequest) (*kubeapi.ListImagesResponse, error) {
	//	glog.Infof("ListImages: req = %+v", req)

	resp, err := m.contProvider.ListImages(req)

	//	glog.Infof("ListImages: resp = %+v, err = %v", resp, err)

	return resp, err
}

func (m *Manager) ImageStatus(ctx context.Context, req *kubeapi.ImageStatusRequest) (*kubeapi.ImageStatusResponse, error) {
	glog.Infof("ImageStatus: req = %+v", req)

	resp, err := m.contProvider.ImageStatus(req)

	glog.Infof("ImageStatus: resp = %+v, err = %v", resp, err)

	return resp, err
}

func (m *Manager) PullImage(ctx context.Context, req *kubeapi.PullImageRequest) (*kubeapi.PullImageResponse, error) {
	glog.Infof("PullImage: req = %+v", req)

	resp, err := m.contProvider.PullImage(req)

	glog.Infof("PullImage: resp = %+v, err = %v", resp, err)

	return resp, err
}

func (m *Manager) RemoveImage(ctx context.Context, req *kubeapi.RemoveImageRequest) (*kubeapi.RemoveImageResponse, error) {
	glog.Infof("RemoveImage: req = %+v", req)

	resp, err := m.contProvider.RemoveImage(req)

	glog.Infof("RemoveImage: resp = %+v, err = %v", resp, err)

	return resp, err
}
