package gcp

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"

	"github.com/golang/glog"

	"github.com/sjpotter/infranetes/pkg/infranetes/provider"
	compute "google.golang.org/api/compute/v1"

	kubeapi "k8s.io/kubernetes/pkg/kubelet/apis/cri/v1alpha1"
)

type gcpImageProvider struct {
	lock sync.RWMutex

	config   *GceConfig
	imageMap map[string]*kubeapi.Image
}

func init() {
	provider.ImageProviders.RegisterProvider("gcp", NewGCPImageProvider)
}

func NewGCPImageProvider() (provider.ImageProvider, error) {
	var conf GceConfig

	file, err := ioutil.ReadFile("gce.json")
	if err != nil {
		return nil, fmt.Errorf("File error: %v\n", err)
	}

	json.Unmarshal(file, &conf)

	if conf.SourceImage == "" || conf.Zone == "" || conf.Project == "" || conf.Scope == "" || conf.AuthFile == "" || conf.Network == "" || conf.Subnet == "" {
		msg := fmt.Sprintf("Failed to read in complete config file: conf = %+v", conf)
		glog.Info(msg)
		return nil, fmt.Errorf(msg)
	}

	provider := &gcpImageProvider{
		config:   &conf,
		imageMap: make(map[string]*kubeapi.Image),
	}

	return provider, nil
}

func (p *gcpImageProvider) ListImages(req *kubeapi.ListImagesRequest) (*kubeapi.ListImagesResponse, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	result := []*kubeapi.Image{}

	if req.Filter != nil && req.Filter.Image != nil {
		if image, ok := p.imageMap[req.Filter.Image.Image]; ok {
			result = append(result, image)
		}
	} else {
		for _, image := range p.imageMap {
			result = append(result, image)
		}
	}

	resp := &kubeapi.ListImagesResponse{
		Images: result,
	}

	return resp, nil
}

func (p *gcpImageProvider) ImageStatus(req *kubeapi.ImageStatusRequest) (*kubeapi.ImageStatusResponse, error) {
	name := req.Image.Image

	if len(strings.Split(name, ":")) == 1 {
		name += ":latest"
		req.Image.Image = name
	}

	newreq := &kubeapi.ListImagesRequest{
		Filter: &kubeapi.ImageFilter{
			Image: req.Image,
		},
	}

	listresp, err := p.ListImages(newreq)
	if err != nil {
		return nil, err
	}

	switch len(listresp.Images) {
	case 0:
		return &kubeapi.ImageStatusResponse{}, nil
	case 1:
		return &kubeapi.ImageStatusResponse{Image: listresp.Images[0]}, nil
	default:
		return nil, fmt.Errorf("ImageStatus returned more than one image: %+v", listresp.Images)
	}
}

func toRuntimeAPIImage(image *compute.Image) (*kubeapi.Image, error) {
	if image == nil {
		return nil, errors.New("unable to convert a nil pointer to a runtime API image")
	}

	size := uint64(image.ArchiveSizeBytes)

	name := image.Name

	return &kubeapi.Image{
		Id:          name,
		RepoTags:    []string{name},
		RepoDigests: []string{name},
		Size_:       size,
	}, nil
}

func (p *gcpImageProvider) PullImage(req *kubeapi.PullImageRequest) (*kubeapi.PullImageResponse, error) {
	var call *compute.ImagesListCall

	s, err := GetService(p.config.AuthFile, p.config.Project, p.config.Zone, []string{p.config.Scope})

	splits := strings.Split(req.Image.Image, "/")
	switch len(splits) {
	case 1:
		call = s.service.Images.List(s.project).Filter("name eq " + splits[0])
		break
	case 2:
		call = s.service.Images.List(splits[0]).Filter("name eq " + splits[1])
		break
	default:
		return nil, fmt.Errorf("PullImage: can't parse %v", req.Image.Image)
	}

	results, err := call.Do()
	if err != nil {
		return nil, fmt.Errorf("PullImage: ec2 DescribeImages failed: %v", err)
	}

	switch len(results.Items) {
	case 0:
		return nil, fmt.Errorf("PullImage: couldn't find any image matching %v", req.Image.Image)
	case 1:
		p.lock.Lock()
		defer p.lock.Unlock()
		image, err := toRuntimeAPIImage(results.Items[0])
		if err != nil {
			return nil, fmt.Errorf("PullImage: toRuntimeAPIImage failed: %v", err)
		}
		p.imageMap[req.Image.Image] = image

		return &kubeapi.PullImageResponse{}, nil
	default:
		return nil, fmt.Errorf("PullImage: ec2.DescribeImages returned more than one image: %+v", results.Items)
	}
}

func (p *gcpImageProvider) RemoveImage(req *kubeapi.RemoveImageRequest) (*kubeapi.RemoveImageResponse, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	delete(p.imageMap, req.Image.Image)

	return &kubeapi.RemoveImageResponse{}, nil
}

func (p *gcpImageProvider) Integrate(pp provider.PodProvider) bool {
	switch pp.(type) {
	case *gcpPodProvider:
		app := pp.(*gcpPodProvider)
		//aws shouldn't boot on pod run if using container images
		app.imagePod = true

		return true
	}

	return false
}

func (p *gcpImageProvider) Translate(spec *kubeapi.ImageSpec) (string, error) {
	return spec.Image, nil
}
