package runtime

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/alwitt/goutils"
	"github.com/apex/log"
	cerrdefs "github.com/containerd/errdefs"
	dockerVolume "github.com/moby/moby/api/types/volume"
	dockerClient "github.com/moby/moby/client"
)

// dockerVolumeManager a Docker-volume-backed VolumeManager. It provisions and inspects
// Docker volumes on the host reachable through a Docker client it establishes in Setup and
// releases in TearDown.
type dockerVolumeManager struct {
	goutils.Component
	cli *dockerClient.Client

	// tornDown guards TearDown so the client is closed at most once.
	tornDown atomic.Bool
}

/*
NewDockerVolumeManager define a new Docker-volume-backed VolumeManager. The manager does
not touch Docker until Setup is called, and owns the client it then establishes.

	@param ctxt context.Context - execution context (used for log-tag derivation)
	@returns the new volume manager
*/
func NewDockerVolumeManager(_ context.Context) (VolumeManager, error) {
	logTags := log.Fields{
		"module":    "runtime",
		"component": "docker-volume-manager",
	}

	return &dockerVolumeManager{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
				goutils.ModifyLogMetadataByMCPRequestParam,
			},
		},
		cli: nil,
	}, nil
}

// Start establish the Docker client the manager operates through.
func (m *dockerVolumeManager) Start(_ context.Context) error {
	if m.cli != nil {
		return goutils.NewConsistencyError("docker volume manager already set up", nil, true)
	}
	cli, err := dockerClient.New(dockerClient.FromEnv)
	if err != nil {
		return goutils.NewDockerError("failed to build docker client", err, true)
	}
	m.cli = cli
	return nil
}

// Cleanup release the Docker client. Safe to call more than once.
func (m *dockerVolumeManager) Cleanup(_ context.Context) error {
	if !m.tornDown.CompareAndSwap(false, true) {
		return nil
	}
	if m.cli == nil {
		return nil
	}
	// Drop the client reference so a post-teardown operation fails checkReady cleanly rather
	// than hitting a closed client.
	cli := m.cli
	m.cli = nil
	if err := cli.Close(); err != nil {
		return goutils.NewDockerError("failed to close docker client", err, true)
	}
	return nil
}

// checkReady guard that Setup has established the Docker client before a volume operation.
func (m *dockerVolumeManager) checkReady() error {
	if m.cli == nil {
		return goutils.NewConsistencyError(
			"docker volume manager not set up; call Setup first", nil, true,
		)
	}
	return nil
}

// ListVolumes list the Docker volumes, optionally restricted to those whose name starts
// with namePrefix. Docker's name filter matches substrings rather than true prefixes, so
// the prefix restriction is applied client-side.
func (m *dockerVolumeManager) ListVolumes(
	ctxt context.Context, namePrefix *string,
) ([]ContainerVolume, error) {
	if err := m.checkReady(); err != nil {
		return nil, err
	}
	listed, err := m.cli.VolumeList(ctxt, dockerClient.VolumeListOptions{})
	if err != nil {
		return nil, goutils.NewDockerError("failed to list docker volumes", err, true)
	}

	volumes := make([]ContainerVolume, 0, len(listed.Items))
	for _, item := range listed.Items {
		if namePrefix != nil && !strings.HasPrefix(item.Name, *namePrefix) {
			continue
		}
		volumes = append(volumes, dockerVolumeToContainerVolume(item.Name, item.UsageData))
	}
	return volumes, nil
}

// GetVolume find a Docker volume by name, returning it along with the names of the
// containers that currently mount it. Returns a not-found error when the volume does not
// exist.
func (m *dockerVolumeManager) GetVolume(
	ctxt context.Context, name string,
) (ContainerVolume, []string, error) {
	if err := m.checkReady(); err != nil {
		return ContainerVolume{}, nil, err
	}
	inspected, err := m.cli.VolumeInspect(ctxt, name, dockerClient.VolumeInspectOptions{})
	if err != nil {
		if cerrdefs.IsNotFound(err) {
			return ContainerVolume{}, nil, goutils.NewNotFoundError(
				"docker volume '"+name+"' not found", err, true,
			)
		}
		return ContainerVolume{}, nil, goutils.NewDockerError(
			"failed to inspect docker volume '"+name+"'", err, true,
		)
	}

	mounters, err := m.volumeMounters(ctxt, name)
	if err != nil {
		return ContainerVolume{}, nil, err
	}

	volume := dockerVolumeToContainerVolume(inspected.Volume.Name, inspected.Volume.UsageData)
	return volume, mounters, nil
}

// DefineVolume create a new Docker volume. The metadata, when non-nil, must be a
// DockerVolumeMetadata carrying the driver, driver options, and labels; a nil metadata
// provisions a default (local-driver) volume.
func (m *dockerVolumeManager) DefineVolume(
	ctxt context.Context, volume ContainerVolume, metadata any,
) (ContainerVolume, error) {
	if err := m.checkReady(); err != nil {
		return ContainerVolume{}, err
	}
	opts := dockerClient.VolumeCreateOptions{Name: volume.Name}

	if metadata != nil {
		meta, ok := metadata.(DockerVolumeMetadata)
		if !ok {
			return ContainerVolume{}, goutils.NewConsistencyError(
				fmt.Sprintf(
					"docker volume metadata must be DockerVolumeMetadata, got %T", metadata,
				), nil, true,
			)
		}
		opts.Driver = meta.Driver
		opts.DriverOpts = meta.DriverOpts
		opts.Labels = meta.Labels
	}

	created, err := m.cli.VolumeCreate(ctxt, opts)
	if err != nil {
		return ContainerVolume{}, goutils.NewDockerError(
			"failed to create docker volume '"+volume.Name+"'", err, true,
		)
	}

	return dockerVolumeToContainerVolume(created.Volume.Name, created.Volume.UsageData), nil
}

// DeleteVolume delete a Docker volume by name. Removing a volume that does not exist is a
// no-op success (idempotent).
func (m *dockerVolumeManager) DeleteVolume(ctxt context.Context, name string) error {
	if err := m.checkReady(); err != nil {
		return err
	}
	if _, err := m.cli.VolumeRemove(
		ctxt, name, dockerClient.VolumeRemoveOptions{Force: true},
	); err != nil {
		if cerrdefs.IsNotFound(err) {
			return nil
		}
		return goutils.NewDockerError("failed to remove docker volume '"+name+"'", err, true)
	}
	return nil
}

// volumeMounters list the names of the containers that currently mount the named volume.
// Docker's VolumeInspect does not report mounters, so this is a separate ContainerList
// filtered by volume; only containers that presently exist are found.
func (m *dockerVolumeManager) volumeMounters(
	ctxt context.Context, name string,
) ([]string, error) {
	containers, err := m.cli.ContainerList(ctxt, dockerClient.ContainerListOptions{
		All:     true,
		Filters: dockerClient.Filters{}.Add("volume", name),
	})
	if err != nil {
		return nil, goutils.NewDockerError(
			"failed to list containers mounting volume '"+name+"'", err, true,
		)
	}

	mounters := make([]string, 0, len(containers.Items))
	for _, entry := range containers.Items {
		// A container summary carries its names with a leading '/'; prefer the first name and
		// fall back to the ID when unnamed.
		if len(entry.Names) > 0 {
			mounters = append(mounters, strings.TrimPrefix(entry.Names[0], "/"))
			continue
		}
		mounters = append(mounters, entry.ID)
	}
	return mounters, nil
}

// dockerVolumeToContainerVolume map a Docker volume's name and usage data onto the shared
// ContainerVolume. The size is only available for local-driver volumes reporting usage
// data (and only via system df); otherwise it is left 0 ("unknown").
func dockerVolumeToContainerVolume(name string, usage *dockerVolume.UsageData) ContainerVolume {
	out := ContainerVolume{Name: name}
	if usage != nil && usage.Size > 0 {
		out.Size = usage.Size
	}
	return out
}
