package runtime

// Default values applied for omitted docker driver parameters.
const (
	// DefaultDockerNetworkMode default container network mode (no networking)
	DefaultDockerNetworkMode = "none"
	// DefaultDockerPortProtocol default published port protocol
	DefaultDockerPortProtocol = "tcp"
	// DefaultDockerPublishHostIP default host interface a published port binds to
	DefaultDockerPublishHostIP = "127.0.0.1"
)

// DockerPortPublish publishes a container port to a host interface so the session
// command can accept inbound connections
type DockerPortPublish struct {
	// ContainerPort the port the session command listens on inside the container
	ContainerPort uint16 `json:"container_port" validate:"required"`
	// Protocol the port protocol; defaults to DefaultContainerPortProtocol when empty
	Protocol string `json:"protocol,omitempty" validate:"omitempty,oneof=tcp udp"`
	// HostPort the host port to bind; 0 requests an ephemeral host port
	HostPort uint16 `json:"host_port,omitempty"`
	// HostIP the host interface to bind to; defaults to DefaultContainerPublishHostIP when empty
	HostIP string `json:"host_ip,omitempty" validate:"omitempty,ip"`
}

// ResolvedProtocol resolve Protocol, defaulting when empty
func (p DockerPortPublish) ResolvedProtocol() string {
	if p.Protocol == "" {
		return DefaultDockerPortProtocol
	}
	return p.Protocol
}

// ResolvedHostIP resolve HostIP, defaulting when empty
func (p DockerPortPublish) ResolvedHostIP() string {
	if p.HostIP == "" {
		return DefaultDockerPublishHostIP
	}
	return p.HostIP
}

// DockerRuntimeParams parameters for a Docker container runtime. It embeds the
// cross-runtime ContainerRuntimeParams and adds only the fields with no Kubernetes-Job
// equivalent (Docker daemon networking, host port publishing, container lifecycle) plus
// the string-valued run-as user/group (Kubernetes requires numeric UIDs/GIDs).
type DockerRuntimeParams struct {
	ContainerRuntimeParams

	// RunAsUser user to run the container process as; defaults to DefaultContainerRunAsUser
	RunAsUser string `json:"run_as_user,omitempty"`
	// RunAsGroup group to run the container process as; defaults to DefaultContainerRunAsGroup
	RunAsGroup string `json:"run_as_group,omitempty"`

	// NetworkMode the container network mode (e.g. "none", "bridge"); defaults to
	// DefaultDockerNetworkMode. Must be routable when PublishPorts is set.
	NetworkMode string `json:"network_mode,omitempty"`
	// PublishPorts container ports published to the host for inbound connections
	PublishPorts []DockerPortPublish `json:"publish_ports,omitempty" validate:"omitempty,dive"`

	// RemoveOnExit remove the container on teardown; defaults to true when nil
	RemoveOnExit *bool `json:"remove_on_exit,omitempty"`
}

// IsRemoveOnExit resolve RemoveOnExit, defaulting to true when unset
func (p DockerRuntimeParams) IsRemoveOnExit() bool {
	return boolOrTrue(p.RemoveOnExit)
}

// DockerVolumeMetadata Docker-specific provisioning parameters passed as the `metadata`
// argument to VolumeManager.DefineVolume for a dockerVolumeManager. It carries the volume
// driver, its driver options, and labels; a zero value provisions a default "local" volume.
type DockerVolumeMetadata struct {
	// Driver the volume driver to provision with (e.g. "local"); defaults to the daemon's
	// default driver when empty
	Driver string `json:"driver,omitempty"`
	// DriverOpts driver-specific options (e.g. "type", "device", "o" for the local driver)
	DriverOpts map[string]string `json:"driver_opts,omitempty"`
	// Labels user-defined key/value metadata applied to the volume
	Labels map[string]string `json:"labels,omitempty"`
}
