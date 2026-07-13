// Package runtime - execute system calls in specific runtime environments
package runtime

import (
	"context"
	"io"
	"path/filepath"
	"time"

	"github.com/alwitt/goutils"
)

// ======================================================================================
// Container Runtime Common ENUM and Structs
// ======================================================================================

// timeoutExitCode the sentinel exit code reported when a tool is killed on timeout
// under the timeout-is-ok policy (the conventional "timed out" code).
const timeoutExitCode = 124

// InMemoryWritableDir a memory-backed writable directory overlaid on the (otherwise
// read-only) container rootfs. Backed by a tmpfs mount on Docker and by an
// emptyDir{medium: Memory} on Kubernetes.
type InMemoryWritableDir struct {
	// Path the directory within the container to back with the memory-backed writable mount
	Path string `json:"path" validate:"required" jsonschema:"the absolute directory path within the container to back with the memory-backed writable mount; required and must be an absolute path"`
	// SizeLimit size of the mount in bytes; defaults to DefaultInMemoryWritableDirSize when 0
	SizeLimit int64 `json:"size_limit,omitempty" validate:"omitempty,gt=0" jsonschema:"size of the mount in bytes; must be > 0 when set; defaults to 67108864 (64 MiB) when omitted"`
}

// Size resolve SizeLimit, defaulting when unset
func (m InMemoryWritableDir) Size() int64 {
	if m.SizeLimit <= 0 {
		return DefaultInMemoryWritableDirSize
	}
	return m.SizeLimit
}

// ContainerHostMount a host path bind-mounted into the container
type ContainerHostMount struct {
	// Path host path to mount
	Path string `json:"path" validate:"required" jsonschema:"the host path to mount; required and must be an absolute path"`
	// ReadOnly whether to mount the path read-only; defaults to true when nil
	ReadOnly *bool `json:"read_only,omitempty" jsonschema:"whether to mount the path read-only; defaults to true when omitted"`
	// MountPath path within the container to mount the host path. The mount path will
	// mirror the host path if this is not specified.
	MountPath *string `json:"mount_path,omitempty" jsonschema:"absolute path within the container to mount the host path at; mirrors the host path when omitted"`
}

// IsReadOnly resolve ReadOnly, defaulting to true when unset
func (m ContainerHostMount) IsReadOnly() bool {
	if m.ReadOnly == nil {
		return true
	}
	return *m.ReadOnly
}

// GetMountPath get the in-container mount path
func (m ContainerHostMount) GetMountPath() string {
	if m.MountPath != nil {
		return *m.MountPath
	}
	return m.Path
}

// ContainerVolume the shared minimum to request a persistent volume (Docker volume) /
// PersistentVolumeClaim (Kubernetes). The runtime-specific provisioning (Docker driver +
// opts, Kubernetes access modes + storage class) lives in the per-runtime controller
// client, not here.
type ContainerVolume struct {
	// Name identity of the volume / PVC
	Name string `json:"name" validate:"required" jsonschema:"identity of the volume / PVC; required"`
	// Size requested capacity in bytes. Honored by Kubernetes (storage request) and by
	// capacity-aware Docker volume drivers; the default Docker "local" driver treats it as
	// advisory.
	Size int64 `json:"size,omitempty" validate:"omitempty,gt=0" jsonschema:"requested capacity in bytes; must be > 0 when set. Honored by Kubernetes (storage request) and capacity-aware Docker volume drivers; the default Docker 'local' driver treats it as advisory"`
}

// ContainerVolumeMount mounts a named ContainerVolume / PVC into the container.
type ContainerVolumeMount struct {
	// Name the ContainerVolume / PVC name to mount
	Name string `json:"name" validate:"required" jsonschema:"the ContainerVolume / PVC name to mount; required"`
	// MountPath path within the container to mount at
	MountPath string `json:"mount_path" validate:"required" jsonschema:"the absolute path within the container to mount at; required and must be an absolute path"`
	// ReadOnly whether to mount read-only; defaults to false when nil. Unlike a host mount,
	// a persistent volume exists to be written to, so read-write is the sensible default.
	ReadOnly *bool `json:"read_only,omitempty" jsonschema:"whether to mount read-only; defaults to false when omitted. Unlike a host mount, a persistent volume exists to be written to, so read-write is the sensible default"`
}

// IsReadOnly resolve ReadOnly, defaulting to false when unset
func (m ContainerVolumeMount) IsReadOnly() bool {
	return boolOrFalse(m.ReadOnly)
}

// ContainerExtraHost an extra host-to-IP mapping injected into the container's /etc/hosts
type ContainerExtraHost struct {
	// Hosts the hostname to map
	Hosts []string `json:"host" validate:"required" jsonschema:"the hostnames to map to the address; required and must contain at least one hostname"`
	// Address the IP address the hostname resolves to
	Address string `json:"address" validate:"required,ip" jsonschema:"the IP address the hostnames resolve to; required and must be a valid IP address"`
}

// ContainerEnvVar an environment variable set on the container process
type ContainerEnvVar struct {
	// Name the environment variable name
	Name string `json:"name" validate:"required" jsonschema:"the environment variable name; required"`
	// Value the environment variable value
	Value string `json:"value" jsonschema:"the environment variable value; may be an empty string"`
}

// Default values applied for omitted container driver parameters.
const (
	// DefaultInMemoryWritableDirSize default memory-backed writable dir size in bytes (64 MiB)
	DefaultInMemoryWritableDirSize = int64(67108864)
	// DefaultContainerRunAsUser default container run-as user
	DefaultContainerRunAsUser = "nobody"
	// DefaultContainerRunAsGroup default container run-as group
	DefaultContainerRunAsGroup = "nogroup"
	// DefaultContainerWorkingDir default container working directory
	DefaultContainerWorkingDir = "/tmp"
	// DefaultContainerMemReservation default container memory reservation
	DefaultContainerMemReservation = "32m"
	// DefaultContainerMemLimit default container memory limit
	DefaultContainerMemLimit = "128m"
	// DefaultContainerStopSignal default signal used to stop the container process
	DefaultContainerStopSignal = goutils.ContainerStopSignalSIGINT
	// DefaultContainerTimeoutPolicy default packaging of a timeout into the result
	DefaultContainerTimeoutPolicy = goutils.ContainerTimeoutPolicyError
)

// ======================================================================================
// Container Runtime Params
// ======================================================================================

// StreamIOParams settings for when streaming STDIN, STDERR, and STDOUT to and from the container
type StreamIOParams struct {
	// DisplayRows TTY number of rows (in cells).
	DisplayRows uint16 `json:"display_rows" validate:"gte=30" jsonschema:"TTY number of rows (in cells); must be >= 30"`
	// DisplayCols TTY number of columns (in cells).
	DisplayCols uint16 `json:"display_cols" validate:"gte=80" jsonschema:"TTY number of columns (in cells); must be >= 80"`
}

// ContainerRuntimeParams parameters common to any container-orchestrating runtime (Docker
// containers, Kubernetes Jobs, ...). Each field maps onto a Pod/Container spec plus its
// security context, so runtime-specific params (e.g. DockerRuntimeParams) embed this base
// and add only the fields with no cross-runtime equivalent.
type ContainerRuntimeParams struct {
	// Image container image reference to run
	Image string `json:"image" validate:"required" jsonschema:"container image reference to run; required"`

	// Entrypoint container entrypoint
	Entrypoint []string `json:"entrypoint" validate:"required,gte=1" jsonschema:"container entrypoint; required and must contain at least one element"`

	// Commands ran by the entry point
	Commands []string `json:"commands,omitempty" validate:"-" jsonschema:"the arguments passed to the entrypoint"`

	// Streaming when streaming STDIN, STDERR, and STDOUT to and from the container,
	// additional configurations needed for streaming.
	Streaming *StreamIOParams `json:"streaming,omitempty" validate:"omitempty" jsonschema:"additional configuration needed when streaming STDIN, STDERR, and STDOUT to and from the container; omit for a non-streaming (batch) run"`

	// MemReservation soft memory reservation (e.g. "32m"); defaults when empty
	MemReservation string `json:"mem_reservation,omitempty" jsonschema:"soft memory reservation (e.g. '32m'); defaults to '32m' when omitted"`
	// MemLimit hard memory limit (e.g. "128m"); defaults when empty
	MemLimit string `json:"mem_limit,omitempty" jsonschema:"hard memory limit (e.g. '128m'); defaults to '128m' when omitted"`

	// WorkingDir working directory for the container process; defaults to DefaultContainerWorkingDir
	WorkingDir string `json:"working_dir,omitempty" jsonschema:"working directory for the container process; defaults to '/tmp' when omitted"`

	// WritableDirs memory-backed writable directories overlaid on the read-only rootfs
	WritableDirs []InMemoryWritableDir `json:"writable_dirs,omitempty" validate:"omitempty,dive" jsonschema:"memory-backed writable directories overlaid on the read-only rootfs"`
	// HostMounts host paths bind-mounted into the container
	HostMounts []ContainerHostMount `json:"host_mounts,omitempty" validate:"omitempty,dive" jsonschema:"host paths bind-mounted into the container"`
	// VolumeMounts named persistent volumes / PVCs mounted into the container. The volumes
	// themselves are provisioned by the runtime's controller client, not by this spec.
	VolumeMounts []ContainerVolumeMount `json:"volume_mounts,omitempty" validate:"omitempty,dive" jsonschema:"named persistent volumes / PVCs mounted into the container; the volumes themselves are provisioned by the runtime's controller client, not by this spec"`

	// AddCapabilities Linux capabilities to add back on top of the dropped-by-default set
	// (e.g. NET_BIND_SERVICE to bind ports below 1024)
	AddCapabilities []string `json:"add_caps,omitempty" jsonschema:"Linux capabilities to add back on top of the dropped-by-default set (e.g. NET_BIND_SERVICE to bind ports below 1024)"`

	// ExtraHosts additional host-to-IP mappings for the container
	ExtraHosts []ContainerExtraHost `json:"extra_hosts,omitempty" validate:"omitempty,dive" jsonschema:"additional host-to-IP mappings for the container"`
	// Environment additional environment variables for the container process
	Environment []ContainerEnvVar `json:"environment,omitempty" validate:"omitempty,dive" jsonschema:"additional environment variables for the container process"`

	// StopSignal signal sent to request the container process stop during teardown;
	// defaults to DefaultContainerStopSignal when empty
	StopSignal goutils.ContainerStopSignalENUMType `json:"stop_signal,omitempty" validate:"omitempty,container_stop_signal" jsonschema:"signal sent to request the container process stop during teardown; defaults to SIGINT when omitted"`

	// TimeoutSecs wall-clock timeout in seconds; clamped to the server ceiling. 0 (unset)
	// means clamp to the server ceiling (treated as +infinity before the min).
	TimeoutSecs int `json:"timeout_secs,omitempty" validate:"omitempty,gt=0" jsonschema:"wall-clock timeout in seconds; must be > 0 when set; clamped to the server ceiling. Omit (0) to clamp to the server ceiling"`
	// TimeoutPolicy how a timeout is packaged into the result; defaults to
	// DefaultContainerTimeoutPolicy when empty
	TimeoutPolicy goutils.ContainerTimeoutPolicyENUMType `json:"timeout_policy,omitempty" validate:"omitempty,container_timeout_policy" jsonschema:"how a timeout is packaged into the result; defaults to packaging the timeout as an error when omitted"`

	// ReadOnlyRootFS mount the container root filesystem read-only; defaults to true when nil
	ReadOnlyRootFS *bool `json:"read_only_rootfs,omitempty" jsonschema:"mount the container root filesystem read-only; defaults to true when omitted"`
	// DropAllCapabilities drop all Linux capabilities; defaults to true when nil
	DropAllCapabilities *bool `json:"drop_all_caps,omitempty" jsonschema:"drop all Linux capabilities; defaults to true when omitted"`
	// NoNewPrivileges set the no-new-privileges security option; defaults to true when nil
	NoNewPrivileges *bool `json:"no_new_privileges,omitempty" jsonschema:"set the no-new-privileges security option; defaults to true when omitted"`
}

// IsStreaming whether runtime setup for streaming
func (p ContainerRuntimeParams) IsStreaming() bool {
	return p.Streaming != nil
}

// IsReadOnlyRootFS resolve ReadOnlyRootFS, defaulting to true when unset
func (p ContainerRuntimeParams) IsReadOnlyRootFS() bool {
	return boolOrTrue(p.ReadOnlyRootFS)
}

// IsDropAllCapabilities resolve DropAllCapabilities, defaulting to true when unset
func (p ContainerRuntimeParams) IsDropAllCapabilities() bool {
	return boolOrTrue(p.DropAllCapabilities)
}

// IsNoNewPrivileges resolve NoNewPrivileges, defaulting to true when unset
func (p ContainerRuntimeParams) IsNoNewPrivileges() bool {
	return boolOrTrue(p.NoNewPrivileges)
}

// ResolvedStopSignal resolve StopSignal, defaulting when empty
func (p ContainerRuntimeParams) ResolvedStopSignal() goutils.ContainerStopSignalENUMType {
	if p.StopSignal == "" {
		return DefaultContainerStopSignal
	}
	return p.StopSignal
}

// Timeout convert `TimeoutSecs` to `time.Duration`
func (p ContainerRuntimeParams) Timeout() time.Duration {
	return time.Second * time.Duration(p.TimeoutSecs)
}

// ValidateMounts verify every mount path is absolute, as required by the container
// runtime: the in-container target of each writable dir, host mount, and volume mount, and
// the host-side source of each host mount. A named volume mount's source is a name rather
// than a path, so it is not path-checked here. Intended to run before the runtime builds
// its container config so a malformed mount fails fast.
func (p ContainerRuntimeParams) ValidateMounts() error {
	for _, dir := range p.WritableDirs {
		if !filepath.IsAbs(dir.Path) {
			return goutils.NewValidationError(
				"writable dir path '"+dir.Path+"' must be absolute", nil, true,
			)
		}
	}
	for _, mount := range p.HostMounts {
		if !filepath.IsAbs(mount.Path) {
			return goutils.NewValidationError(
				"host mount source '"+mount.Path+"' must be absolute", nil, true,
			)
		}
		if target := mount.GetMountPath(); !filepath.IsAbs(target) {
			return goutils.NewValidationError(
				"host mount target '"+target+"' must be absolute", nil, true,
			)
		}
	}
	for _, mount := range p.VolumeMounts {
		if !filepath.IsAbs(mount.MountPath) {
			return goutils.NewValidationError(
				"volume mount target '"+mount.MountPath+"' must be absolute", nil, true,
			)
		}
	}
	return nil
}

// ResolvedTimeoutPolicy resolve TimeoutPolicy, defaulting when empty
func (p ContainerRuntimeParams) ResolvedTimeoutPolicy() goutils.ContainerTimeoutPolicyENUMType {
	if p.TimeoutPolicy == "" {
		return DefaultContainerTimeoutPolicy
	}
	return p.TimeoutPolicy
}

// boolOrTrue resolve an optional bool, defaulting to true when nil
func boolOrTrue(v *bool) bool {
	if v == nil {
		return true
	}
	return *v
}

// boolOrFalse resolve an optional bool, defaulting to false when nil
func boolOrFalse(v *bool) bool {
	if v == nil {
		return false
	}
	return *v
}

// ======================================================================================
// Runtime Interface
// ======================================================================================

// SystemCallRuntime system call runtime operator. This will execute the system call with
// in runtime supported by the implementation.
type SystemCallRuntime interface {
	/*
		Start launch the runtime and begin executing the system cll

			@param ctx context.Context - execution context
	*/
	Start(ctx context.Context) error

	/*
		PipeInput only usable on runtimes configured with streaming, stream data from `input`
		into the runtime's STDIN until EOF.

			@param ctx context.Context - execution context
	*/
	PipeInput(input io.Reader) error

	/*
		PipeOutput only usable on runtimes configured with streaming, stream data from runtime
		STDERR and STDOUT to `output` until EOF

			@param ctx context.Context - execution context
	*/
	PipeOutput(output io.Writer) error

	/*
		Wait for the runtime to finish or time out, then collect the exit code and combined output.

			@param ctx context.Context - execution context
			@returns the execution result process status code and combined output
	*/
	Wait(ctx context.Context) (SystemCallResp, error)

	/*
		Cleanup tear down the runtime. For container backed runtimes that are configured with
		RemoveOnExit, the container associated with this runtime is removed; otherwise this
		is a NOOP beyond releasing internal resources.

			@param ctx context.Context - execution context
			@returns error if cleanup failed
	*/
	Cleanup(ctx context.Context) error
}

// SystemCallResp the fixed structured result for every runtime: the wrapped program's exit
// code and its combined output.
type SystemCallResp struct {
	// ExitCode the wrapped program's exit code.
	ExitCode int `json:"exit_code"`
	// Output the combined STDOUT+STDERR captured from the program in execution order
	Output string `json:"output"`
}

// ======================================================================================
// Runtime Storage Interface
// ======================================================================================

// VolumeManager a controller client that manages persistent volumes for a runtime (Docker
// volumes, Kubernetes PersistentVolumeClaims, ...). It provisions and inspects the volumes
// that a ContainerRuntimeParams.VolumeMounts entry refers to by name.
//
// Start must be called before any volume operation and Cleanup once the manager is no
// longer needed; the manager owns whatever client/connection it establishes in Start.
type VolumeManager interface {
	/*
		Start perform any initialization the manager needs before serving volume operations
		(e.g. establishing the Docker client connection).

			@param ctxt context.Context - the operational context
	*/
	Start(ctxt context.Context) error

	/*
		Cleanup release any resources the manager acquired in Start (e.g. closing the Docker
		client connection).

			@param ctxt context.Context - the operational context
	*/
	Cleanup(ctxt context.Context) error

	/*
		ListVolumes list the managed volumes, optionally restricted to those whose name
		carries a given prefix.

			@param ctxt context.Context - the operational context
			@param namePrefix *string - only volumes whose name starts with this prefix are
			    returned; an empty string returns all volumes
			@returns the matching volumes
	*/
	ListVolumes(ctxt context.Context, namePrefix *string) ([]ContainerVolume, error)

	/*
		GetVolume find a specific volume by name, returning it along with the identities of the
		entities that currently mount it (container names/IDs for Docker, pod names for
		Kubernetes). Returns a not-found error when no volume by that name exists.

			@param ctxt context.Context - the operational context
			@param name string - the volume name
			@returns the volume, and the identities of the entities currently mounting it
	*/
	GetVolume(ctxt context.Context, name string) (ContainerVolume, []string, error)

	/*
		DefineVolume create a new volume.

			@param ctxt context.Context - the operational context
			@param volume ContainerVolume - the volume to create
			@param metadata any - manager-specific provisioning parameters (e.g. Docker driver
			    and driver options); the concrete type is defined by the implementation
			@returns the created volume
	*/
	DefineVolume(ctxt context.Context, volume ContainerVolume, metadata any) (ContainerVolume, error)

	/*
		DeleteVolume delete a volume by name. Deleting a volume that does not exist is a
		no-op success (idempotent).

			@param ctxt context.Context - the operational context
			@param name string - the volume name
	*/
	DeleteVolume(ctxt context.Context, name string) error
}
