package runtime

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"sync/atomic"

	"github.com/alwitt/goutils"
	"github.com/apex/log"
	dockerUnits "github.com/docker/go-units"
	"github.com/go-playground/validator/v10"
	dockerContainer "github.com/moby/moby/api/types/container"
	dockerNetwork "github.com/moby/moby/api/types/network"
	dockerClient "github.com/moby/moby/client"
	"github.com/oklog/ulid/v2"
)

// dockerSystemCallRuntime a Docker-container-backed system call runtime.
//
// The system call runs as the container's entrypoint inside a sand boxed container hardened by
// default (read-only rootfs, all capabilities dropped, no-new-privileges) and isolated from the
// network unless the config opts in.
type dockerSystemCallRuntime struct {
	goutils.Component

	params DockerRuntimeParams

	instanceID  string
	cli         *dockerClient.Client
	name        string
	containerID string
	stripANSI   bool

	// attach when streaming INPUT and OUTPUT
	attach dockerClient.HijackedResponse

	// started guards Start so the container is created at most once.
	started atomic.Bool
	// tornDown guards Cleanup so the container is removed / the client closed at most
	// once. After cleanup the container ID is gone and must not be acted on again.
	tornDown atomic.Bool

	// command what to run in the container
	command ContainerCommand
}

/*
NewDockerSystemCallRuntime define a new Docker-container-backed system call runtime for a
single system call. The runtime generates its own ULID instance ID and does not touch Docker
until Start is called.

	@param ctx context.Context - execution context (used for log-tag derivation)
	@param name string - a descriptive name for the runtime
	@param command ContainerCommandParams - the command to execute within the container
	@param params DockerRuntimeParams - docker runtime parameter
	@param clearANSIFromOutput bool - whether to remove ANSI escape sequences from output
	    when returning system call output
	@returns the new runtime
*/
func NewDockerSystemCallRuntime(
	_ context.Context,
	name string,
	command ContainerCommand,
	params DockerRuntimeParams,
	clearANSIFromOutput bool,
) (SystemCallRuntime, error) {
	instanceID := ulid.Make().String()

	logTags := log.Fields{
		"module":    "runtime",
		"component": "docker-runtime",
		"name":      name,
		"instance":  instanceID,
	}

	validate := validator.New()

	if err := goutils.RegisterWithValidator(validate); err != nil {
		return nil, goutils.NewRuntimeError("failed to register custom validator types", err, true)
	}

	if err := validate.Struct(&params); err != nil {
		return nil, goutils.NewValidationError("docker runtime params is invalid", err, true)
	}

	if err := validate.Struct(&command); err != nil {
		return nil, goutils.NewValidationError("runtime command is invalid", err, true)
	}

	instance := &dockerSystemCallRuntime{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
				goutils.ModifyLogMetadataByMCPRequestParam,
			},
		},
		params:      params,
		instanceID:  instanceID,
		cli:         nil,
		containerID: "",
		stripANSI:   clearANSIFromOutput,
		command:     command,
	}
	instance.name = instance.containerName(name, instanceID)
	instance.LogTags["container"] = instance.name

	return instance, nil
}

// containerName build a Docker-safe container name for this runtime instance
func (d *dockerSystemCallRuntime) containerName(name, instanceID string) string {
	return url.PathEscape(fmt.Sprintf("%s.%s", name, instanceID))
}

// Start connect to Docker, build the container from the config, and start it.
func (d *dockerSystemCallRuntime) Start(ctx context.Context) error {
	if !d.started.CompareAndSwap(false, true) {
		return goutils.NewConsistencyError(
			"'"+d.name+"' runtime already started", nil, true,
		)
	}

	logTags := d.GetLogTagsForContext(ctx)

	// ------------------------------------------------------------------------------------
	// Validate mount paths before touching Docker so a malformed mount fails fast.

	if err := d.params.ValidateMounts(); err != nil {
		return err
	}

	// ------------------------------------------------------------------------------------
	// Docker client

	cli, err := dockerClient.New(dockerClient.FromEnv)
	if err != nil {
		return goutils.NewDockerError("failed to build docker client", err, true)
	}
	d.cli = cli

	// ------------------------------------------------------------------------------------
	// Build container configuration

	exposedPorts, portBindings, err := d.buildPortMappings()
	if err != nil {
		return err
	}
	containerCfg := d.buildContainerConfig(d.command.Entrypoint, d.command.Commands, exposedPorts)
	hostCfg, err := d.buildHostConfig(portBindings)
	if err != nil {
		return err
	}

	// ------------------------------------------------------------------------------------
	// Create the container

	created, err := d.cli.ContainerCreate(ctx, dockerClient.ContainerCreateOptions{
		Config:     containerCfg,
		HostConfig: hostCfg,
		Name:       d.name,
	})
	if err != nil {
		return goutils.NewDockerError(
			fmt.Sprintf("failed to create container for '%s'", d.name), err, true,
		)
	}
	d.containerID = created.ID
	for _, warning := range created.Warnings {
		log.
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Warnf("container create warning: %s", warning)
	}

	// ------------------------------------------------------------------------------------
	// Attach to the container streams (before start)

	if d.params.IsStreaming() {
		attach, err := d.cli.ContainerAttach(
			ctx, d.containerID, dockerClient.ContainerAttachOptions{
				Stream: true,
				Stdin:  true,
				Stdout: true,
				Stderr: true,
			},
		)
		if err != nil {
			return goutils.NewDockerError("failed to attach to container '"+d.name+"'", err, true)
		}
		d.attach = attach.HijackedResponse
	}

	// ------------------------------------------------------------------------------------
	// Start the container

	if _, err := d.cli.ContainerStart(
		ctx, d.containerID, dockerClient.ContainerStartOptions{},
	); err != nil {
		return goutils.NewDockerError(
			fmt.Sprintf("failed to start container for '%s'", d.name), err, true,
		)
	}

	// ------------------------------------------------------------------------------------
	// Size the TTY to the requested geometry

	if d.params.IsStreaming() {
		if _, err := d.cli.ContainerResize(
			ctx, d.containerID, dockerClient.ContainerResizeOptions{
				Height: uint(d.params.Streaming.DisplayRows),
				Width:  uint(d.params.Streaming.DisplayCols),
			},
		); err != nil {
			return goutils.NewDockerError("failed to set container '"+d.name+"' TTY size", err, true)
		}
	}

	log.
		WithFields(goutils.UpdateCodePositionInTags(logTags)).
		Infof("Started container %s for '%s'", d.containerID, d.name)

	return nil
}

// PipeInput pipe user input into the container's STDIN over the hijacked connection.
//
// This copies from input until it is exhausted (EOF); for a long-lived stream the reader is
// expected to stay open for the life of the session and unblock via Cleanup. The write half
// is deliberately NOT closed here: on a TTY-enabled container a socket half-close does not
// signal STDIN EOF to the process anyway (that requires the terminal EOF character), and
// worse, closing it immediately after a bounded write races the daemon still forwarding those
// bytes into the container TTY and silently drops them. The connection is fully torn down by
// Cleanup instead.
func (d *dockerSystemCallRuntime) PipeInput(input io.Reader) error {
	if !d.params.IsStreaming() {
		return goutils.NewConsistencyError("'"+d.name+"' does not support streaming", nil, true)
	}
	if _, err := io.Copy(d.attach.Conn, input); err != nil {
		return goutils.NewDockerError("'"+d.name+"' INPUT pipe failure", err, true)
	}
	return nil
}

// PipeOutput pipe the container's STDOUT/STDERR into the buffer. As the container runs with a
// TTY, the attached reader is a single raw stream and needs no stdcopy de-multiplexing.
func (d *dockerSystemCallRuntime) PipeOutput(output io.Writer) error {
	if !d.params.IsStreaming() {
		return goutils.NewConsistencyError("'"+d.name+"' does not support streaming", nil, true)
	}
	_, err := io.Copy(output, d.attach.Reader)
	if err != nil {
		return goutils.NewDockerError("'"+d.name+"' OUTPUT pipe failure", err, true)
	}
	return nil
}

// WaitOnCompletion wait for the container to finish or time out, then collect the exit
// code and combined output.
func (d *dockerSystemCallRuntime) Wait(ctx context.Context) (SystemCallResp, error) {
	waitCtx := ctx
	if d.params.TimeoutSecs > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, d.params.Timeout())
		defer cancel()
	}

	waitResult := d.cli.ContainerWait(
		waitCtx, d.containerID, dockerClient.ContainerWaitOptions{
			Condition: dockerContainer.WaitConditionNotRunning,
		},
	)

	select {
	case <-waitCtx.Done():
		// The wait context closed: either the parent ctx was cancelled (a genuine abort) or
		// only the derived timeout deadline fired. Either way the container is stopped and
		// whatever output it produced is collected; the two cases differ only in how the
		// result is attributed (see handleTimeout).
		return d.handleTimeout(ctx, waitCtx)

	case err := <-waitResult.Error:
		if err != nil {
			return SystemCallResp{}, goutils.NewDockerError(
				"'"+d.name+"' container wait failed", err, true,
			)
		}
		// A closed error channel without a result is unexpected; fall back to inspect.
		return d.collectResult(ctx, d.inspectExitCode(ctx))

	case res := <-waitResult.Result:
		if res.Error != nil {
			return SystemCallResp{}, goutils.NewDockerError(
				"'"+d.name+"' container wait reported error: "+res.Error.Message,
				nil, true,
			)
		}
		return d.collectResult(ctx, int(res.StatusCode))
	}
}

// handleTimeout stop the container after its wait context closed, collect whatever output
// was produced, and package the result. The parent ctx being cancelled is a genuine abort
// (always surfaced as an error, regardless of TimeoutPolicy); only the derived timeout
// deadline firing is a tool timeout, which honors TimeoutPolicy.
func (d *dockerSystemCallRuntime) handleTimeout(
	ctx, waitCtx context.Context,
) (SystemCallResp, error) {
	logTags := d.GetLogTagsForContext(ctx)

	// The parent ctx carrying an error means the caller aborted (cancel / shutdown), as
	// opposed to only the derived wait deadline expiring.
	aborted := ctx.Err() != nil

	if aborted {
		log.
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Warnf("'%s' aborted; stopping container %s", d.name, d.containerID)
	} else {
		log.
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Warnf("'%s' timed out; stopping container %s", d.name, d.containerID)
	}

	// Teardown runs on a context derived from one that just closed, so use a fresh,
	// un-cancellable context bounded by nothing but the stop grace period.
	stopCtx := context.WithoutCancel(waitCtx)
	stopSignal := string(d.params.ResolvedStopSignal())
	if _, err := d.cli.ContainerStop(stopCtx, d.containerID, dockerClient.ContainerStopOptions{
		Signal: stopSignal,
	}); err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Warn("container stop returned an error")
	}

	output := d.resultOutput(stopCtx)

	// A genuine abort is always an error; it is not a tool timeout and the TimeoutPolicy does
	// not apply.
	if aborted {
		return SystemCallResp{ExitCode: d.inspectExitCode(stopCtx), Output: output},
			goutils.NewDockerError(
				fmt.Sprintf("'%s' was aborted before completion", d.name), ctx.Err(), true,
			)
	}

	// timeout-is-ok: a normal successful result carrying the sentinel exit code.
	if d.params.ResolvedTimeoutPolicy() == goutils.ContainerTimeoutPolicyOK {
		return SystemCallResp{ExitCode: timeoutExitCode, Output: output}, nil
	}

	// timeout-is-error (default): surface the timeout as an error so the caller can set
	// IsError: true, while still returning any partial output that was captured.
	return SystemCallResp{ExitCode: d.inspectExitCode(stopCtx), Output: output},
		goutils.NewDockerError(
			fmt.Sprintf("'%s' did not complete on time", d.name), waitCtx.Err(), true,
		)
}

// collectResult package a completed container's result with the given exit code.
func (d *dockerSystemCallRuntime) collectResult(
	ctx context.Context, exitCode int,
) (SystemCallResp, error) {
	return SystemCallResp{ExitCode: exitCode, Output: d.resultOutput(ctx)}, nil
}

// resultOutput the combined output to place in the result. For a streaming runtime the
// output was already delivered live to the caller via PipeOutput, so it is not re-read into
// the result (which would duplicate it); batch runtimes read the combined output post-hoc.
func (d *dockerSystemCallRuntime) resultOutput(ctx context.Context) string {
	if d.params.IsStreaming() {
		return ""
	}
	return d.readCombinedOutput(ctx)
}

// ansiEscapeSequence matches ANSI / VT escape sequences in raw terminal output: the 7-bit
// "ESC <Fe>" forms, the standalone 8-bit C1 controls, and CSI sequences (both the 7-bit "ESC ["
// and 8-bit 0x9B introducers).
var ansiEscapeSequence = regexp.MustCompile(
	`(?:\x1B[@-Z\\-_]|[\x80-\x9A\x9C-\x9F]|(?:\x1B\[|\x9B)[0-?]*[ -/]*[@-~])`,
)

// stripANSIEscapes removes all ANSI escape sequences from the given output bytes, returning the
// cleaned slice.
func stripANSIEscapes(data []byte) []byte {
	return ansiEscapeSequence.ReplaceAll(data, nil)
}

// readCombinedOutput read the container's combined STDOUT+STDERR post-hoc. Because the
// container runs with a TTY the log stream is a single raw combined stream needing no
// stdcopy de-multiplexing.
func (d *dockerSystemCallRuntime) readCombinedOutput(ctx context.Context) string {
	logTags := d.GetLogTagsForContext(ctx)

	logs, err := d.cli.ContainerLogs(ctx, d.containerID, dockerClient.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Error("failed to read container output")
		return ""
	}
	defer func() {
		_ = logs.Close()
	}()

	raw, err := io.ReadAll(logs)
	if err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Error("failed to drain container output")
		// Return whatever was read before the error.
	}
	if d.stripANSI {
		raw = stripANSIEscapes(raw)
	}
	return string(raw)
}

// inspectExitCode read the container's exit code via inspect. Used when the exit code is
// not available from the wait result (e.g. after a timeout-kill). Returns 0 on failure.
func (d *dockerSystemCallRuntime) inspectExitCode(ctx context.Context) int {
	logTags := d.GetLogTagsForContext(ctx)
	inspected, err := d.cli.ContainerInspect(
		ctx, d.containerID, dockerClient.ContainerInspectOptions{},
	)
	if err != nil {
		log.
			WithError(err).
			WithFields(goutils.UpdateCodePositionInTags(logTags)).
			Warn("failed to inspect container for exit code")
		return 0
	}
	if inspected.Container.State == nil {
		return 0
	}
	return inspected.Container.State.ExitCode
}

// Cleanup remove the container when RemoveOnExit, then release the Docker client. Safe
// to call on every exit path; the container is force-removed unconditionally so no
// orphan can accumulate.
func (d *dockerSystemCallRuntime) Cleanup(ctx context.Context) error {
	if !d.tornDown.CompareAndSwap(false, true) {
		return nil
	}
	if d.cli == nil {
		return nil
	}

	// Close client connection
	defer func() {
		_ = d.cli.Close()
	}()

	if d.params.IsStreaming() {
		// Close the hijacked connection so the input/output transfer loops unwind.
		d.attach.Close()
	}

	if d.containerID == "" || !d.params.IsRemoveOnExit() {
		return nil
	}

	if _, err := d.cli.ContainerRemove(ctx, d.containerID, dockerClient.ContainerRemoveOptions{
		Force: true,
	}); err != nil {
		return goutils.NewDockerError(
			"'"+d.name+"' container removal failed", err, true,
		)
	}
	return nil
}

// buildContainerConfig assemble the container.Config from the runtime params and
// the assembled invocation. The container runs with a TTY so the combined STDOUT/STDERR
// arrives as a single ordered stream; as a batch run there is no stdin.
func (d *dockerSystemCallRuntime) buildContainerConfig(
	entrypoint, cmd []string, exposedPorts dockerNetwork.PortSet,
) *dockerContainer.Config {
	runAsUser := d.params.RunAsUser
	if runAsUser == "" {
		runAsUser = DefaultContainerRunAsUser
	}
	runAsGroup := d.params.RunAsGroup
	if runAsGroup == "" {
		runAsGroup = DefaultContainerRunAsGroup
	}
	user := runAsUser + ":" + runAsGroup

	workingDir := d.params.WorkingDir
	if workingDir == "" {
		workingDir = DefaultContainerWorkingDir
	}

	env := make([]string, 0, len(d.params.Environment))
	for _, entry := range d.params.Environment {
		env = append(env, entry.Name+"="+entry.Value)
	}

	configs := &dockerContainer.Config{
		Image:        d.params.Image,
		Entrypoint:   entrypoint,
		Cmd:          cmd,
		Env:          env,
		User:         user,
		WorkingDir:   workingDir,
		ExposedPorts: exposedPorts,
		StopSignal:   string(d.params.ResolvedStopSignal()),
		Tty:          true,
	}

	if d.params.IsStreaming() {
		configs.OpenStdin = true
		configs.StdinOnce = false
		configs.AttachStdin = true
		configs.AttachStdout = true
		configs.AttachStderr = true
	}

	return configs
}

// buildHostConfig assemble the container.HostConfig from the runtime params,
// applying the sandbox hardening defaults.
func (d *dockerSystemCallRuntime) buildHostConfig(
	portBindings dockerNetwork.PortMap,
) (*dockerContainer.HostConfig, error) {
	params := d.params

	memReservation := params.MemReservation
	if memReservation == "" {
		memReservation = DefaultContainerMemReservation
	}
	memReservationBytes, err := dockerUnits.RAMInBytes(memReservation)
	if err != nil {
		return nil, goutils.NewDockerError(
			fmt.Sprintf("invalid memory reservation '%s'", memReservation), err, true,
		)
	}
	memLimit := params.MemLimit
	if memLimit == "" {
		memLimit = DefaultContainerMemLimit
	}
	memLimitBytes, err := dockerUnits.RAMInBytes(memLimit)
	if err != nil {
		return nil, goutils.NewDockerError(
			fmt.Sprintf("invalid memory limit '%s'", memLimit), err, true,
		)
	}

	networkMode := params.NetworkMode
	if networkMode == "" {
		networkMode = DefaultDockerNetworkMode
	}

	// Writable tmpfs mounts for the otherwise read-only rootfs.
	var tmpfs map[string]string
	if len(params.WritableDirs) > 0 {
		tmpfs = make(map[string]string, len(params.WritableDirs))
		for _, dir := range params.WritableDirs {
			tmpfs[dir.Path] = fmt.Sprintf("size=%d", dir.Size())
		}
	}

	// Host path bind mounts.
	var binds []string
	for _, mount := range params.HostMounts {
		bind := mount.Path + ":" + mount.GetMountPath()
		if mount.IsReadOnly() {
			bind += ":ro"
		}
		binds = append(binds, bind)
	}

	// Named volume mounts. Docker's bind syntax "source:target[:ro]" treats a source without
	// a leading '/' as a named volume rather than a host path, so a ContainerVolumeMount maps
	// onto the same Binds slice. The volume itself must already be provisioned (by the
	// VolumeManager); this only attaches it.
	for _, mount := range params.VolumeMounts {
		bind := mount.Name + ":" + mount.MountPath
		if mount.IsReadOnly() {
			bind += ":ro"
		}
		binds = append(binds, bind)
	}

	// Extra host-to-IP mappings in docker's "host:ip" form.
	var extraHosts []string
	for _, oneExtra := range params.ExtraHosts {
		for _, oneHost := range oneExtra.Hosts {
			extraHosts = append(extraHosts, oneHost+":"+oneExtra.Address)
		}
	}

	var capDrop []string
	if params.IsDropAllCapabilities() {
		capDrop = []string{"ALL"}
	}
	var securityOpt []string
	if params.IsNoNewPrivileges() {
		securityOpt = []string{"no-new-privileges=true"}
	}

	return &dockerContainer.HostConfig{
		NetworkMode:    dockerContainer.NetworkMode(networkMode),
		PortBindings:   portBindings,
		Binds:          binds,
		Tmpfs:          tmpfs,
		ExtraHosts:     extraHosts,
		CapDrop:        capDrop,
		CapAdd:         params.AddCapabilities,
		SecurityOpt:    securityOpt,
		ReadonlyRootfs: params.IsReadOnlyRootFS(),
		// The container lifecycle is managed explicitly via Cleanup; AutoRemove would race
		// the ContainerWait/inspect performed while collecting the result.
		AutoRemove: false,
		Resources: dockerContainer.Resources{
			Memory:            memLimitBytes,
			MemoryReservation: memReservationBytes,
		},
	}, nil
}

// buildPortMappings translate the runtime PublishPorts into the docker exposed-port set
// and host port bindings used to accept inbound connections.
func (d *dockerSystemCallRuntime) buildPortMappings() (
	dockerNetwork.PortSet, dockerNetwork.PortMap, error,
) {
	publishPorts := d.params.PublishPorts
	if len(publishPorts) == 0 {
		return nil, nil, nil
	}

	exposed := make(dockerNetwork.PortSet, len(publishPorts))
	bindings := make(dockerNetwork.PortMap, len(publishPorts))

	for _, publish := range publishPorts {
		port, ok := dockerNetwork.PortFrom(
			publish.ContainerPort, dockerNetwork.IPProtocol(publish.ResolvedProtocol()),
		)
		if !ok {
			return nil, nil, goutils.NewDockerError(
				fmt.Sprintf(
					"invalid published port %d/%s",
					publish.ContainerPort, publish.ResolvedProtocol(),
				), nil, true,
			)
		}

		hostIP, err := netip.ParseAddr(publish.ResolvedHostIP())
		if err != nil {
			return nil, nil, goutils.NewDockerError(
				fmt.Sprintf("invalid published host IP '%s'", publish.ResolvedHostIP()), err, true,
			)
		}

		hostPort := ""
		if publish.HostPort != 0 {
			hostPort = strconv.Itoa(int(publish.HostPort))
		}

		exposed[port] = struct{}{}
		bindings[port] = append(bindings[port], dockerNetwork.PortBinding{
			HostIP:   hostIP,
			HostPort: hostPort,
		})
	}

	return exposed, bindings, nil
}
