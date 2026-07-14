//go:build need_docker

package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests exercise dockerSystemCallRuntime against a live Docker daemon using the
// python:3.12-slim image. They cover the non-streaming, no-network path. They are gated
// behind the `need_docker` build tag; run with: `go test -tags need_docker ./runtime/...`.

const testImage = "python:3.12-slim"

// testPtr returns a pointer to v, for the *bool config fields.
func testPtr[T any](v T) *T { return &v }

// baseParams returns DockerRuntimeParams with the test image and a short timeout so a hung
// container can never wedge the suite. NetworkMode is left unset (defaults to "none").
func baseParams(entrypoint ...string) (ContainerCommand, DockerRuntimeParams) {
	return ContainerCommand{Entrypoint: entrypoint},
		DockerRuntimeParams{
			ContainerRuntimeParams: ContainerRuntimeParams{Image: testImage, TimeoutSecs: 60},
		}
}

// runOnce builds a runtime, drives Start -> Wait -> Cleanup, and returns the result. ANSI
// escapes are stripped so assertions match clean text. Cleanup is registered so the
// container is removed even if an assertion fails.
func runOnce(
	ctxt context.Context,
	t *testing.T,
	name string,
	command ContainerCommand,
	params DockerRuntimeParams,
) SystemCallResp {
	rt, err := NewDockerSystemCallRuntime(ctxt, name, command, params, true)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, rt.Cleanup(context.Background())) })

	require.NoError(t, rt.Start(ctxt))
	resp, err := rt.Wait(ctxt)
	require.NoError(t, err)
	return resp
}

// uniqueName builds a greppable, collision-free runtime/volume name.
func uniqueName(kind string) string {
	return fmt.Sprintf("goutils-runtest-%s-%s", kind, uuid.NewString())
}

func TestDockerRuntimeBasicExecution(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	// Exit 0 with a sentinel on stdout.
	{
		sentinel := "hello-" + uuid.NewString()
		command, params := baseParams("sh", "-c", fmt.Sprintf("echo %s; exit 0", sentinel))
		resp := runOnce(ctxt, t, uniqueName("basic"), command, params)
		assert.Equal(0, resp.ExitCode)
		assert.Contains(resp.Output, sentinel)
	}

	// A non-zero exit code is reported faithfully and does not surface as a Wait error.
	{
		command, params := baseParams("sh", "-c", "echo failing; exit 7")
		resp := runOnce(ctxt, t, uniqueName("exit7"), command, params)
		assert.Equal(7, resp.ExitCode)
		assert.Contains(resp.Output, "failing")
	}
}

func TestDockerRuntimeWritableDir(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	// A memory-backed writable dir overlaid on the read-only rootfs is writable.
	{
		sentinel := "work-" + uuid.NewString()
		script := fmt.Sprintf("echo %s > /work/f && cat /work/f", sentinel)
		command, params := baseParams("sh", "-c", script)
		params.WritableDirs = []InMemoryWritableDir{{Path: "/work"}}
		resp := runOnce(ctxt, t, uniqueName("writabledir"), command, params)
		assert.Equal(0, resp.ExitCode)
		assert.Contains(resp.Output, sentinel)
	}

	// Negative: the rootfs really is read-only, so writing outside a writable dir fails.
	{
		command, params := baseParams("sh", "-c", "echo x > /root/should-fail")
		resp := runOnce(ctxt, t, uniqueName("readonlyroot"), command, params)
		assert.NotEqual(0, resp.ExitCode)
	}
}

func TestDockerRuntimeHostMount(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	// Create a world-readable host dir + file so the default `nobody` user can read it.
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o755))
	sentinel := "host-" + uuid.NewString()
	hostFile := filepath.Join(dir, "hostfile.txt")
	require.NoError(t, os.WriteFile(hostFile, []byte(sentinel), 0o644))

	command, params := baseParams("sh", "-c", "cat /hostdata/hostfile.txt")
	params.HostMounts = []ContainerHostMount{{
		Path:      dir,
		MountPath: testPtr("/hostdata"),
		ReadOnly:  testPtr(true),
	}}
	resp := runOnce(ctxt, t, uniqueName("hostmount"), command, params)
	assert.Equal(0, resp.ExitCode)
	assert.Contains(resp.Output, sentinel)
}

func TestDockerRuntimeVolumeRoundTrip(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	// Provision a volume through the VolumeManager.
	mgr, err := NewDockerVolumeManager(ctxt)
	require.NoError(t, err)
	require.NoError(t, mgr.Start(ctxt))
	t.Cleanup(func() { assert.NoError(mgr.Cleanup(context.Background())) })

	volName := uniqueName("vol")
	_, err = mgr.DefineVolume(ctxt, ContainerVolume{Name: volName}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(mgr.DeleteVolume(context.Background(), volName)) })

	sentinel := "vol-" + uuid.NewString()

	// Writer container writes a sentinel into the volume. A fresh named volume is root-owned,
	// so run as root.
	{
		command, params := baseParams("sh", "-c", fmt.Sprintf("echo %s > /vol/data.txt", sentinel))
		params.RunAsUser = "root"
		params.RunAsGroup = "root"
		params.VolumeMounts = []ContainerVolumeMount{{Name: volName, MountPath: "/vol"}}
		resp := runOnce(ctxt, t, uniqueName("volwriter"), command, params)
		assert.Equal(0, resp.ExitCode)
	}

	// A separate reader container reads the file back, proving cross-container persistence.
	{
		command, params := baseParams("sh", "-c", "cat /vol/data.txt")
		params.RunAsUser = "root"
		params.RunAsGroup = "root"
		params.VolumeMounts = []ContainerVolumeMount{{Name: volName, MountPath: "/vol"}}
		resp := runOnce(ctxt, t, uniqueName("volreader"), command, params)
		assert.Equal(0, resp.ExitCode)
		assert.Contains(resp.Output, sentinel)
	}
}

func TestDockerRuntimeExtraHosts(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	// One ContainerExtraHost carrying multiple hostnames (Kubernetes-Pod-shaped: one IP ->
	// many hostnames) fans out into multiple docker "host:ip" lines. Verify both names
	// resolve to the shared address.
	const addr = "10.9.8.7"
	script := "getent hosts myhost.local; getent hosts alt.local"
	command, params := baseParams("sh", "-c", script)
	params.ExtraHosts = []ContainerExtraHost{{
		Hosts:   []string{"myhost.local", "alt.local"},
		Address: addr,
	}}
	resp := runOnce(ctxt, t, uniqueName("extrahosts"), command, params)
	assert.Equal(0, resp.ExitCode)
	assert.Contains(resp.Output, "myhost.local")
	assert.Contains(resp.Output, "alt.local")
	assert.Contains(resp.Output, addr)
}

func TestDockerRuntimeEnvVars(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	// Environment variables set on the container reach the process.
	script := `python -c 'import os; print("FOO="+os.environ.get("FOO","")); ` +
		`print("BAZ="+os.environ.get("BAZ",""))'`
	command, params := baseParams("sh", "-c", script)
	params.Environment = []ContainerEnvVar{
		{Name: "FOO", Value: "bar123"},
		{Name: "BAZ", Value: "qux"},
	}
	resp := runOnce(ctxt, t, uniqueName("envvars"), command, params)
	assert.Equal(0, resp.ExitCode)
	assert.Contains(resp.Output, "FOO=bar123")
	assert.Contains(resp.Output, "BAZ=qux")
}

// capBndBit is the bit position of CAP_NET_RAW in the capability bitmask (see
// <linux/capability.h>); the runtime under test drops all capabilities by default, so this
// bit is a clean present/absent signal for AddCapabilities.
const capBndBit = 13 // CAP_NET_RAW

// readCapBnd runs a container that prints its capability bounding set and returns the parsed
// mask. CapBnd (not CapEff) is the reliable signal: it reflects exactly what CapDrop/CapAdd
// control, independent of whether the process runs as root. The runtime runs as `nobody`, so
// CapEff would always be empty and tell us nothing.
func readCapBnd(
	ctxt context.Context,
	t *testing.T,
	name string,
	command ContainerCommand,
	params DockerRuntimeParams,
) uint64 {
	resp := runOnce(ctxt, t, name, command, params)
	require.Equal(t, 0, resp.ExitCode)

	var hex string
	for _, line := range strings.Split(resp.Output, "\n") {
		if _, after, found := strings.Cut(line, "CapBnd:"); found {
			hex = strings.TrimSpace(after)
			break
		}
	}
	require.NotEmpty(t, hex, "CapBnd not found in output: %q", resp.Output)
	mask, err := strconv.ParseUint(hex, 16, 64)
	require.NoError(t, err)
	return mask
}

func TestDockerRuntimeAddCapabilities(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	// The container prints its bounding capability set. python:3.12-slim has no capsh /
	// getpcaps, so read it straight from /proc/self/status.
	script := "grep CapBnd /proc/self/status"

	// Negative: with the default drop-all baseline and no additions, the bounding set is
	// empty, so the NET_RAW bit is absent.
	{
		command, params := baseParams("sh", "-c", script)
		mask := readCapBnd(ctxt, t, uniqueName("nocaps"), command, params)
		assert.Zero(mask&(uint64(1)<<capBndBit), "NET_RAW must be absent by default (CapBnd=%016x)", mask)
	}

	// Positive: adding NET_RAW back restores exactly that bit in the bounding set, proving
	// AddCapabilities is plumbed through to Docker's CapAdd.
	{
		command, params := baseParams("sh", "-c", script)
		params.AddCapabilities = []string{"NET_RAW"}
		mask := readCapBnd(ctxt, t, uniqueName("netraw"), command, params)
		assert.NotZero(mask&(uint64(1)<<capBndBit), "NET_RAW must be present when added (CapBnd=%016x)", mask)
	}
}

func TestDockerRuntimeEgress(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	// python:3.12-slim has no curl, so reach out with urllib. On a bridge network the
	// container can egress to the internet; print the HTTP status for a positive assertion.
	src := "import sys, urllib.request\n" +
		"try:\n" +
		"    r = urllib.request.urlopen('https://www.example.com', timeout=15)\n" +
		"    print('STATUS', r.status)\n" +
		"except Exception as e:\n" +
		"    print('ERROR', e); sys.exit(1)\n"

	command, params := baseParams("python", "-c", src)
	params.NetworkMode = "bridge"
	resp := runOnce(ctxt, t, uniqueName("egress"), command, params)
	assert.Equal(0, resp.ExitCode)
	assert.Contains(resp.Output, "STATUS 200")
}

// ingressServerSrc is a one-shot TCP server: it binds 0.0.0.0:50501, prints a LISTENING
// marker, accepts a single connection, prints the first whitespace-delimited word it
// received as "GOT:<word>", then exits 0.
const ingressServerSrc = `import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("0.0.0.0", 50501))
s.listen(1)
print("LISTENING", flush=True)
conn, _ = s.accept()
data = conn.recv(1024)
parts = data.split()
word = parts[0].decode() if parts else ""
print("GOT:" + word, flush=True)
conn.close()
s.close()
sys.exit(0)
`

func TestDockerRuntimeIngress(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	// Host-mount the server source, world-readable so the default `nobody` user can read it
	// (same pattern as TestDockerRuntimeHostMount). Mount the dir, reference the file within.
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "srv.py"), []byte(ingressServerSrc), 0o644))

	command, params := baseParams("python", "/mnt/srv.py")
	params.NetworkMode = "bridge"
	params.HostMounts = []ContainerHostMount{{
		Path:      dir,
		MountPath: testPtr("/mnt"),
		ReadOnly:  testPtr(true),
	}}
	// Publish the container port to 127.0.0.1:50501 (fixed host port -> no discovery needed;
	// Protocol/HostIP default to tcp / 127.0.0.1).
	params.PublishPorts = []DockerPortPublish{{ContainerPort: 50501, HostPort: 50501}}

	// Ingress needs the host to connect while the container runs, so drive the runtime
	// directly rather than via runOnce (which blocks on Wait).
	rt, err := NewDockerSystemCallRuntime(ctxt, uniqueName("ingress"), command, params, true)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(rt.Cleanup(context.Background())) })
	require.NoError(t, rt.Start(ctxt))

	log.Debug("Waiting for server in container to listen...")
	time.Sleep(time.Second)
	log.Debug("Try connecting...")

	// Dial with a retry loop so we connect only once the server is listening.
	var conn net.Conn
	for range 25 {
		conn, err = net.DialTimeout("tcp", "127.0.0.1:50501", time.Second)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.NoError(t, err, "failed to connect to published ingress port")

	_, err = conn.Write([]byte("hello world\n"))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	resp, err := rt.Wait(ctxt)
	require.NoError(t, err)
	assert.Equal(0, resp.ExitCode)
	assert.Contains(resp.Output, "GOT:hello")
}

// startAndWait builds a runtime, Starts it, and returns Wait's (resp, err) WITHOUT asserting
// on the error — used by tests (like timeout) whose expected outcome is a non-nil error.
// Cleanup is registered so the container is force-removed regardless.
func startAndWait(
	ctxt context.Context,
	t *testing.T,
	name string,
	command ContainerCommand,
	params DockerRuntimeParams,
) (SystemCallResp, error) {
	rt, err := NewDockerSystemCallRuntime(ctxt, name, command, params, true)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, rt.Cleanup(context.Background())) })
	require.NoError(t, rt.Start(ctxt))
	return rt.Wait(ctxt)
}

func TestDockerRuntimeTimeout(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	// A 2s TTL against a `sleep 3600` always times out. Force SIGKILL as the stop signal.
	buildParams := func() (ContainerCommand, DockerRuntimeParams) {
		c, p := baseParams("sleep", "3600")
		p.TimeoutSecs = 2
		p.StopSignal = goutils.ContainerStopSignalSIGKILL
		return c, p
	}

	// Sub-case A: default policy (TIMEOUT_IS_ERROR) surfaces the timeout as a DockerError.
	{
		start := time.Now()
		command, params := buildParams()
		_, err := startAndWait(ctxt, t, uniqueName("timeout-err"), command, params)
		assert.Error(err)
		var dockerErr goutils.DockerError
		assert.True(errors.As(err, &dockerErr))
		assert.Contains(err.Error(), "did not complete on time")
		// Killed on the ~2s deadline, nowhere near the 3600s sleep.
		assert.Less(time.Since(start), 30*time.Second)
	}

	// Sub-case B: TIMEOUT_IS_OK packages the timeout as a success carrying the sentinel exit
	// code (124), with no error.
	{
		command, params := buildParams()
		params.TimeoutPolicy = goutils.ContainerTimeoutPolicyOK
		resp, err := startAndWait(ctxt, t, uniqueName("timeout-ok"), command, params)
		assert.NoError(err)
		assert.Equal(124, resp.ExitCode)
	}
}

// streamingServerSrc reads lines from stdin; on a line equal to "PING" it prints the value of
// the TOKEN env var (a host-chosen sentinel) and exits 0.
const streamingServerSrc = `import os, sys
for line in sys.stdin:
    if line.strip() == "PING":
        print(os.environ.get("TOKEN", ""), flush=True)
        break
`

func TestDockerRuntimeStreaming(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	// Host-mount the server script, world-readable (same pattern as the ingress/host-mount
	// tests).
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "srv.py"), []byte(streamingServerSrc), 0o644))

	// The token is host-generated and passed via env, so the "random" reply is deterministic.
	token := uuid.NewString()

	command, params := baseParams("python", "/mnt/srv.py")
	params.Streaming = &StreamIOParams{DisplayRows: 40, DisplayCols: 120}
	params.Environment = []ContainerEnvVar{{Name: "TOKEN", Value: token}}
	params.HostMounts = []ContainerHostMount{{
		Path:      dir,
		MountPath: testPtr("/mnt"),
		ReadOnly:  testPtr(true),
	}}

	rt, err := NewDockerSystemCallRuntime(ctxt, uniqueName("streaming"), command, params, true)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(rt.Cleanup(context.Background())) })
	require.NoError(t, rt.Start(ctxt))

	// PipeOutput blocks until the container closes the stream (EOF), so drain it in a
	// goroutine. buf is read only after wg.Wait(), so no lock is needed.
	var buf bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(rt.PipeOutput(&buf))
	}()

	// Send PING and half-close stdin; the server replies with TOKEN and exits.
	require.NoError(t, rt.PipeInput(strings.NewReader("PING\n")))

	resp, err := rt.Wait(ctxt)
	require.NoError(t, err)
	assert.Equal(0, resp.ExitCode)
	// Streaming delivers output live via PipeOutput, not in the result.
	assert.Empty(resp.Output)

	wg.Wait()
	// TTY echoes stdin, so the captured stream contains the echoed PING plus the token.
	assert.Contains(buf.String(), token)
}

func TestDockerRuntimeStreamingGuards(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	// A non-streaming runtime (Streaming nil) rejects PipeInput/PipeOutput with a consistency
	// error rather than operating on a nil hijacked connection.
	command, params := baseParams("sh", "-c", "true")
	rt, err := NewDockerSystemCallRuntime(ctxt, uniqueName("noguard"), command, params, true)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(rt.Cleanup(context.Background())) })

	var consistencyErr goutils.ConsistencyError

	inErr := rt.PipeInput(strings.NewReader("x"))
	assert.Error(inErr)
	assert.True(errors.As(inErr, &consistencyErr))

	outErr := rt.PipeOutput(&bytes.Buffer{})
	assert.Error(outErr)
	assert.True(errors.As(outErr, &consistencyErr))
}
