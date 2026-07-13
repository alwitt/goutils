//go:build need_docker

package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/alwitt/goutils"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests exercise dockerVolumeManager against a live Docker daemon. They are gated
// behind the `need_docker` build tag so the normal `go test ./...` run does not require a
// daemon. Run with: `go test -tags need_docker ./runtime/...`.

// uniqueVolumeName builds a greppable, collision-free test volume name. The shared prefix
// lets a crashed run leave identifiable debris (see `docker volume ls | grep`).
func uniqueVolumeName() string {
	return fmt.Sprintf("goutils-voltest-%s", uuid.NewString())
}

// setupVolumeManager builds a manager, calls Start, and registers Cleanup for cleanup.
func setupVolumeManager(ctxt context.Context, t *testing.T) VolumeManager {
	mgr, err := NewDockerVolumeManager(ctxt)
	require.NoError(t, err)
	require.NoError(t, mgr.Start(ctxt))
	t.Cleanup(func() {
		assert.NoError(t, mgr.Cleanup(context.Background()))
	})
	return mgr
}

func TestDockerVolumeManagerLifecycle(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	mgr := setupVolumeManager(ctxt, t)

	// A second Start must fail the already-set-up guard.
	{
		err := mgr.Start(ctxt)
		assert.Error(err)
		var consistencyErr goutils.ConsistencyError
		assert.True(errors.As(err, &consistencyErr))
	}

	// Define a volume with nil metadata.
	name0 := uniqueVolumeName()
	{
		created, err := mgr.DefineVolume(ctxt, ContainerVolume{Name: name0}, nil)
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(mgr.DeleteVolume(context.Background(), name0)) })
		assert.Equal(name0, created.Name)
	}

	// Define a volume with DockerVolumeMetadata (driver + labels).
	name1 := uniqueVolumeName()
	{
		meta := DockerVolumeMetadata{
			Driver: "local",
			Labels: map[string]string{"goutils-voltest": "true"},
		}
		created, err := mgr.DefineVolume(ctxt, ContainerVolume{Name: name1}, meta)
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(mgr.DeleteVolume(context.Background(), name1)) })
		assert.Equal(name1, created.Name)
	}

	// GetVolume returns the volume and, since nothing mounts it, an empty mounters slice.
	{
		got, mounters, err := mgr.GetVolume(ctxt, name0)
		require.NoError(t, err)
		assert.Equal(name0, got.Name)
		assert.Empty(mounters)
	}

	// ListVolumes(nil) returns everything, including both test volumes.
	{
		all, err := mgr.ListVolumes(ctxt, nil)
		require.NoError(t, err)
		listedNames := make(map[string]bool, len(all))
		for _, v := range all {
			listedNames[v.Name] = true
		}
		assert.True(listedNames[name0])
		assert.True(listedNames[name1])
	}

	// ListVolumes(&prefix) restricts to the shared test prefix; a control volume created
	// under a different prefix must be excluded.
	{
		control := fmt.Sprintf("goutils-ctrltest-%s", uuid.NewString())
		_, err := mgr.DefineVolume(ctxt, ContainerVolume{Name: control}, nil)
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(mgr.DeleteVolume(context.Background(), control)) })

		prefix := "goutils-voltest-"
		filtered, err := mgr.ListVolumes(ctxt, &prefix)
		require.NoError(t, err)
		for _, v := range filtered {
			assert.Contains(v.Name, prefix)
			assert.NotEqual(control, v.Name)
		}
		filteredNames := make(map[string]bool, len(filtered))
		for _, v := range filtered {
			filteredNames[v.Name] = true
		}
		assert.True(filteredNames[name0])
		assert.True(filteredNames[name1])
	}

	// DeleteVolume removes it; a follow-up GetVolume reports not-found.
	{
		require.NoError(t, mgr.DeleteVolume(ctxt, name0))
		_, _, err := mgr.GetVolume(ctxt, name0)
		assert.Error(err)
		var notFoundErr goutils.NotFoundError
		assert.True(errors.As(err, &notFoundErr))
	}
}

func TestDockerVolumeManagerDeleteIdempotent(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	mgr := setupVolumeManager(ctxt, t)

	// Deleting a volume that was never created is a no-op success.
	err := mgr.DeleteVolume(ctxt, uniqueVolumeName())
	assert.NoError(err)
}

func TestDockerVolumeManagerGetMissing(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	mgr := setupVolumeManager(ctxt, t)

	got, mounters, err := mgr.GetVolume(ctxt, uniqueVolumeName())
	assert.Error(err)
	var notFoundErr goutils.NotFoundError
	assert.True(errors.As(err, &notFoundErr))
	assert.Equal(ContainerVolume{}, got)
	assert.Empty(mounters)
}

func TestDockerVolumeManagerBadMetadata(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	mgr := setupVolumeManager(ctxt, t)

	// A non-nil metadata of the wrong concrete type is a consistency error; no volume is
	// created. Register a defensive cleanup in case the guard ever regresses.
	name := uniqueVolumeName()
	t.Cleanup(func() { _ = mgr.DeleteVolume(context.Background(), name) })

	created, err := mgr.DefineVolume(ctxt, ContainerVolume{Name: name}, "not-a-metadata")
	assert.Error(err)
	var consistencyErr goutils.ConsistencyError
	assert.True(errors.As(err, &consistencyErr))
	assert.Equal(ContainerVolume{}, created)

	// The volume must not exist.
	_, _, getErr := mgr.GetVolume(ctxt, name)
	var notFoundErr goutils.NotFoundError
	assert.True(errors.As(getErr, &notFoundErr))
}

func TestDockerVolumeManagerNotSetup(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)
	ctxt := context.Background()

	// A manager that never had Start called must fail every operation via checkReady. This
	// case needs no daemon.
	mgr, err := NewDockerVolumeManager(ctxt)
	require.NoError(t, err)

	assertNotReady := func(err error) {
		assert.Error(err)
		var consistencyErr goutils.ConsistencyError
		assert.True(errors.As(err, &consistencyErr))
	}

	{
		_, err := mgr.ListVolumes(ctxt, nil)
		assertNotReady(err)
	}
	{
		_, _, err := mgr.GetVolume(ctxt, uniqueVolumeName())
		assertNotReady(err)
	}
	{
		_, err := mgr.DefineVolume(ctxt, ContainerVolume{Name: uniqueVolumeName()}, nil)
		assertNotReady(err)
	}
	{
		err := mgr.DeleteVolume(ctxt, uniqueVolumeName())
		assertNotReady(err)
	}
}
