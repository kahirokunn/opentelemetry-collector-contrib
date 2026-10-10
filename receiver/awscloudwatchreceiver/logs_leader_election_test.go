// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awscloudwatchreceiver

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/storagetest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/k8sleaderelectortest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/adapter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscloudwatchreceiver/internal/metadata"
)

func TestLogsLeaderElectionHandoff(t *testing.T) {
	store := &leaderElectionStorage{Extension: storagetest.NewFileBackedStorageExtension("shared", t.TempDir())}
	sink := &consumertest.LogsSink{}
	first, firstClient, firstElector, _ := newLeaderElectionReceiver(t, store, sink)
	second, secondClient, secondElector, _ := newLeaderElectionReceiver(t, store, sink)

	require.Never(t, func() bool { return firstClient.calls.Load()+secondClient.calls.Load() > 0 }, 50*time.Millisecond, time.Millisecond)
	require.Zero(t, store.opened.Load())

	firstElector.InvokeOnLeading()
	require.Eventually(t, func() bool { return firstClient.calls.Load() >= 2 && sink.LogRecordCount() == 1 }, time.Second, time.Millisecond)
	require.Zero(t, secondClient.calls.Load())
	firstElector.InvokeOnStopping()
	firstCalls := firstClient.calls.Load()
	require.EqualValues(t, 1, store.closed.Load())

	secondElector.InvokeOnLeading()
	require.Eventually(t, func() bool { return secondClient.calls.Load() > 0 }, time.Second, time.Millisecond)
	require.Greater(t, secondClient.lastStart.Load(), secondClient.eventTime)
	secondElector.InvokeOnStopping()
	require.Equal(t, firstCalls, firstClient.calls.Load())
	require.Equal(t, 1, sink.LogRecordCount())
	require.EqualValues(t, 2, store.closed.Load())

	firstElector.InvokeOnLeading()
	firstElector.InvokeOnLeading()
	require.Eventually(t, func() bool { return firstClient.calls.Load() > firstCalls }, time.Second, time.Millisecond)
	require.EqualValues(t, 3, store.opened.Load())
	require.NoError(t, first.Shutdown(t.Context()))
	firstElector.InvokeOnLeading()
	firstElector.InvokeOnStopping()
	require.EqualValues(t, 3, store.opened.Load())
	require.EqualValues(t, 3, store.closed.Load())
	require.Equal(t, 1, sink.LogRecordCount())
	require.NoError(t, second.Shutdown(t.Context()))
}

func TestLogsLeaderLossCancelsRequest(t *testing.T) {
	store := &leaderElectionStorage{Extension: storagetest.NewFileBackedStorageExtension("shared", t.TempDir())}
	r, c, elector, host := newLeaderElectionReceiver(t, store, &consumertest.LogsSink{})
	c.started = make(chan struct{})
	c.canceled = make(chan struct{})

	persister := openCheckpointPersister(t, r, host)
	checkpoint := time.UnixMilli(c.eventTime).Format(time.RFC3339)
	require.NoError(t, persister.SetCheckpoint(t.Context(), testLogGroupName, checkpoint))
	require.NoError(t, persister.Shutdown(t.Context()))

	elector.OnLeading(t.Context())
	select {
	case <-c.started:
	case <-time.After(time.Second):
		t.Fatal("log request did not start")
	}
	stopped := make(chan struct{})
	go func() {
		elector.InvokeOnStopping()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("leadership loss did not cancel the log request")
	}
	require.EqualValues(t, 1, c.calls.Load())
	select {
	case <-c.canceled:
	default:
		t.Fatal("log request was not canceled")
	}

	persister = openCheckpointPersister(t, r, host)
	actual, err := persister.GetCheckpoint(t.Context(), testLogGroupName)
	require.NoError(t, err)
	require.Equal(t, checkpoint, actual)
	require.NoError(t, persister.Shutdown(t.Context()))
}

func TestLogsLeaderElectionDuringShutdown(t *testing.T) {
	store := &leaderElectionStorage{Extension: storagetest.NewInMemoryStorageExtension("shared")}
	r, _, elector, _ := newLeaderElectionReceiver(t, store, &consumertest.LogsSink{})
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for range 20 {
		wg.Go(elector.InvokeOnLeading)
		wg.Go(elector.InvokeOnStopping)
		wg.Go(func() { errs <- r.Shutdown(t.Context()) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	elector.InvokeOnLeading()
	require.Equal(t, store.opened.Load(), store.closed.Load())
}

func TestLogsLeaderElectionInvalidExtension(t *testing.T) {
	tests := []struct {
		name string
		ext  component.Component
		want string
	}{
		{name: "missing", want: "unknown k8s leader elector"},
		{name: "wrong type", ext: storagetest.NewNonStorageExtension("invalid"), want: "does not implement k8sleaderelector.LeaderElection"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			id := component.MustNewIDWithName("k8s_leader_elector", "cloudwatch")
			cfg.Logs.K8sLeaderElector = &id
			r := newLogsReceiver(cfg, receivertest.NewNopSettings(metadata.Type), &consumertest.LogsSink{})
			host := storagetest.NewStorageHost()
			if tt.ext != nil {
				host.WithExtension(id, tt.ext)
			}
			require.ErrorContains(t, r.Start(t.Context(), host), tt.want)
			require.NoError(t, r.Shutdown(t.Context()))
		})
	}
}

func TestLogsLeaderElectionInvalidStorage(t *testing.T) {
	tests := []struct {
		name string
		ext  component.Component
		want string
	}{
		{name: "missing", want: "storage extension 'file_storage/cloudwatch' not found"},
		{name: "wrong type", ext: storagetest.NewNonStorageExtension("invalid"), want: "non-storage extension 'file_storage/cloudwatch' found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			leaderID := component.MustNewIDWithName("k8s_leader_elector", "cloudwatch")
			storageID := component.MustNewIDWithName("file_storage", "cloudwatch")
			cfg.Logs.K8sLeaderElector = &leaderID
			cfg.StorageID = &storageID
			host := storagetest.NewStorageHost().WithExtension(leaderID, &k8sleaderelectortest.FakeLeaderElection{})
			if tt.ext != nil {
				host.WithExtension(storageID, tt.ext)
			}
			r := newLogsReceiver(cfg, receivertest.NewNopSettings(metadata.Type), &consumertest.LogsSink{})
			require.ErrorContains(t, r.Start(t.Context(), host), tt.want)
			require.NoError(t, r.Shutdown(t.Context()))
		})
	}
}

func TestLogsLeaderElectionStartFailureIsLogged(t *testing.T) {
	store := &leaderElectionStorage{Extension: storagetest.NewInMemoryStorageExtension("shared")}
	store.fail.Store(true)
	r, c, elector, _ := newLeaderElectionReceiver(t, store, &consumertest.LogsSink{})
	core, logs := observer.New(zap.ErrorLevel)
	r.settings.Logger = zap.New(core)

	elector.InvokeOnLeading()
	entries := logs.FilterMessage("failed to start log collection as leader").All()
	require.Len(t, entries, 1)
	require.Equal(t, "storage unavailable", entries[0].ContextMap()["error"])
	require.Zero(t, c.calls.Load())
	require.Zero(t, store.opened.Load())

	// The next leadership term retries once storage is usable again.
	store.fail.Store(false)
	elector.InvokeOnLeading()
	require.Eventually(t, func() bool { return c.calls.Load() > 0 }, time.Second, time.Millisecond)
}

func TestCanceledLogPollDoesNotCreateSession(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	r := newLogsReceiver(cfg, receivertest.NewNopSettings(metadata.Type), &consumertest.LogsSink{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	start := time.Now().Add(-time.Minute)
	next, err := r.pollForLogs(ctx, &streamNames{group: testLogGroupName}, start, time.Now())
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, start, next)
	require.Nil(t, r.client)
}

func newLeaderElectionReceiver(t *testing.T, store *leaderElectionStorage, sink *consumertest.LogsSink) (*logsReceiver, *leaderElectionLogsClient, *k8sleaderelectortest.FakeLeaderElection, component.Host) {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.Region = "us-west-1"
	cfg.Logs.PollInterval = 10 * time.Millisecond
	cfg.Logs.StartFrom = "2025-01-01T00:00:00Z"
	cfg.Logs.Groups = GroupConfig{NamedConfigs: map[string]StreamConfig{testLogGroupName: {}}}
	leaderID := component.MustNewIDWithName("k8s_leader_elector", "cloudwatch")
	storageID := component.MustNewIDWithName("file_storage", "shared")
	cfg.Logs.K8sLeaderElector = &leaderID
	cfg.StorageID = &storageID
	c := &leaderElectionLogsClient{eventTime: time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC).UnixMilli()}
	set := receivertest.NewNopSettings(metadata.Type)
	set.ID = component.NewIDWithName(metadata.Type, "cloudwatch")
	r := newLogsReceiver(cfg, set, sink)
	r.client = c
	elector := &k8sleaderelectortest.FakeLeaderElection{}
	host := storagetest.NewStorageHost().WithExtension(storageID, store).WithExtension(leaderID, elector)
	require.NoError(t, r.Start(t.Context(), host))
	t.Cleanup(func() { require.NoError(t, r.Shutdown(context.WithoutCancel(t.Context()))) })
	return r, c, elector, host
}

func openCheckpointPersister(t *testing.T, r *logsReceiver, host component.Host) *cloudwatchCheckpointPersister {
	t.Helper()
	storageClient, err := adapter.GetStorageClient(t.Context(), host, r.storageID, r.settings.ID)
	require.NoError(t, err)
	return newCloudwatchCheckpointPersister(storageClient, zap.NewNop())
}

type leaderElectionLogsClient struct {
	client
	calls     atomic.Int32
	lastStart atomic.Int64
	eventTime int64
	started   chan struct{}
	canceled  chan struct{}
}

func (c *leaderElectionLogsClient) FilterLogEvents(ctx context.Context, input *cloudwatchlogs.FilterLogEventsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.FilterLogEventsOutput, error) {
	c.lastStart.Store(*input.StartTime)
	c.calls.Add(1)
	if c.started != nil {
		close(c.started)
		<-ctx.Done()
		close(c.canceled)
		return nil, ctx.Err()
	}
	output := &cloudwatchlogs.FilterLogEventsOutput{}
	if *input.StartTime <= c.eventTime && c.eventTime < *input.EndTime {
		output.Events = []types.FilteredLogEvent{{
			EventId: aws.String("event"), Timestamp: aws.Int64(c.eventTime),
			LogStreamName: aws.String("stream"), Message: aws.String("message"),
		}}
	}
	return output, nil
}

type leaderElectionStorage struct {
	storage.Extension
	opened atomic.Int32
	closed atomic.Int32
	fail   atomic.Bool
}

func (s *leaderElectionStorage) GetClient(ctx context.Context, kind component.Kind, id component.ID, name string) (storage.Client, error) {
	if s.fail.Load() {
		return nil, errors.New("storage unavailable")
	}
	c, err := s.Extension.GetClient(ctx, kind, id, name)
	if err != nil {
		return nil, err
	}
	s.opened.Add(1)
	return &leaderElectionStorageClient{Client: c, closed: &s.closed}, nil
}

type leaderElectionStorageClient struct {
	storage.Client
	closed *atomic.Int32
}

func (c *leaderElectionStorageClient) Close(ctx context.Context) error {
	c.closed.Add(1)
	return c.Client.Close(ctx)
}
