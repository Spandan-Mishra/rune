// Copyright (C) 2017-2026 The Rune Authors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at
// your option) any later version.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
// General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package firstmover

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/unstablebuild/rune-go-sdk/api/storageapi"
	"github.com/unstablebuild/rune-go-sdk/api/storageapi/docmarshal/doctoml"
	"github.com/unstablebuild/rune-go-sdk/api/storageapi/storagestub"
	"github.com/unstablebuild/rune-go-sdk/retry"
)

func TestPubSub(t *testing.T) {
	t.Run("Close called more than once doesn't block or panic", func(t *testing.T) {
		svc, _ := makeLeaderFollowerPair(t, 0)
		assert.NotPanics(t, func() {
			require.NoError(t, svc.Close())
			require.NoError(t, svc.Close())
			require.NoError(t, svc.Close())
		})
	})

	t.Run("leader subscribe more than once returns an error", func(t *testing.T) {
		leader, followers := makeLeaderFollowerPair(t, 1)
		require.NoError(t, leader.Subscribe(context.Background(), "coffee"))
		require.Error(t, leader.Subscribe(context.Background(), "coffee"))
		cleanupNodes(t, append([]*Service{leader}, followers...)...)
	})

	t.Run("follower subscribe more than once returns an error", func(t *testing.T) {
		leader, followers := makeLeaderFollowerPair(t, 1)
		require.NoError(t, followers[0].Subscribe(context.Background(), "coffee"))
		require.Error(t, followers[0].Subscribe(context.Background(), "coffee"))
		cleanupNodes(t, append([]*Service{leader}, followers...)...)
	})

	t.Run("nodes receive ErrMessageTooLarge when publishing oversized messages", func(t *testing.T) {
		leader, followers := makeLeaderFollowerPair(t, 1)
		ctx := context.Background()
		err := leader.Publish(ctx, "1234", make([]byte, leader.cfg.MaxMessageSize*2))
		require.Equal(t, ErrMessageTooLarge, err)
		err = followers[0].Publish(ctx, "1234", make([]byte, leader.cfg.MaxMessageSize*2))
		require.Equal(t, ErrMessageTooLarge, err)
		cleanupNodes(t, append([]*Service{leader}, followers...)...)
	})

	t.Run("leader publishes and follower receives", func(t *testing.T) {
		leader, followers := makeLeaderFollowerPair(t, 1)
		publishAndReceive(t, leader, followers[0], 1)
		cleanupNodes(t, append([]*Service{leader}, followers...)...)
	})

	t.Run("follower publishes and leader receives", func(t *testing.T) {
		leader, followers := makeLeaderFollowerPair(t, 1)
		publishAndReceive(t, followers[0], leader, 1)
		cleanupNodes(t, append([]*Service{leader}, followers...)...)
	})

	t.Run("client re-connect", func(t *testing.T) {
		leader, followers := makeLeaderFollowerPair(t, 2)
		topic := "1234"
		ctx := context.Background()
		require.NoError(t, followers[0].Subscribe(ctx, topic))
		require.NoError(t, followers[1].Subscribe(ctx, topic))
		// Publish before Close: the publish is acked into every
		// subscriber's stream buffer, and the BYE handshake preserves
		// those buffers across the re-connect to the new leader.
		require.NoError(t, followers[1].Publish(ctx, topic, []byte("block")))
		require.NoError(t, leader.Close())
		data, err := followers[0].Receive(ctx, topic)
		require.NoError(t, err)
		assert.Equal(t, "block", string(data))
		cleanupNodes(t, followers...)
	})

	t.Run("post-failover publishes are eventually received", func(t *testing.T) {
		leader, followers := makeLeaderFollowerPair(t, 2)
		topic := "1234"
		require.NoError(t, followers[0].Subscribe(context.Background(), topic))
		require.NoError(t, followers[1].Subscribe(context.Background(), topic))
		require.NoError(t, leader.Close())

		// A publish can race the other follower's re-subscription on
		// the freshly elected leader and be dropped for that
		// subscriber: a new leader only learns about a remote
		// subscriber once it re-connects. Republish until observed;
		// pubsub is at-least-once so duplicates are fine.
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for {
				_ = followers[1].Publish(context.Background(), topic, []byte("block"))
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		data, err := followers[0].Receive(ctx, topic)
		close(stop)
		<-done
		require.NoError(t, err)
		assert.Equal(t, "block", string(data))
		cleanupNodes(t, followers...)
	})

	t.Run("receive continues after leader handoff", func(t *testing.T) {
		leader, followers := makeLeaderFollowerPair(t, 1)
		follower := followers[0]
		topic := "1234"
		ctx := context.Background()
		require.NoError(t, follower.Subscribe(ctx, topic))
		require.NoError(t, leader.Close())
		require.NoError(t, follower.Publish(ctx, topic, []byte("block")))
		receiveCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		data, err := follower.Receive(receiveCtx, topic)
		require.NoError(t, err)
		assert.Equal(t, "block", string(data))
		cleanupNodes(t, follower)
	})

	t.Run("subscriptions survive a leader that dies without a goodbye", func(t *testing.T) {
		leader, followers := makeLeaderFollowerPair(t, 1)
		follower := followers[0]
		topic := "1234"
		ctx := context.Background()
		require.NoError(t, follower.Subscribe(ctx, topic))

		// A crashed leader never sends the goodbye that carries the
		// subscription hand-over, so the follower learns of the
		// handoff from the dead connection alone.
		testHookSuppressBye.Store(true)
		defer testHookSuppressBye.Store(false)

		require.NoError(t, leader.Close())
		require.NoError(t, follower.Publish(ctx, topic, []byte("block")))
		receiveCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		data, err := follower.Receive(receiveCtx, topic)
		require.NoError(t, err)
		assert.Equal(t, "block", string(data))
		cleanupNodes(t, follower)
	})

	t.Run("silent leader death is detected without client traffic", func(t *testing.T) {
		leader, followers := makeLeaderFollowerPair(t, 1)
		follower := followers[0]
		topic := "1234"
		ctx := context.Background()
		require.NoError(t, follower.Subscribe(ctx, topic))

		testHookSuppressBye.Store(true)
		defer testHookSuppressBye.Store(false)

		require.NoError(t, leader.Close())
		// The sibling test above publishes immediately, and that RPC is
		// itself what forces the dead connection to fail. Election must
		// not depend on a client happening to send something, or a node
		// that only waits for messages stays leaderless forever.
		require.Eventually(t, func() bool { return follower.IsLeader() },
			5*time.Second, 10*time.Millisecond,
			"follower must take leadership without client traffic")

		require.NoError(t, follower.Publish(ctx, topic, []byte("block")))
		receiveCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		data, err := follower.Receive(receiveCtx, topic)
		require.NoError(t, err)
		assert.Equal(t, "block", string(data))
		cleanupNodes(t, follower)
	})

	t.Run("publish while the new leader is still resubscribing is delivered", func(t *testing.T) {
		leader, followers := makeLeaderFollowerPair(t, 1)
		follower := followers[0]
		topic := "1234"
		ctx := context.Background()
		require.NoError(t, follower.Subscribe(ctx, topic))

		windowEntered := make(chan struct{})
		windowRelease := make(chan struct{})
		var once sync.Once
		hook := func() {
			once.Do(func() { close(windowEntered) })
			<-windowRelease
		}
		testHookLeadBeforeResubscribe.Store(&hook)
		defer testHookLeadBeforeResubscribe.Store(nil)

		require.NoError(t, leader.Close())
		<-windowEntered

		// The follower has won the election and unlocked the API but
		// has not restored its recovered subscriptions yet. A publish
		// issued now must not be dropped.
		pubDone := make(chan error, 1)
		go func() {
			pubDone <- follower.Publish(ctx, topic, []byte("block"))
		}()
		select {
		case err := <-pubDone:
			// Broken path: the publish "succeeded" inside the window,
			// which means it was broadcast to zero subscribers.
			require.NoError(t, err)
			pubDone = nil
		case <-time.After(500 * time.Millisecond):
			// Fixed path: the publish is parked until the recovered
			// subscriptions are re-established.
		}
		close(windowRelease)

		receiveCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		data, err := follower.Receive(receiveCtx, topic)
		require.NoError(t, err)
		assert.Equal(t, "block", string(data))
		if pubDone != nil {
			require.NoError(t, <-pubDone)
		}
		cleanupNodes(t, follower)
	})

	t.Run("concurrent subscribes to one topic share a single server stream", func(t *testing.T) {
		leader, followers := makeLeaderFollowerPair(t, 1)
		follower := followers[0]
		defer cleanupNodes(t, leader, follower)
		topic := "1234"
		ctx := context.Background()
		require.NoError(t, follower.waitReady(ctx))

		// A Receive racing the post-failover resubscribe used to open
		// a second server stream for the same topic. Whichever one
		// registered after a publish never saw the message, and the
		// caller blocked on it forever.
		const n = 16
		streams := make([]chan msgError, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		wg.Add(n)
		for i := range n {
			go func() {
				defer wg.Done()
				_, streams[i], errs[i] = follower.pubsub.subscribe(ctx, topic, false)
			}()
		}
		wg.Wait()
		for i := range n {
			require.NoError(t, errs[i], i)
			assert.Equal(t, streams[0], streams[i], "subscriber %d got a different stream", i)
		}
		leader.mu.Lock()
		subscribers := len(leader.pubsub.subscribers[topic])
		leader.mu.Unlock()
		assert.Equal(t, 1, subscribers, "server registered more than one stream for the topic")

		require.NoError(t, leader.Publish(ctx, topic, []byte("block")))
		data, err := follower.Receive(ctx, topic)
		require.NoError(t, err)
		assert.Equal(t, "block", string(data))
	})

	t.Run("a stream that failed while the leader was lost is not replayed as a message", func(t *testing.T) {
		leader, followers := makeLeaderFollowerPair(t, 1)
		follower := followers[0]
		topic := "1234"
		ctx := context.Background()
		require.NoError(t, follower.Subscribe(ctx, topic))

		// The stream pump reports the broken connection into the
		// buffered stream. recoverSubscriptions must not carry that
		// error entry over as an empty message.
		testHookSuppressBye.Store(true)
		defer testHookSuppressBye.Store(false)
		require.NoError(t, leader.Close())
		require.Eventually(t, func() bool { return follower.IsLeader() },
			5*time.Second, 10*time.Millisecond)

		require.NoError(t, follower.Publish(ctx, topic, []byte("block")))
		receiveCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		data, err := follower.Receive(receiveCtx, topic)
		require.NoError(t, err)
		assert.Equal(t, "block", string(data))
		cleanupNodes(t, follower)
	})

	t.Run("extreme concurrency of leaders and followers", func(t *testing.T) {
		const n, m = 40, 20
		cfg := testConfig()
		lockFile := makeTempLockFile(t)
		backend := storagestub.NewInMemoryService()
		instances := make([]*Service, 0, n)
		for i := 0; i < n-1; i++ {
			instance := New(factoryFor(backend), lockFile, cfg)
			instance.retryStrategy = retry.CombinedStrategy(
				retry.SequentialStrategy(2*time.Millisecond),
				retry.LimitStrategy(m*2),
			)
			_ = instance.Get(context.Background(), lockFile, nil)
			instances = append(instances, instance)
		}
		instance1 := instances[len(instances)-1]
		instance2 := instances[len(instances)-2]

		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			ticker := time.NewTicker(cfg.DialTimeout + cfg.ConnectRetryCadence)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
				for _, instance := range instances {
					if instance.IsLeader() && instance != instance1 && instance != instance2 {
						_ = instance.Close()
						break
					}
				}
			}
		}()
		defer func() {
			close(stop)
			<-done
			cleanupNodes(t, instances...)
		}()
		publishAndReceive(t, instance1, instance2, m)
	})
}

func cleanupNodes(t *testing.T, nodes ...*Service) {
	t.Helper()
	for _, node := range nodes {
		if node != nil {
			assert.NoError(t, node.Close())
		}
	}
}

func makeLeaderFollowerPair(t *testing.T, nfollowers int) (*Service, []*Service) {
	return makeLeaderFollowerPairLockFileListen(t, nfollowers, "")
}

func makeLeaderFollowerPairLockFileListen(
	t *testing.T, nfollowers int, lockFileListen string,
) (*Service, []*Service) {
	lockFile := makeTempLockFile(t)
	cfg := testConfig()
	cfg.Marshaler = doctoml.Marshaler()
	svc := storagestub.NewInMemoryServiceWithMarshaler(cfg.Marshaler)
	leader := New(factoryFor(svc), lockFile, cfg)
	var temp testStruct
	err := leader.Get(context.Background(), lockFile, &temp)
	require.Equal(t, storageapi.ErrNotFound, err)
	var followers []*Service
	for i := 0; i < nfollowers; i++ {
		follower := new(Service)
		follower.lockFileListen = lockFileListen
		follower.Init(factoryFor(svc), lockFile, cfg)
		followers = append(followers, follower)
	}
	return leader, followers
}

func publishAndReceive(t *testing.T, sender, receiver *Service, n int) {
	ctx := context.Background()
	var done, ready sync.WaitGroup
	actualMsg := make([][]byte, n)
	actualErr := make([]error, n)
	done.Add(n)
	ready.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer done.Done()
			actualErr[i] = receiver.Subscribe(ctx, strconv.Itoa(i))
			ready.Done()
			if actualErr[i] != nil {
				return
			}
			actualMsg[i], actualErr[i] = receiver.Receive(ctx, strconv.Itoa(i))
		}(i)
	}
	ready.Wait()
	for i := 0; i < n; i++ {
		err := sender.Publish(ctx, strconv.Itoa(i), []byte(strconv.Itoa(i)))
		require.NoError(t, err)
	}
	done.Wait()
	for i := 0; i < n; i++ {
		require.NoError(t, actualErr[i], i)
		assert.Equal(t, strconv.Itoa(i), string(actualMsg[i]), i)
	}
}

func TestPubSubPublishMatrix(t *testing.T) {
	suite := []struct {
		indexPub int
		n        int
	}{
		{0, 2}, {1, 2}, {0, 3}, {1, 3}, {2, 3},
	}
	for _, tc := range suite {
		desc := fmt.Sprintf("%d node is able to publish to the rest of nodes (n=%d)", tc.indexPub, tc.n)
		t.Run(desc, func(t *testing.T) {
			leader, followers := makeLeaderFollowerPair(t, tc.n)
			nodes := append(followers, leader)
			ctx := context.Background()
			topic := "1234"
			publisher := nodes[tc.indexPub]
			rest := make([]*Service, 0, len(nodes)-1)
			rest = append(rest, nodes[:tc.indexPub]...)
			rest = append(rest, nodes[tc.indexPub+1:]...)
			for _, node := range rest {
				require.NoError(t, node.Subscribe(ctx, topic))
			}
			require.NoError(t, publisher.Publish(ctx, topic, []byte("block")))
			for _, node := range rest {
				data, err := node.Receive(ctx, topic)
				require.NoError(t, err)
				assert.Equal(t, "block", string(data))
			}
			cleanupNodes(t, nodes...)
		})
	}
}
