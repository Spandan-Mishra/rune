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

package browserrpc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/unstablebuild/rune-go-sdk/api/browserapi"
	"github.com/unstablebuild/rune-go-sdk/api/browserapi/browserrpc"
	"github.com/unstablebuild/rune-go-sdk/api/workspaceapi"
	"github.com/unstablebuild/rune-go-sdk/term"
	"github.com/unstablebuild/rune-go-sdk/term/termrpc"
	gomock "go.uber.org/mock/gomock"
	"unstable.build/rune/internal/browser/browsertest"
)

const asyncResultsSleepDuration = 300 * time.Millisecond

func newServerWithNoBroker(ctrl *gomock.Controller) (
	*Server, *browsertest.MockBrowser,
) {
	mock := browsertest.NewMockBrowser(ctrl)
	s := NewServer(mock, new(sync.Mutex))
	s.SetSyncMode()
	return s, mock
}

func newTestServer(ctrl *gomock.Controller, mu *sync.Mutex) (
	*Server, *browsertest.MockBrowser,
) {
	mockBrowser := browsertest.NewMockBrowser(ctrl)
	s := NewServer(mockBrowser, mu)
	s.SetSyncMode()

	return s, mockBrowser
}

func TestServerNotify(t *testing.T) {
	ctx := context.Background()

	t.Run("delegates Notify to underlying Browser", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		s, mock := newServerWithNoBroker(ctrl)

		mock.EXPECT().
			Notify(gomock.Eq(browserapi.LevelSuccess), gomock.Eq("%s"), gomock.Eq("blah")).
			Return("1234", nil)

		req := browserrpc.NotifyRequest{Level: uint32(browserapi.LevelSuccess), Msg: "blah"}
		res, err := s.Notify(ctx, &req)
		require.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, "1234", res.GetId())
	})

	t.Run("bubbles up Notify Browser error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		s, mock := newServerWithNoBroker(ctrl)

		mock.EXPECT().Notify(gomock.Any(), gomock.Any(), gomock.Any()).Return("", errors.New("oopsie daisy"))

		_, err := s.Notify(ctx, new(browserrpc.NotifyRequest))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "oopsie")
	})
}

func TestServerOpen(t *testing.T) {
	ctx := context.Background()

	t.Run("delegates Open to underlying Browser", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		s, mock := newTestServer(ctrl, new(sync.Mutex))
		uri, err := workspaceapi.ParseURI("file:///tmp/coronavirus.sql")
		require.NoError(t, err)

		h := browsertest.NewTestHandler()
		mock.EXPECT().Open(gomock.Eq(uri)).Return(h, nil)

		req := browserrpc.OpenResourceRequest{Resource: "file:///tmp/coronavirus.sql"}
		res, err := s.Open(ctx, &req)
		require.NoError(t, err)
		assert.NotNil(t, res)
	})

	t.Run("bubbles up Open Browser error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		s, mock := newTestServer(ctrl, new(sync.Mutex))

		mock.EXPECT().Open(gomock.Any()).Return(nil, errors.New("oopsie daisy"))

		req := browserrpc.OpenResourceRequest{Resource: "file:///a"}
		_, err := s.Open(ctx, &req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "oopsie")
	})
}

func TestServerPublish(t *testing.T) {
	ctx := context.Background()

	t.Run("handles interrupt event by calling interrupt handler", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		var mu sync.Mutex
		s, mock := newTestServer(ctrl, &mu)
		req := browserrpc.PublishRequest{Ev: &termrpc.Event{Type: termrpc.Event_TypeInterrupt}}

		mock.EXPECT().PublishEvent(gomock.Eq(term.Event{Type: term.EventInterrupt})).Times(1)

		res, err := s.Publish(ctx, &req)
		require.NoError(t, err)
		assert.NotNil(t, res)
	})

	t.Run("handles EventNone event by calling interrupt handler", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		var mu sync.Mutex
		s, mock := newTestServer(ctrl, &mu)
		req := browserrpc.PublishRequest{Ev: &termrpc.Event{Type: termrpc.Event_TypeNone}}

		mock.EXPECT().PublishEvent(gomock.Eq(term.Event{Type: term.EventNone})).Times(1)

		res, err := s.Publish(ctx, &req)
		require.NoError(t, err)
		assert.NotNil(t, res)
	})

	t.Run("handles interrupt handler error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		var mu sync.Mutex
		s, mock := newTestServer(ctrl, &mu)
		req := browserrpc.PublishRequest{Ev: &termrpc.Event{Type: termrpc.Event_TypeInterrupt}}

		mock.EXPECT().PublishEvent(gomock.Any()).Return(errors.New("uRock"))

		res, err := s.Publish(ctx, &req)
		require.Error(t, err)
		assert.Nil(t, res)
	})

	t.Run("handles event none handler error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		var mu sync.Mutex
		s, mock := newTestServer(ctrl, &mu)
		req := browserrpc.PublishRequest{Ev: &termrpc.Event{Type: termrpc.Event_TypeNone}}

		mock.EXPECT().PublishEvent(gomock.Any()).Return(errors.New("uRock"))

		res, err := s.Publish(ctx, &req)
		require.Error(t, err)
		assert.Nil(t, res)
	})
}

func TestServerSetContent(t *testing.T) {
	/* tested via ex integration tests */
}

// resizeRecorder stands in for the handler on the far end of a tab's
// stream, recording the dimensions the server forwards to it.
type resizeRecorder struct {
	*browsertest.TestHandler
	mu      sync.Mutex
	resizes [][2]int
}

func (r *resizeRecorder) Resize(width, height int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resizes = append(r.resizes, [2]int{width, height})
}

func (r *resizeRecorder) last() [2]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.resizes) == 0 {
		return [2]int{}
	}
	return r.resizes[len(r.resizes)-1]
}

// resizeOnUnlockLocker runs fn as the browser lock is released. Resizes
// reach a streamHandler under that lock, so this places one exactly where
// another goroutine's would land while the tab's stream finishes setting up.
type resizeOnUnlockLocker struct {
	sync.Mutex
	once sync.Once
	fn   func()
}

func (l *resizeOnUnlockLocker) Unlock() {
	l.once.Do(func() {
		if l.fn != nil {
			l.fn()
		}
	})
	l.Mutex.Unlock()
}

// TestStreamHandlerResizeDuringStreamSetup covers a tab whose window sizes
// it while the stream serving that tab is still being set up. The size has
// to reach the far end either way, or the tab renders blank.
func TestStreamHandlerResizeDuringStreamSetup(t *testing.T) {
	rec := &resizeRecorder{TestHandler: browsertest.NewTestHandler()}
	lock := new(resizeOnUnlockLocker)
	h := &streamHandler{Handler: rec, mu: lock}
	lock.fn = func() { h.Resize(20, 6) }

	h.doneSetup()

	assert.Equal(t, [2]int{20, 6}, rec.last(),
		"the window's size must reach the handler behind the stream")
}
