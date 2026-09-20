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

// Package idehistory consolidates all persisted workspace session state
// (open files, file→window mapping, tile layout, open terminal sessions,
// open task sessions) behind a single Store.
package idehistory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/unstablebuild/blue/logging"
	"github.com/unstablebuild/rune-go-sdk/api/storageapi"
	"github.com/unstablebuild/rune-go-sdk/api/textapi"
	"github.com/unstablebuild/rune-go-sdk/api/workspaceapi"
	"github.com/unstablebuild/rune-go-sdk/component"
	"github.com/unstablebuild/rune-go-sdk/term"
	tcomponent "unstable.build/rune/internal/component"
	"unstable.build/rune/internal/term/vte"
	"unstable.build/rune/internal/text"
)

const (
	workspaceStateDocumentKind   = "workspace-state"
	workspaceStateDocumentPrefix = "workspace-state:"

	lastSessionDocumentKind = "last-session"
	lastSessionDocumentID   = "last-session"
)

// State is the unified workspace state persisted by Store.
type State struct {
	Name      string
	Files     []File
	Layout    tcomponent.TileLayout
	HasLayout bool
	Terminals []TerminalSession
	Tasks     []TaskSession
}

// IsEmpty reports whether there's nothing worth restoring. A persisted
// layout alone is not considered worth restoring: the IDE always saves
// a layout on workspace close, so observing a layout with no
// files/terminals/tasks is the common "empty workspace" case and
// triggering restore on it produces an empty-but-visible window.
//
// The workspace name is likewise not content: it is applied whenever
// the workspace opens, without going through restore.
func (s State) IsEmpty() bool {
	return len(s.Files) == 0 &&
		len(s.Terminals) == 0 &&
		len(s.Tasks) == 0
}

// File describes a file that was open in the previous session.
type File struct {
	URI      workspaceapi.URI
	Dirty    bool
	OpenAt   time.Time
	Cursor   term.Coordinates
	WindowID uint64
}

// TerminalSession is a serialized open-terminal session.
type TerminalSession struct {
	Name     string
	Snapshot vte.Snapshot
	Tab      bool
	Visible  bool
	Focus    bool
	WindowID uint64
}

// TaskSession is a serialized open-task session.
type TaskSession struct {
	Name                     string
	TaskName                 string
	Filter                   string
	Cmd                      string
	Args                     []string
	MinimizeAlignment        component.Alignment
	WindowID                 uint64
	WindowMinimized          bool
	WindowMinimizedAlignment component.Alignment
}

// Snapshotter supplies live workspace state when the Store needs to
// persist a fresh snapshot at close/reload time.
type Snapshotter interface {
	// Name is the workspace's display name, empty when it has not
	// been renamed.
	Name() string
	Terminals() []TerminalSession
	Tasks() []TaskSession
	Layout() (tcomponent.TileLayout, bool)
	FileWindowIDs() map[string]uint64
}

// SessionWorkspace is a workspace that was installed in a given slot
// when the session snapshot was taken.
type SessionWorkspace struct {
	URI  workspaceapi.URI
	Slot int
}

// Session records which workspaces were open at a point in time, so a
// later run of the IDE can offer to reopen them.
type Session struct {
	Workspaces []SessionWorkspace
	FocusSlot  int
	SavedAt    time.Time
}

// Store persists workspace state directly through a storageapi.Service.
// Calls are synchronous; the caller is responsible for not invoking
// Store from contexts that cannot tolerate I/O latency.
//
// Store is not safe for concurrent use. It expects to be called from
// a single goroutine (typically the IDE event loop).
type Store struct {
	storage  storageapi.Service
	trackers map[string]*tracker
}

// New constructs a Store backed by storage. The Store does NOT take
// ownership of storage and never calls Close on it.
func New(storage storageapi.Service) *Store {
	return &Store{
		storage:  storage,
		trackers: make(map[string]*tracker),
	}
}

// StoreWorkspaceState writes state for uri.
func (s *Store) StoreWorkspaceState(
	ctx context.Context, uri workspaceapi.URI, state State,
) error {
	doc := newWorkspaceStateDocument(uri, state)
	if err := s.storage.Set(ctx, workspaceStateDocumentID(uri), doc); err != nil {
		return fmt.Errorf(
			"idehistory: store %q: %w", uri.String(), err)
	}
	return nil
}

// StoreWorkspaceStateForClose writes the workspace state at
// close/reload time. It seeds the state from the in-memory tracker
// (preserving file cursors and the most recent open/flush events) and
// enriches it with the live terminal/task/layout/window-ID snapshot
// produced by snap.
//
// When no tracker is registered for uri the written state still
// contains a fresh terminal/task/layout snapshot but an empty file
// list — callers that need the file list must therefore
// SubscribeEvents earlier.
func (s *Store) StoreWorkspaceStateForClose(
	ctx context.Context, uri workspaceapi.URI, snap Snapshotter,
) error {
	state, _ := s.trackerSnapshot(uri)
	if snap != nil {
		if winIDs := snap.FileWindowIDs(); len(winIDs) > 0 {
			for i, f := range state.Files {
				if wid, ok := winIDs[f.URI.String()]; ok {
					f.WindowID = wid
					state.Files[i] = f
				}
			}
		}
		layout, hasLayout := snap.Layout()
		state.Layout = layout
		state.HasLayout = hasLayout
		state.Terminals = snap.Terminals()
		state.Tasks = snap.Tasks()
		state.Name = snap.Name()
	}
	return s.StoreWorkspaceState(ctx, uri, state)
}

// PersistWorkspaceState rewrites uri's state from its registered
// tracker. Callers use it when something outside the editor's event
// stream changed persisted state and must not wait for the next
// editor event to make it durable. It is a no-op when uri has no
// tracker.
func (s *Store) PersistWorkspaceState(
	ctx context.Context, uri workspaceapi.URI,
) error {
	t, ok := s.trackers[uri.String()]
	if !ok {
		return nil
	}
	return s.StoreWorkspaceState(ctx, uri, t.buildState())
}

// ClearWorkspaceState deletes the persisted state for uri.
func (s *Store) ClearWorkspaceState(
	ctx context.Context, uri workspaceapi.URI,
) error {
	id := workspaceStateDocumentID(uri)
	if err := s.storage.Delete(ctx, id); err != nil &&
		!errors.Is(err, storageapi.ErrNotFound) {
		return fmt.Errorf("idehistory: clear %q: %w", uri.String(), err)
	}
	return nil
}

// LoadWorkspaceState returns the persisted state for uri. If no state
// is persisted yet, the zero State is returned.
func (s *Store) LoadWorkspaceState(
	ctx context.Context, uri workspaceapi.URI,
) (State, error) {
	var doc workspaceStateDocument
	err := s.storage.Get(ctx, workspaceStateDocumentID(uri), &doc)
	if errors.Is(err, storageapi.ErrNotFound) {
		return State{}, nil
	}
	if err != nil {
		return State{}, fmt.Errorf(
			"idehistory: load %q: %w", uri.String(), err)
	}
	return doc.toState(), nil
}

// StoreLastSession records which workspaces are currently open so a
// later run can offer to reopen them.
func (s *Store) StoreLastSession(ctx context.Context, session Session) error {
	doc := lastSessionDocument{
		Kind:      lastSessionDocumentKind,
		FocusSlot: session.FocusSlot,
		SavedAt:   session.SavedAt,
	}
	for _, w := range session.Workspaces {
		doc.Workspaces = append(doc.Workspaces, sessionWorkspaceDoc{
			URI:  w.URI.String(),
			Slot: w.Slot,
		})
	}
	if err := s.storage.Set(ctx, lastSessionDocumentID, doc); err != nil {
		return fmt.Errorf("idehistory: store last session: %w", err)
	}
	return nil
}

// LoadLastSession returns the workspaces recorded by the most recent
// StoreLastSession call. If nothing was ever persisted, the zero
// Session is returned.
func (s *Store) LoadLastSession(ctx context.Context) (Session, error) {
	var doc lastSessionDocument
	err := s.storage.Get(ctx, lastSessionDocumentID, &doc)
	if errors.Is(err, storageapi.ErrNotFound) {
		return Session{}, nil
	}
	if err != nil {
		return Session{}, fmt.Errorf("idehistory: load last session: %w", err)
	}
	session := Session{FocusSlot: doc.FocusSlot, SavedAt: doc.SavedAt}
	for _, w := range doc.Workspaces {
		uri, err := workspaceapi.ParseURI(w.URI)
		if err != nil {
			continue
		}
		session.Workspaces = append(session.Workspaces, SessionWorkspace{
			URI:  uri,
			Slot: w.Slot,
		})
	}
	return session, nil
}

// ListWorkspaceURIs returns the URI of every workspace that has
// persisted state.
func (s *Store) ListWorkspaceURIs(
	ctx context.Context,
) ([]workspaceapi.URI, error) {
	it, err := s.storage.List(ctx, []storageapi.Filter{{
		Field: storageapi.Field{
			FieldPath: []string{"Kind"},
			Value:     workspaceStateDocumentKind,
		},
		Op: storageapi.OpEqual,
	}})
	if err != nil {
		return nil, fmt.Errorf("idehistory: list workspace states: %w", err)
	}
	defer it.Close()

	var uris []workspaceapi.URI
	for it.HasNext() {
		var doc workspaceStateDocument
		if err := it.NextTo(&doc); err != nil {
			return nil, fmt.Errorf("idehistory: list workspace states: %w", err)
		}
		if doc.WorkspaceURI == "" {
			continue
		}
		uri, err := workspaceapi.ParseURI(doc.WorkspaceURI)
		if err != nil {
			continue
		}
		uris = append(uris, uri)
	}
	return uris, nil
}

// SubscribeEvents subscribes to ed's events and maintains the
// in-memory File list + cursor map for uri. snap is invoked when the
// tracker needs terminal/task/layout context for the next Store call.
// The returned Closer unsubscribes.
//
// skip lists pseudo-buffer namespaces whose events must be ignored
// entirely — the file explorer, the :gitshow diff popup and anything
// else that must never enter the persisted file list. Entries match by
// path prefix, so a namespace covers resources minted under it.
func (s *Store) SubscribeEvents(
	ctx context.Context,
	uri workspaceapi.URI, ed text.Editor, snap Snapshotter,
	skip ...workspaceapi.URI,
) io.Closer {
	t := &tracker{
		store: s,
		uri:   uri,
		snap:  snap,
		ctx:   ctx,
		files: make(map[string]File),
		skip:  skip,
	}
	err := ed.SubscribeEvents([]textapi.EventType{
		textapi.EventTypeOpen,
		textapi.EventTypeClose,
		textapi.EventTypeFlush,
		textapi.EventTypeEdit,
		textapi.EventTypeCursor,
	}, t)
	if err != nil {
		log.WithFields(log.Fields{logging.KeyClass: "ide.idehistory"}).
			Errorf("subscribe editor events: %v", err)
	}
	t.ed = ed
	s.trackers[uri.String()] = t
	return t
}

// DirtyFilesOpen reports whether any file tracked by any active
// SubscribeEvents subscriber is currently dirty. The shutdown prompt
// uses this to ask the user before quitting with unsaved changes.
func (s *Store) DirtyFilesOpen() bool {
	for _, t := range s.trackers {
		for _, f := range t.files {
			if f.Dirty {
				return true
			}
		}
	}
	return false
}

type workspaceStateDocument struct {
	Kind         string
	WorkspaceURI string
	Name         string
	Files        []fileDoc
	Layout       tcomponent.TileLayout
	HasLayout    bool
	Terminals    []terminalDoc
	Tasks        []taskDoc
}

type fileDoc struct {
	URI      string
	Dirty    bool
	OpenAt   time.Time
	Cursor   term.Coordinates
	WindowID uint64
}

type terminalDoc struct {
	Name     string
	Snapshot vte.Snapshot
	Tab      bool
	Visible  bool
	Focus    bool
	WindowID uint64
}

type taskDoc struct {
	Name                     string
	TaskName                 string
	Filter                   string
	Cmd                      string
	Args                     []string
	MinimizeAlignment        component.Alignment
	WindowID                 uint64
	WindowMinimized          bool
	WindowMinimizedAlignment component.Alignment
}

type lastSessionDocument struct {
	Kind       string
	Workspaces []sessionWorkspaceDoc
	FocusSlot  int
	SavedAt    time.Time
}

type sessionWorkspaceDoc struct {
	URI  string
	Slot int
}

func workspaceStateDocumentID(uri workspaceapi.URI) string {
	return workspaceStateDocumentPrefix + url.QueryEscape(uri.String())
}

func newWorkspaceStateDocument(
	uri workspaceapi.URI, state State,
) workspaceStateDocument {
	doc := workspaceStateDocument{
		Kind:         workspaceStateDocumentKind,
		WorkspaceURI: uri.String(),
		Name:         state.Name,
		Layout:       state.Layout,
		HasLayout:    state.HasLayout,
	}
	for _, f := range state.Files {
		doc.Files = append(doc.Files, fileDoc{
			URI:      f.URI.String(),
			Dirty:    f.Dirty,
			OpenAt:   f.OpenAt,
			Cursor:   f.Cursor,
			WindowID: f.WindowID,
		})
	}
	for _, t := range state.Terminals {
		doc.Terminals = append(doc.Terminals, terminalDoc(t))
	}
	for _, t := range state.Tasks {
		doc.Tasks = append(doc.Tasks, taskDoc{
			Name:                     t.Name,
			TaskName:                 t.TaskName,
			Filter:                   t.Filter,
			Cmd:                      t.Cmd,
			Args:                     append([]string(nil), t.Args...),
			MinimizeAlignment:        t.MinimizeAlignment,
			WindowID:                 t.WindowID,
			WindowMinimized:          t.WindowMinimized,
			WindowMinimizedAlignment: t.WindowMinimizedAlignment,
		})
	}
	return doc
}

func (d workspaceStateDocument) toState() State {
	state := State{
		Name:      d.Name,
		Layout:    normalizeTileLayout(d.Layout),
		HasLayout: d.HasLayout,
	}
	for _, f := range d.Files {
		uri, err := workspaceapi.ParseURI(f.URI)
		if err != nil {
			continue
		}
		state.Files = append(state.Files, File{
			URI:      uri,
			Dirty:    f.Dirty,
			OpenAt:   f.OpenAt,
			Cursor:   f.Cursor,
			WindowID: f.WindowID,
		})
	}
	for _, t := range d.Terminals {
		state.Terminals = append(state.Terminals, TerminalSession(t))
	}
	for _, t := range d.Tasks {
		state.Tasks = append(state.Tasks, TaskSession{
			Name:                     t.Name,
			TaskName:                 t.TaskName,
			Filter:                   t.Filter,
			Cmd:                      t.Cmd,
			Args:                     append([]string(nil), t.Args...),
			MinimizeAlignment:        t.MinimizeAlignment,
			WindowID:                 t.WindowID,
			WindowMinimized:          t.WindowMinimized,
			WindowMinimizedAlignment: t.WindowMinimizedAlignment,
		})
	}
	return state
}

func normalizeTileLayout(layout tcomponent.TileLayout) tcomponent.TileLayout {
	if len(layout.Floating) == 0 {
		layout.Floating = nil
	}
	for i := range layout.Children {
		layout.Children[i] = normalizeTileLayout(layout.Children[i])
	}
	return layout
}
