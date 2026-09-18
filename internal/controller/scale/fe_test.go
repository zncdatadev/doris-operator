/*
Copyright 2026 zncdatadev.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scale

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/zncdatadev/doris-operator/internal/controller/constants"
	"github.com/zncdatadev/doris-operator/internal/controller/doris_client"
)

var errTestDropObserver = errors.New("drop observer failed")
var errTestShowFrontends = errors.New("show frontends failed")

type fakeFrontendScaleClient struct {
	frontends     []doris_client.FrontendInfo
	showErr       error
	dropErr       error
	dropErrByHost map[string]error
	drops         []string
	showCalls     int
	events        *[]string
}

func (c *fakeFrontendScaleClient) ShowFrontends(context.Context) ([]doris_client.FrontendInfo, error) {
	c.showCalls++
	return c.frontends, c.showErr
}

func (c *fakeFrontendScaleClient) DropObserver(_ context.Context, host string, port int) error {
	c.drops = append(c.drops, host)
	if c.events != nil {
		*c.events = append(*c.events, "drop:"+host)
	}
	if err := c.dropErrByHost[host]; err != nil {
		return err
	}
	return c.dropErr
}

type fakeFrontendDropTracker struct {
	phases     map[string]string
	persistErr error
	persists   int
	events     *[]string
}

func (t *fakeFrontendDropTracker) RecordFrontendDropIntent(podName string) {
	if _, exists := t.phases[podName]; !exists {
		t.phases[podName] = frontendDropPhasePending
	}
}

func (t *fakeFrontendDropTracker) MarkFrontendRemoved(podName string) {
	t.phases[podName] = frontendDropPhaseRemoved
}

func (t *fakeFrontendDropTracker) Persist(context.Context) error {
	t.persists++
	if t.events != nil {
		*t.events = append(*t.events, "persist")
	}
	return t.persistErr
}

func TestFEScaleManagerKeepsIntentUntilStatefulSetConverges(t *testing.T) {
	const podName = "doris-fe-observer-2"
	action := ScaleAction{
		Component:       constants.ComponentTypeFE,
		CurrentReplicas: 3,
		DesiredReplicas: 2,
		PodsToRemove:    []string{podName},
		Strategy:        StrategyDropObserver,
	}
	tests := []struct {
		name      string
		frontends []doris_client.FrontendInfo
		dropErr   error
		wantReady []string
		wantPhase string
		wantDrops []string
	}{
		{
			name:      "node already absent remains latched as removed",
			wantReady: []string{podName},
			wantPhase: frontendDropPhaseRemoved,
		},
		{
			name: "successful drop remains latched as removed",
			frontends: []doris_client.FrontendInfo{{
				Host:        podName + ".default.svc.cluster.local",
				EditLogPort: 9010,
				Role:        frontendRoleObserver,
			}},
			wantReady: []string{podName},
			wantPhase: frontendDropPhaseRemoved,
			wantDrops: []string{podName + ".default.svc.cluster.local"},
		},
		{
			name: "failed drop retains intent",
			frontends: []doris_client.FrontendInfo{{
				Host:        podName,
				EditLogPort: 9010,
				Role:        frontendRoleObserver,
			}},
			dropErr:   errTestDropObserver,
			wantPhase: frontendDropPhasePending,
			wantDrops: []string{podName},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeFrontendScaleClient{frontends: test.frontends, dropErr: test.dropErr}
			tracker := &fakeFrontendDropTracker{phases: make(map[string]string)}
			manager := &FEScaleManager{client: client}

			plans, err := manager.PrepareScaleDownActions(context.Background(), []ScaleAction{action})
			if err != nil {
				t.Fatalf("PrepareScaleDownActions() error = %v", err)
			}
			if err := manager.AuthorizeScaleDown(context.Background(), plans[0], tracker); err != nil {
				t.Fatalf("AuthorizeScaleDown() error = %v", err)
			}
			ready, err := manager.ExecuteScaleDown(context.Background(), plans[0], tracker)
			if test.dropErr != nil {
				if !errors.Is(err, test.dropErr) {
					t.Fatalf("ScaleDown() error = %v, want %v", err, test.dropErr)
				}
			} else if err != nil {
				t.Fatalf("ScaleDown() error = %v", err)
			}
			if !reflect.DeepEqual(ready, test.wantReady) {
				t.Errorf("ScaleDown() ready = %#v, want %#v", ready, test.wantReady)
			}
			if got := tracker.phases[podName]; got != test.wantPhase {
				t.Errorf("drop intent phase = %q, want %q", got, test.wantPhase)
			}
			if !reflect.DeepEqual(client.drops, test.wantDrops) {
				t.Errorf("DROP OBSERVER hosts = %#v, want %#v", client.drops, test.wantDrops)
			}
		})
	}
}

func TestFEScaleManagerPreflightsAllActionsBeforeAuthorization(t *testing.T) {
	actions := []ScaleAction{
		{
			Component:       constants.ComponentTypeFE,
			CurrentReplicas: 2,
			DesiredReplicas: 1,
			PodsToRemove:    []string{"doris-fe-first-1"},
			Strategy:        StrategyDropObserver,
		},
		{
			Component:       constants.ComponentTypeFE,
			CurrentReplicas: 2,
			DesiredReplicas: 1,
			PodsToRemove:    []string{"doris-fe-second-1"},
			Strategy:        StrategyDropObserver,
		},
	}
	client := &fakeFrontendScaleClient{frontends: []doris_client.FrontendInfo{
		{Host: "doris-fe-first-1", Role: frontendRoleObserver, EditLogPort: 9010},
		{Host: "doris-fe-second-1", Role: frontendRoleFollower, EditLogPort: 9010},
	}}
	manager := &FEScaleManager{client: client}

	if _, err := manager.PrepareScaleDownActions(context.Background(), actions); err == nil {
		t.Fatal("PrepareScaleDownActions() error = nil, want protected FOLLOWER rejection")
	}
	if len(client.drops) != 0 {
		t.Fatalf("DROP OBSERVER calls = %#v, want none before complete preflight", client.drops)
	}
}

func TestFEScaleManagerPreflightFailureCreatesNoIntent(t *testing.T) {
	baseAction := ScaleAction{
		Component:       constants.ComponentTypeFE,
		CurrentReplicas: 2,
		DesiredReplicas: 1,
		PodsToRemove:    []string{extensionTestFEPodOne},
		Strategy:        StrategyDropObserver,
	}
	tests := []struct {
		name          string
		action        ScaleAction
		client        *fakeFrontendScaleClient
		wantShowCalls int
	}{
		{
			name:          "show frontends failure",
			action:        baseAction,
			client:        &fakeFrontendScaleClient{showErr: errTestShowFrontends},
			wantShowCalls: 1,
		},
		{
			name:   "protected follower",
			action: baseAction,
			client: &fakeFrontendScaleClient{frontends: []doris_client.FrontendInfo{{
				Host: extensionTestFEPodOne, Role: frontendRoleFollower, EditLogPort: 9010,
			}}},
			wantShowCalls: 1,
		},
		{
			name:   "unexpected role",
			action: baseAction,
			client: &fakeFrontendScaleClient{frontends: []doris_client.FrontendInfo{{
				Host: "doris-fe-hot-1", Role: "UNKNOWN", EditLogPort: 9010,
			}}},
			wantShowCalls: 1,
		},
		{
			name: "unknown strategy",
			action: func() ScaleAction {
				action := baseAction
				action.Strategy = "invalid"
				return action
			}(),
			client: &fakeFrontendScaleClient{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tracker := &fakeFrontendDropTracker{phases: make(map[string]string)}
			manager := &FEScaleManager{client: test.client}
			plans, err := manager.PrepareScaleDownActions(context.Background(), []ScaleAction{test.action})
			if err == nil {
				t.Fatal("PrepareScaleDownActions() error = nil, want preflight failure")
			}
			if plans != nil {
				t.Fatalf("PrepareScaleDownActions() plans = %#v, want nil", plans)
			}
			if len(tracker.phases) != 0 || tracker.persists != 0 {
				t.Fatalf("tracker after preflight failure = phases %#v, persists %d; want untouched", tracker.phases, tracker.persists)
			}
			if test.client.showCalls != test.wantShowCalls {
				t.Errorf("ShowFrontends() calls = %d, want %d", test.client.showCalls, test.wantShowCalls)
			}
			if len(test.client.drops) != 0 {
				t.Fatalf("DROP OBSERVER calls = %#v, want none", test.client.drops)
			}
		})
	}
}

func TestFEScaleManagerPersistsAuthorizationBeforeDropAndRetainsPartialState(t *testing.T) {
	events := []string{}
	action := ScaleAction{
		Component:       constants.ComponentTypeFE,
		CurrentReplicas: 3,
		DesiredReplicas: 1,
		PodsToRemove:    []string{extensionTestFEPodOne, extensionTestFEPodTwo},
		Strategy:        StrategyDropObserver,
	}
	client := &fakeFrontendScaleClient{
		frontends: []doris_client.FrontendInfo{
			{Host: extensionTestFEPodOne, Role: frontendRoleObserver, EditLogPort: 9010},
			{Host: extensionTestFEPodTwo, Role: frontendRoleObserver, EditLogPort: 9010},
		},
		dropErrByHost: map[string]error{extensionTestFEPodTwo: errTestDropObserver},
		events:        &events,
	}
	tracker := &fakeFrontendDropTracker{phases: make(map[string]string), events: &events}
	manager := &FEScaleManager{client: client}

	plans, err := manager.PrepareScaleDownActions(context.Background(), []ScaleAction{action})
	if err != nil {
		t.Fatalf("PrepareScaleDownActions() error = %v", err)
	}
	if err := manager.AuthorizeScaleDown(context.Background(), plans[0], tracker); err != nil {
		t.Fatalf("AuthorizeScaleDown() error = %v", err)
	}
	if _, err := manager.ExecuteScaleDown(context.Background(), plans[0], tracker); !errors.Is(err, errTestDropObserver) {
		t.Fatalf("ExecuteScaleDown() error = %v, want %v", err, errTestDropObserver)
	}
	// ScaleExtension persists phase transitions even when a later DROP fails.
	if err := tracker.Persist(context.Background()); err != nil {
		t.Fatalf("Persist() partial FE state error = %v", err)
	}

	wantEvents := []string{"persist", "drop:" + extensionTestFEPodOne, "drop:" + extensionTestFEPodTwo, "persist"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Errorf("operation order = %#v, want %#v", events, wantEvents)
	}
	wantPhases := map[string]string{
		extensionTestFEPodOne: frontendDropPhaseRemoved,
		extensionTestFEPodTwo: frontendDropPhasePending,
	}
	if !reflect.DeepEqual(tracker.phases, wantPhases) {
		t.Errorf("phases after partial failure = %#v, want %#v", tracker.phases, wantPhases)
	}
}
