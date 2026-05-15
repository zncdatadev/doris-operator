/*
Copyright 2025 zncdatadev.

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

package doris_client

import "testing"

func TestEscapeSQLString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
		{
			name:  "no special characters",
			input: "root",
			want:  "root",
		},
		{
			name:  "single quote",
			input: "it's",
			want:  "it''s",
		},
		{
			name:  "multiple single quotes",
			input: "'hello'",
			want:  "''hello''",
		},
		{
			name:  "backslash",
			input: `path\to\file`,
			want:  `path\\to\\file`,
		},
		{
			name:  "mixed single quotes and backslashes",
			input: `it's a \path`,
			want:  `it''s a \\path`,
		},
		{
			name:  "consecutive single quotes",
			input: "''",
			want:  "''''",
		},
		{
			name:  "password with special chars",
			input: `P@ss'w0rd!\/`,
			want:  `P@ss''w0rd!\\/`,
		},
		{
			name:  "only backslashes",
			input: `\\`,
			want:  `\\\\`,
		},
		{
			name:  "doris admin password example",
			input: "MyP@ss'123\\",
			want:  "MyP@ss''123\\\\",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := escapeSQLString(tt.input)
			if got != tt.want {
				t.Errorf("escapeSQLString(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestGetClusterAuthCredentials(t *testing.T) {
	tests := []struct {
		name       string
		secretData map[string][]byte
		wantUser   string
		wantPass   string
	}{
		{
			name:       "nil secret data",
			secretData: nil,
			wantUser:   "root",
			wantPass:   "",
		},
		{
			name:       "empty secret data",
			secretData: map[string][]byte{},
			wantUser:   "root",
			wantPass:   "",
		},
		{
			name:       "both username and password",
			secretData: map[string][]byte{"username": []byte("admin"), "password": []byte("secret")},
			wantUser:   "admin",
			wantPass:   "secret",
		},
		{
			name:       "only password",
			secretData: map[string][]byte{"password": []byte("mypass")},
			wantUser:   "root",
			wantPass:   "mypass",
		},
		{
			name:       "only username without password",
			secretData: map[string][]byte{"username": []byte("operator")},
			wantUser:   "operator",
			wantPass:   "",
		},
		{
			name:       "empty username falls back to root",
			secretData: map[string][]byte{"username": []byte(""), "password": []byte("pass")},
			wantUser:   "root",
			wantPass:   "pass",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotUser, gotPass := GetClusterAuthCredentials(tt.secretData)
			if gotUser != tt.wantUser {
				t.Errorf("GetClusterAuthCredentials username = %q, want %q", gotUser, tt.wantUser)
			}
			if gotPass != tt.wantPass {
				t.Errorf("GetClusterAuthCredentials password = %q, want %q", gotPass, tt.wantPass)
			}
		})
	}
}

func TestMatchPodToBackend(t *testing.T) {
	tests := []struct {
		name     string
		podName  string
		backends []BackendInfo
		want     bool
	}{
		{
			name:    "exact hostname match",
			podName: "doris-sample-be-default-0",
			backends: []BackendInfo{
				{Host: "doris-sample-be-default-0", Port: 9050},
			},
			want: true,
		},
		{
			name:    "substring match",
			podName: "be-default-0",
			backends: []BackendInfo{
				{Host: "doris-sample-be-default-0", Port: 9050},
			},
			want: true,
		},
		{
			name:    "no match",
			podName: "other-pod-0",
			backends: []BackendInfo{
				{Host: "doris-sample-be-default-0", Port: 9050},
			},
			want: false,
		},
		{
			name:     "empty backends",
			podName:  "pod-0",
			backends: []BackendInfo{},
			want:     false,
		},
		{
			name:    "matches correct one among multiple",
			podName: "be-default-1",
			backends: []BackendInfo{
				{Host: "doris-sample-be-default-0", Port: 9050},
				{Host: "doris-sample-be-default-1", Port: 9050},
				{Host: "doris-sample-be-default-2", Port: 9050},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchPodToBackend(tt.podName, tt.backends)
			if (got != nil) != tt.want {
				t.Errorf("MatchPodToBackend() = %v, want match=%v", got != nil, tt.want)
			}
		})
	}
}

func TestMatchPodToFrontend(t *testing.T) {
	tests := []struct {
		name      string
		podName   string
		frontends []FrontendInfo
		want      bool
	}{
		{
			name:    "exact hostname match",
			podName: "doris-sample-fe-default-0",
			frontends: []FrontendInfo{
				{Host: "doris-sample-fe-default-0"},
			},
			want: true,
		},
		{
			name:    "substring match",
			podName: "fe-default-0",
			frontends: []FrontendInfo{
				{Host: "doris-sample-fe-default-0"},
			},
			want: true,
		},
		{
			name:      "no match",
			podName:   "other-pod-0",
			frontends: []FrontendInfo{{Host: "fe-0"}},
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchPodToFrontend(tt.podName, tt.frontends)
			if (got != nil) != tt.want {
				t.Errorf("MatchPodToFrontend() = %v, want match=%v", got != nil, tt.want)
			}
		})
	}
}

func TestIsDecommissionComplete(t *testing.T) {
	tests := []struct {
		name string
		be   BackendInfo
		want bool
	}{
		{
			name: "decommission complete",
			be:   BackendInfo{Decommission: true, TabletNum: 0},
			want: true,
		},
		{
			name: "decommission in progress",
			be:   BackendInfo{Decommission: true, TabletNum: 100},
			want: false,
		},
		{
			name: "not decommissioning",
			be:   BackendInfo{Decommission: false, TabletNum: 0},
			want: false,
		},
		{
			name: "decommission with zero tablets by default",
			be:   BackendInfo{TabletNum: 0},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsDecommissionComplete(tt.be); got != tt.want {
				t.Errorf("IsDecommissionComplete() = %v, want %v", got, tt.want)
			}
		})
	}
}
