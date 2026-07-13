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

package common

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	dorisv1alpha1 "github.com/zncdatadev/doris-operator/api/v1alpha1"
	"github.com/zncdatadev/doris-operator/internal/controller/constants"
)

func TestGetImage(t *testing.T) {
	pullAlways := corev1.PullAlways

	tests := []struct {
		name           string
		imageSpec      *dorisv1alpha1.ImageSpec
		componentType  constants.ComponentType
		wantImage      string
		wantPullPolicy corev1.PullPolicy
	}{
		{
			name:           "nil spec falls back to official image with default version",
			imageSpec:      nil,
			componentType:  constants.ComponentTypeFE,
			wantImage:      "apache/doris:fe-" + constants.DefaultProductVersion,
			wantPullPolicy: corev1.PullIfNotPresent,
		},
		{
			name:           "productVersion overrides default in official image",
			imageSpec:      &dorisv1alpha1.ImageSpec{ProductVersion: "3.0.3"},
			componentType:  constants.ComponentTypeBE,
			wantImage:      "apache/doris:be-3.0.3",
			wantPullPolicy: corev1.PullIfNotPresent,
		},
		{
			name: "defaulted spec without explicit productVersion uses default version",
			imageSpec: &dorisv1alpha1.ImageSpec{
				Repo:            dorisv1alpha1.DefaultRepository,
				KubedoopVersion: dorisv1alpha1.DefaultKubedoopVersion,
			},
			componentType:  constants.ComponentTypeBroker,
			wantImage:      "apache/doris:broker-" + constants.DefaultProductVersion,
			wantPullPolicy: corev1.PullIfNotPresent,
		},
		{
			name: "pull policy from spec is honored on official image path",
			imageSpec: &dorisv1alpha1.ImageSpec{
				ProductVersion: "2.1.8",
				PullPolicy:     &pullAlways,
			},
			componentType:  constants.ComponentTypeFE,
			wantImage:      "apache/doris:fe-2.1.8",
			wantPullPolicy: corev1.PullAlways,
		},
		{
			name:           "custom image bypasses official image path",
			imageSpec:      &dorisv1alpha1.ImageSpec{Custom: "my.repo.org/doris:custom"},
			componentType:  constants.ComponentTypeFE,
			wantImage:      "my.repo.org/doris:custom",
			wantPullPolicy: corev1.PullIfNotPresent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			img := GetImage(tt.imageSpec, tt.componentType)
			if got := img.String(); got != tt.wantImage {
				t.Errorf("GetImage() image = %q, want %q", got, tt.wantImage)
			}
			if img.PullPolicy != tt.wantPullPolicy {
				t.Errorf("GetImage() pull policy = %q, want %q", img.PullPolicy, tt.wantPullPolicy)
			}
		})
	}
}

func TestGetImagePullSecret(t *testing.T) {
	img := GetImage(&dorisv1alpha1.ImageSpec{
		ProductVersion: "3.0.3",
		PullSecretName: "my-pull-secret",
	}, constants.ComponentTypeFE)
	if img.PullSecretName != "my-pull-secret" {
		t.Errorf("GetImage() pull secret = %q, want %q", img.PullSecretName, "my-pull-secret")
	}
}
