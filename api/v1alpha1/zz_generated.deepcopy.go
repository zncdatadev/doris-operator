//go:build !ignore_autogenerated

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

// Code generated by controller-gen. DO NOT EDIT.

package v1alpha1

import (
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ClusterConfigSpec) DeepCopyInto(out *ClusterConfigSpec) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ClusterConfigSpec.
func (in *ClusterConfigSpec) DeepCopy() *ClusterConfigSpec {
	if in == nil {
		return nil
	}
	out := new(ClusterConfigSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ConfigSpec) DeepCopyInto(out *ConfigSpec) {
	*out = *in
	if in.RoleGroupConfigSpec != nil {
		in, out := &in.RoleGroupConfigSpec, &out.RoleGroupConfigSpec
		*out = new(commonsv1alpha1.RoleGroupConfigSpec)
		(*in).DeepCopyInto(*out)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ConfigSpec.
func (in *ConfigSpec) DeepCopy() *ConfigSpec {
	if in == nil {
		return nil
	}
	out := new(ConfigSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *DorisCluster) DeepCopyInto(out *DorisCluster) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new DorisCluster.
func (in *DorisCluster) DeepCopy() *DorisCluster {
	if in == nil {
		return nil
	}
	out := new(DorisCluster)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *DorisCluster) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *DorisClusterList) DeepCopyInto(out *DorisClusterList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]DorisCluster, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new DorisClusterList.
func (in *DorisClusterList) DeepCopy() *DorisClusterList {
	if in == nil {
		return nil
	}
	out := new(DorisClusterList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *DorisClusterList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *DorisClusterSpec) DeepCopyInto(out *DorisClusterSpec) {
	*out = *in
	if in.Image != nil {
		in, out := &in.Image, &out.Image
		*out = new(ImageSpec)
		(*in).DeepCopyInto(*out)
	}
	if in.ClusterConfig != nil {
		in, out := &in.ClusterConfig, &out.ClusterConfig
		*out = new(ClusterConfigSpec)
		**out = **in
	}
	if in.ClusterOperationSpec != nil {
		in, out := &in.ClusterOperationSpec, &out.ClusterOperationSpec
		*out = new(commonsv1alpha1.ClusterOperationSpec)
		**out = **in
	}
	if in.FrontEnd != nil {
		in, out := &in.FrontEnd, &out.FrontEnd
		*out = new(RoleSpec)
		(*in).DeepCopyInto(*out)
	}
	if in.BackEnd != nil {
		in, out := &in.BackEnd, &out.BackEnd
		*out = new(RoleSpec)
		(*in).DeepCopyInto(*out)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new DorisClusterSpec.
func (in *DorisClusterSpec) DeepCopy() *DorisClusterSpec {
	if in == nil {
		return nil
	}
	out := new(DorisClusterSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ImageSpec) DeepCopyInto(out *ImageSpec) {
	*out = *in
	if in.PullPolicy != nil {
		in, out := &in.PullPolicy, &out.PullPolicy
		*out = new(v1.PullPolicy)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ImageSpec.
func (in *ImageSpec) DeepCopy() *ImageSpec {
	if in == nil {
		return nil
	}
	out := new(ImageSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RoleGroupSpec) DeepCopyInto(out *RoleGroupSpec) {
	*out = *in
	if in.Replicas != nil {
		in, out := &in.Replicas, &out.Replicas
		*out = new(int32)
		**out = **in
	}
	if in.Config != nil {
		in, out := &in.Config, &out.Config
		*out = new(ConfigSpec)
		(*in).DeepCopyInto(*out)
	}
	if in.OverridesSpec != nil {
		in, out := &in.OverridesSpec, &out.OverridesSpec
		*out = new(commonsv1alpha1.OverridesSpec)
		(*in).DeepCopyInto(*out)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RoleGroupSpec.
func (in *RoleGroupSpec) DeepCopy() *RoleGroupSpec {
	if in == nil {
		return nil
	}
	out := new(RoleGroupSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RoleSpec) DeepCopyInto(out *RoleSpec) {
	*out = *in
	if in.Config != nil {
		in, out := &in.Config, &out.Config
		*out = new(ConfigSpec)
		(*in).DeepCopyInto(*out)
	}
	if in.RoleGroups != nil {
		in, out := &in.RoleGroups, &out.RoleGroups
		*out = make(map[string]RoleGroupSpec, len(*in))
		for key, val := range *in {
			(*out)[key] = *val.DeepCopy()
		}
	}
	if in.RoleConfig != nil {
		in, out := &in.RoleConfig, &out.RoleConfig
		*out = new(commonsv1alpha1.RoleConfigSpec)
		(*in).DeepCopyInto(*out)
	}
	if in.OverridesSpec != nil {
		in, out := &in.OverridesSpec, &out.OverridesSpec
		*out = new(commonsv1alpha1.OverridesSpec)
		(*in).DeepCopyInto(*out)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RoleSpec.
func (in *RoleSpec) DeepCopy() *RoleSpec {
	if in == nil {
		return nil
	}
	out := new(RoleSpec)
	in.DeepCopyInto(out)
	return out
}
