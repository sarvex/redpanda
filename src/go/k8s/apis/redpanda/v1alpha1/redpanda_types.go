// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package v1alpha1

import (
	"encoding/json"
	"fmt"
	"github.com/fluxcd/pkg/apis/meta"
	"github.com/ghodss/yaml"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RedpandaClusterSpec defines the desired state of Redpanda Cluster
type RedpandaClusterSpec struct {
	// NameOverride is the override to give your redpanda release
	NameOverride string `json:"nameOverride,omitempty"`
	// NameOverride is the override to give your redpanda release
	FullnameOverride string `json:"fullnameOverride,omitempty"`
	// NameOverride is the override to give your redpanda release
	ClusterDomain string `json:"clusterDomain,omitempty"`
	// NameOverride is the override to give your redpanda release
	CommonLabels []string `json:"commonLabels,omitempty"`
	// NameOverride is the override to give your redpanda release
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// NameOverride is the override to give your redpanda release
	Tolerations []string `json:"tolerations,omitempty"`
	// Image defines the container image to use for the redpanda cluster
	Image RedpandaImage `json:"image,omitempty"`
}

// RedpandaSpec defines the desired state of Redpanda
type RedpandaSpec struct {

	// ChartVersion defines the helm chart version to use
	ChartVersion string `json:"chartVersion,omitempty"`
	// HelmRepositoryName defines the repository to use, defaults to redpanda if not defined
	HelmRepositoryName string `json:"helmRepositoryName,omitempty"`
	// ClusterSpec defines the values to use in the cluster
	ClusterSpec RedpandaClusterSpec `json:"clusterSpec,omitempty"`
}

// RedpandaStatus defines the observed state of Redpanda
type RedpandaStatus struct {
	// Conditions holds the conditions for the HelmRelease.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastAppliedRevision is the revision of the last successfully applied source.
	// +optional
	LastAppliedRevision string `json:"lastAppliedRevision,omitempty"`

	// LastAttemptedRevision is the revision of the last reconciliation attempt.
	// +optional
	LastAttemptedRevision string `json:"lastAttemptedRevision,omitempty"`

	// +optional
	HelmRelease string `json:"helmRelease,omitempty"`

	// +optional
	UpgradeFailures int64 `json:"upgradeFailures,omitempty"`

	// +optional
	InstallFailures int64 `json:"installFailures,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Redpanda is the Schema for the redpanda API
type Redpanda struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedpandaSpec   `json:"spec,omitempty"`
	Status RedpandaStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// RedpandaList contains a list of Redpanda
type RedpandaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Redpanda `json:"items"`
}

type RedpandaImage struct {
	Repository string `json:"repository,omitempty"`
	Tag        string `json:"tag,omitempty"`
	PullPolicy string `json:"pullPolicy,omitempty"`
}

func init() {
	SchemeBuilder.Register(&Redpanda{}, &RedpandaList{})
}

// GetHelmRelease returns the namespace and name of the HelmRelease.
func (in *RedpandaStatus) GetHelmRelease() string {
	return in.HelmRelease
}
func (in *Redpanda) GetHelmReleaseName() string {
	return in.Name
}

func (in *Redpanda) GetHelmRepositoryName() string {
	helmRepository := in.Spec.HelmRepositoryName
	if helmRepository == "" {
		helmRepository = "redpanda-repository"
	}
	return helmRepository
}

func (in *Redpanda) GetValuesJson() (*apiextensionsv1.JSON, error) {
	vyaml, err := yaml.Marshal(in.Spec.ClusterSpec)
	if err != nil {
		return nil, fmt.Errorf("could not convert spec to yaml: %s", err)
	}
	values := apiextensionsv1.JSON{Raw: []byte{}}
	json.Unmarshal(vyaml, &values)

	return &values, nil
}

// RedpandaReady registers a successful reconciliation of the given HelmRelease.
func RedpandaReady(rp Redpanda) Redpanda {
	newCondition := metav1.Condition{
		Type:    meta.ReadyCondition,
		Status:  metav1.ConditionTrue,
		Reason:  "Redpanda Cluster Deployed",
		Message: "Redpanda reconciliation succeeded",
	}
	apimeta.SetStatusCondition(rp.GetConditions(), newCondition)
	rp.Status.LastAppliedRevision = rp.Status.LastAttemptedRevision
	return rp
}

// GetConditions returns the status conditions of the object.
func (in *Redpanda) GetConditions() *[]metav1.Condition {
	return &in.Status.Conditions
}
