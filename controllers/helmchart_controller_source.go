/*
Copyright 2021 The Flux authors

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

package controllers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/fluxcd/pkg/apis/meta"
	"github.com/fluxcd/pkg/runtime/conditions"
	runerr "github.com/fluxcd/pkg/runtime/errors"
	"github.com/fluxcd/pkg/runtime/events"
	"github.com/fluxcd/pkg/untar"
	sourcev1 "github.com/fluxcd/source-controller/api/v1beta1"
	"github.com/fluxcd/source-controller/internal/helm"
	"helm.sh/helm/v3/pkg/getter"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

func (r *HelmChartReconciler) reconcileSource(ctx context.Context, obj *sourcev1.HelmChart, build *chartBuilder) (ctrl.Result, error) {
	// Attempt to get the source
	sourceObj, err := r.getSource(ctx, obj)
	if err != nil {
		switch {
		case errors.Is(err, &runerr.UnsupportedResourceKindError{}):
			return ctrl.Result{}, nil
		default:
			return ctrl.Result{}, err
		}
	}

	// Confirm source has an artifact
	if sourceObj.GetArtifact() == nil {
		conditions.MarkFalse(obj, sourcev1.FetchFailedCondition, "NoArtifact", "No artifact available for %s source '%s'",
			obj.Spec.SourceRef.Kind, obj.Spec.SourceRef.Name)
		// The watcher should notice an artifact change
		return ctrl.Result{}, nil
	}

	// Retrieve the contents from the source
	switch typedSource := sourceObj.(type) {
	case *sourcev1.HelmRepository:
		return r.reconcileFromHelmRepository(ctx, obj, typedSource, build)
	case *sourcev1.GitRepository, *sourcev1.Bucket:
		return r.reconcileFromTarballArtifact(ctx, obj, *typedSource.GetArtifact(), build)
	default:
		// This should never happen
		return ctrl.Result{}, fmt.Errorf("missing target for typed source object")
	}
}

func (r *HelmChartReconciler) reconcileFromHelmRepository(ctx context.Context, obj *sourcev1.HelmChart, repository *sourcev1.HelmRepository, build *chartBuilder) (ctrl.Result, error) {
	// TODO: move this to a validation webhook once the discussion around
	//  certificates has settled: https://github.com/fluxcd/image-reflector-controller/issues/69
	if err := validHelmChartName(obj.Spec.Chart); err != nil {
		conditions.MarkFalse(obj, sourcev1.FetchFailedCondition, "InvalidChartName",
			"Chart name validation error: %s", err.Error())
		return ctrl.Result{}, nil
	}

	// Configure Helm client to access repository
	clientOpts := []getter.Option{
		getter.WithTimeout(repository.Spec.Timeout.Duration),
		getter.WithURL(repository.Spec.URL),
		getter.WithPassCredentialsAll(repository.Spec.PassCredentials),
	}

	// Configure any authentication related options
	if repository.Spec.SecretRef != nil {
		// Attempt to retrieve secret
		name := types.NamespacedName{
			Namespace: repository.GetNamespace(),
			Name:      repository.Spec.SecretRef.Name,
		}
		var secret corev1.Secret
		if err := r.Client.Get(ctx, name, &secret); err != nil {
			conditions.MarkTrue(obj, sourcev1.FetchFailedCondition, sourcev1.AuthenticationFailedReason,
				"Failed to get secret '%s': %s", name.String(), err.Error())
			r.Eventf(ctx, obj, events.EventSeverityError, sourcev1.AuthenticationFailedReason,
				"Failed to get secret '%s': %s", name.String(), err.Error())
			// Return error as the world as observed may change
			return ctrl.Result{}, err
		}

		// Get client options from secret
		tmpDir, err := os.MkdirTemp(build.workDir, fmt.Sprintf("%s-%s-auth-", obj.GetNamespace(), obj.GetName()))
		if err != nil {
			conditions.MarkTrue(obj, sourcev1.FetchFailedCondition, sourcev1.AuthenticationFailedReason,
				"Failed to create temporary directory for credentials: %s", err.Error())
			r.Eventf(ctx, obj, events.EventSeverityError, sourcev1.AuthenticationFailedReason,
				"Failed to create temporary directory for credentials: %s", err.Error())
			return ctrl.Result{}, err
		}

		// Construct actual options
		opts, err := helm.ClientOptionsFromSecret(secret, tmpDir)
		if err != nil {
			conditions.MarkTrue(obj, sourcev1.FetchFailedCondition, sourcev1.AuthenticationFailedReason,
				"Failed to configure Helm client with secret data: %s", err)
			r.Eventf(ctx, obj, events.EventSeverityError, sourcev1.AuthenticationFailedReason,
				"Failed to configure Helm client with secret data: %s", err)
			// Return err as the content of the secret may change
			return ctrl.Result{}, err
		}
		clientOpts = append(clientOpts, opts...)
		defer os.RemoveAll(tmpDir)
	}

	// Construct Helm chart repository with options and load the index
	index, err := helm.NewChartRepository(repository.Spec.URL, r.Storage.LocalPath(*repository.GetArtifact()), r.Getters, clientOpts)
	if err != nil {
		switch err.(type) {
		case *url.Error:
			ctrl.LoggerFrom(ctx).Error(err, "invalid Helm repository URL")
			conditions.MarkTrue(obj, sourcev1.FetchFailedCondition, sourcev1.URLInvalidReason,
				"Invalid Helm repository URL: %s", err.Error())
			return ctrl.Result{}, nil
		default:
			ctrl.LoggerFrom(ctx).Error(err, "failed to construct Helm client")
			conditions.MarkTrue(obj, sourcev1.FetchFailedCondition, meta.FailedReason,
				"Failed to construct Helm client: %s", err.Error())
			return ctrl.Result{}, nil
		}
	}
	if err = index.LoadIndexFromFile(r.Storage.LocalPath(*repository.GetArtifact())); err != nil {
		conditions.MarkTrue(obj, sourcev1.FetchFailedCondition, sourcev1.ChartPullFailedReason,
			"Failed to load index from HelmRepository '%s' artifact: %s", repository.GetName(), err.Error())
		return ctrl.Result{}, err
	}

	// Lookup the chart version in the chart repository index
	chartVer, err := index.Get(obj.Spec.Chart, obj.Spec.Version)
	if err != nil {
		conditions.MarkTrue(obj, sourcev1.FetchFailedCondition, sourcev1.ChartPullFailedReason,
			"Could not find '%s' chart with version '%s': %s", obj.Spec.Chart, obj.Spec.Version, err.Error())
		// Do not return error, as change in HelmRepository artifact or HelmChart should trigger new reconcile
		return ctrl.Result{}, nil
	}

	// Create a new temporary file for the chart and download it
	f, err := os.CreateTemp(build.workDir, fmt.Sprintf("%s-%s-*.tgz", chartVer.Name, chartVer.Version))
	if err != nil {
		return ctrl.Result{}, err
	}
	b, err := index.DownloadChart(chartVer)
	if err != nil {
		f.Close()
		conditions.MarkTrue(obj, sourcev1.FetchFailedCondition, sourcev1.ChartPullFailedReason,
			"Failed to download chart version '%s': %s", chartVer.Version, err.Error())
		return ctrl.Result{}, err
	}
	if _, err = io.Copy(f, b); err != nil {
		f.Close()
		conditions.MarkTrue(obj, sourcev1.FetchFailedCondition, sourcev1.ChartPullFailedReason,
			"Failed to copy downloaded bytes of chart version '%s': %s", chartVer.Version, err.Error())
		return ctrl.Result{}, err
	}
	f.Close()

	// Fetch succeeded, set path and delete FetchFailedCondition
	build.chartPath = f.Name()
	conditions.Delete(obj, sourcev1.FetchFailedCondition)
	// TODO(event)?
	return ctrl.Result{RequeueAfter: obj.GetRequeueAfter()}, nil
}

func (r *HelmChartReconciler) reconcileFromTarballArtifact(ctx context.Context, obj *sourcev1.HelmChart, artifact sourcev1.Artifact, build *chartBuilder) (ctrl.Result, error) {
	f, err := os.Open(r.Storage.LocalPath(artifact))
	if err != nil {
		conditions.MarkTrue(obj, sourcev1.FetchFailedCondition, sourcev1.StorageOperationFailedReason,
			"Could not open artifact: %s", err.Error())
		return ctrl.Result{}, err
	}

	tmpDir, err := os.MkdirTemp(build.workDir, "source")
	if err != nil {
		conditions.MarkTrue(obj, sourcev1.FetchFailedCondition, sourcev1.StorageOperationFailedReason,
			"Could not create target directory for artifact extraction: %s", err.Error())
		return ctrl.Result{}, err
	}
	if _, err = untar.Untar(f, tmpDir); err != nil {
		f.Close()
		conditions.MarkTrue(obj, sourcev1.FetchFailedCondition, sourcev1.StorageOperationFailedReason,
			"Extraction of artifact failed: %s", err.Error())
		return ctrl.Result{}, err
	}
	f.Close()

	chartPath, err := determineChartPath(obj, tmpDir)
	if err != nil {
		conditions.MarkTrue(obj, sourcev1.FetchFailedCondition, sourcev1.StorageOperationFailedReason,
			"Extraction of artifact failed: %s", err.Error())
	}

	build.sourcePath = tmpDir
	build.chartPath = chartPath
	conditions.Delete(obj, sourcev1.FetchFailedCondition)
	return ctrl.Result{RequeueAfter: obj.GetRequeueAfter()}, nil
}

// getSource attempts to get the source referenced in the given object, if the referenced source kind is not supported
// it returns an UnsupportedSourceKindError, or any other error during the retrieval of the Source object.
func (r *HelmChartReconciler) getSource(ctx context.Context, obj *sourcev1.HelmChart) (sourcev1.Source, error) {
	namespacedName := types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.Spec.SourceRef.Name,
	}
	switch obj.Spec.SourceRef.Kind {
	case sourcev1.HelmRepositoryKind:
		repository := &sourcev1.HelmRepository{}
		if err := r.Client.Get(ctx, namespacedName, repository); err != nil {
			return nil, err
		}
		return repository, nil
	case sourcev1.GitRepositoryKind:
		repository := &sourcev1.GitRepository{}
		if err := r.Client.Get(ctx, namespacedName, repository); err != nil {
			return nil, err
		}
		return repository, nil
	case sourcev1.BucketKind:
		bucket := &sourcev1.Bucket{}
		if err := r.Client.Get(ctx, namespacedName, bucket); err != nil {
			return nil, err
		}
		return bucket, nil
	default:
		return nil, &runerr.UnsupportedResourceKindError{
			Kind:           obj.Spec.SourceRef.Kind,
			NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()},
			SupportedKinds: []string{sourcev1.HelmRepositoryKind, sourcev1.GitRepositoryKind, sourcev1.BucketKind},
		}
	}
}

func determineChartPath(obj *sourcev1.HelmChart, path string) (string, error) {
	switch obj.Spec.SourceRef.Kind {
	case sourcev1.BucketKind, sourcev1.GitRepositoryKind:
		chartPath, err := securejoin.SecureJoin(path, obj.Spec.Chart)
		if err != nil {
			return "", fmt.Errorf("failed to securely calculate chart path for '%s': %w", obj.Spec.Chart, err)
		}
		return chartPath, nil
	default:
		return "", fmt.Errorf("can not determine chart source path for kind '%s'", obj.Spec.SourceRef.Kind)
	}
}

