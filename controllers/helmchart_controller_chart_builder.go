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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	securejoin "github.com/cyphar/filepath-securejoin"
	helmchart "helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/getter"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/fluxcd/pkg/runtime/transform"

	sourcev1 "github.com/fluxcd/source-controller/api/v1beta1"
	"github.com/fluxcd/source-controller/internal/helm"
)

type chartBuilder struct {
	client    client.Client
	storage   *Storage
	getters   getter.Providers
	namespace string
	workDir   string

	sourcePath string
	chartPath  string

	metadata      *helmchart.Metadata
	loadedChart   *helmchart.Chart
	modifiedChart bool

	repositories map[string]*helm.ChartRepository
}

func newChartBuilder(client client.Client, storage *Storage, getters getter.Providers, namespace, workDir string) *chartBuilder {
	return &chartBuilder{
		client:    client,
		storage:   storage,
		getters:   getters,
		namespace: namespace,
		workDir:   workDir,
	}
}

func (b *chartBuilder) MergeValuesFiles(valuesRefs []string) (err error) {
	chart, err := b.chart()
	if err != nil {
		return err
	}

	var merged map[string]interface{}
	switch {
	case b.sourcePath != "":
		if merged, err = mergeFileValues(b.sourcePath, valuesRefs); err != nil {
			return
		}
	default:
		if merged, err = mergeChartValues(chart, valuesRefs); err != nil {
			return
		}
	}

	var (
		mb      []byte
		changed bool
	)
	if mb, err = yaml.Marshal(merged); err != nil {
		return
	}
	if changed, err = helm.OverwriteChartDefaultValues(chart, mb); err != nil {
		return
	}
	if changed {
		b.modifiedChart = true
	}
	return
}

func (b *chartBuilder) FetchMissingDependencies(ctx context.Context) (int, error) {
	// We only want to build dependencies for charts from a source that aren't packaged
	if chartIsDir, err := b.ChartSourceIsDir(); err != nil || !chartIsDir || b.sourcePath == "" {
		return 0, err
	}

	// Ensure chart is loaded
	chart, err := b.chart()
	if err != nil {
		return 0, err
	}

	// Collect chart dependency metadata, granting the lock file precedence
	var (
		deps = chart.Dependencies()
		reqs = chart.Metadata.Dependencies
	)
	if lock := chart.Lock; lock != nil {
		reqs = lock.Dependencies
	}

	// If the number of dependencies equals the number of requests we already do have all dependencies
	if len(deps) == len(reqs) {
		return 0, nil
	}

	// Configure DependencyManager for missing dependencies
	relChartPath, err := filepath.Rel(b.sourcePath, b.chartPath)
	if err != nil {
		return 0, err
	}
	dm := &helm.DependencyManager{
		WorkingDir: b.sourcePath,
		ChartPath:  relChartPath,
		Chart:      chart,
	}
	for _, dep := range reqs {
		// Exclude existing dependencies
		found := false
		for _, existing := range deps {
			name := dep.Name
			if dep.Alias != "" {
				name = dep.Alias
			}
			if existing.Name() == name {
				found = true
			}
		}
		if found {
			continue
		}
		dwr, err := b.getDependency(ctx, dep)
		if err != nil {
			return 0, err
		}
		dm.Dependencies = append(dm.Dependencies, dwr)
	}

	// Run the build and mark the chart as modified
	if err = dm.Build(context.TODO()); err != nil {
		return 0, err
	}
	b.modifiedChart = true
	return len(dm.Dependencies), nil
}

func (b *chartBuilder) LoadMetadata() (*helmchart.Metadata, error) {
	if b.loadedChart != nil {
		// Prefer the metadata from the loaded chart over any previous loaded metadata
		b.metadata = b.loadedChart.Metadata
	}
	if b.metadata != nil {
		// Return early if we already loaded the metadata
		return b.metadata, nil
	}
	if b.chartPath == "" {
		return nil, fmt.Errorf("failed to load metadata: chartPath is not set")
	}

	// Load the metadata
	m, err := helm.LoadChartMetadata(b.chartPath)
	if err != nil {
		return nil, err
	}
	b.metadata = m
	return b.metadata, nil
}

func (b *chartBuilder) Build() (string, error) {
	chartIsDir, err := b.ChartSourceIsDir()
	if err != nil {
		return "", err
	}
	switch {
	case b.modifiedChart, chartIsDir:
		targetDir, err := os.MkdirTemp(b.workDir, "build-")
		chart, err := b.chart()
		if err != nil {
			return "", err
		}
		return chartutil.Save(chart, targetDir)
	default:
		return b.chartPath, nil
	}
}

// ChartSourceIsDir returns if the underlying source of this chart build is a directory (opposed to it being a packaged
// Helm chart file).
func (b *chartBuilder) ChartSourceIsDir() (bool, error) {
	if b.chartPath == "" {
		return false, fmt.Errorf("failed to determine if chart is dir: chartPath not set")
	}
	i, err := os.Stat(b.chartPath)
	if err != nil {
		return false, fmt.Errorf("failed to determine if chartPath is dir: %w", err)
	}
	return i.IsDir(), nil
}

// IndexChartRepository adds the given helm.ChartRepository to the list of known indexes using the url as key.
func (b *chartBuilder) IndexChartRepository(url string, index *helm.ChartRepository) {
	b.repositories[helm.NormalizeChartRepositoryURL(url)] = index
}

func (b *chartBuilder) getDependency(ctx context.Context, dep *helmchart.Dependency) (*helm.DependencyWithRepository, error) {
	// Return early if file schema detected
	if dep.Repository == "" || strings.HasPrefix(dep.Repository, "file://") {
		return &helm.DependencyWithRepository{
			Dependency: dep,
			Repository: nil,
		}, nil
	}

	// Check if we already resolved the index for this dependency, if we didn't, fetch it
	index, ok := b.repositories[helm.NormalizeChartRepositoryURL(dep.Repository)];
	if !ok {
		// Attempt to resolve it based on the URL, if no repository is found; a shim is created to attempt to download
		// the index without any custom configuration
		repository, err := b.resolveDependencyRepository(ctx, helm.NormalizeChartRepositoryURL(dep.Repository))
		if err != nil {
			repository = &sourcev1.HelmRepository{
				Spec: sourcev1.HelmRepositorySpec{
					URL:     dep.Repository,
					Timeout: &metav1.Duration{Duration: 60 * time.Second},
				},
			}
		}

		// Configure Helm client getter options with data from repository
		clientOpts := []getter.Option{
			getter.WithTimeout(repository.Spec.Timeout.Duration),
			getter.WithURL(repository.Spec.URL),
			getter.WithPassCredentialsAll(repository.Spec.PassCredentials),
		}
		if repository.Spec.SecretRef != nil {
			name := types.NamespacedName{
				Namespace: repository.GetNamespace(),
				Name:      repository.Spec.SecretRef.Name,
			}
			secret := corev1.Secret{}
			if err := b.client.Get(ctx, name, &secret); err != nil {
				return nil, err
			}
			opts, err := helm.ClientOptionsFromSecret(secret, b.workDir)
			if err != nil {
				return nil, err
			}
			clientOpts = append(clientOpts, opts...)
		}

		var cachePath string
		if artifact := repository.GetArtifact(); artifact != nil {
			cachePath = b.storage.LocalPath(*artifact)
		}

		// Initialize the chart repository and load the index file, adding it to the builder's indexes once loaded
		index, err = helm.NewChartRepository(repository.Spec.URL, cachePath, b.getters, clientOpts)
		if err != nil {
			return nil, err
		}
		b.IndexChartRepository(index.URL, index)
	}

	return &helm.DependencyWithRepository{
		Dependency: dep,
		Repository: index,
	}, nil
}

// resolveDependencyRepository attempts to resolve the given URL to a HelmRepository in the configured namespace using
// the v1beta1.HelmRepositoryURLIndexKey.
func (b *chartBuilder) resolveDependencyRepository(ctx context.Context, url string) (*sourcev1.HelmRepository, error) {
	listOpts := []client.ListOption{
		client.InNamespace(b.namespace),
		client.MatchingFields{sourcev1.HelmRepositoryURLIndexKey: url},
	}
	var list sourcev1.HelmRepositoryList
	if err := b.client.List(ctx, &list, listOpts...); err != nil {
		return nil, fmt.Errorf("unable to retrieve HelmRepositoryList: %w", err)
	}
	if len(list.Items) > 0 {
		return &list.Items[0], nil
	}
	return nil, fmt.Errorf("no HelmRepository found for URL '%s'", url)
}

// chart lazy-loads loadedChart from chartPath.
func (b *chartBuilder) chart() (_ *helmchart.Chart, err error) {
	if b.loadedChart == nil {
		if b.chartPath == "" {
			return nil, fmt.Errorf("failed to load chart: chartPath not set")
		}
		if b.loadedChart, err = loader.Load(b.chartPath); err != nil {
			return nil, err
		}
	}
	return b.loadedChart, nil
}

func mergeChartValues(chart *helmchart.Chart, valuesRefs []string) (map[string]interface{}, error) {
	mergedValues := make(map[string]interface{})
	for _, p := range valuesRefs {
		cfn := filepath.Clean(p)
		if cfn == chartutil.ValuesfileName {
			mergedValues = transform.MergeMaps(mergedValues, chart.Values)
		}
		var b []byte
		for _, f := range chart.Files {
			if f.Name == cfn {
				b = f.Data
			}
		}
		if b == nil {
			return nil, fmt.Errorf("no values file found at path '%s'", p)
		}
		values := make(map[string]interface{})
		if err := yaml.Unmarshal(b, values); err != nil {
			return nil, fmt.Errorf("unmarshaling values from %q failed: %w", p, err)
		}
		mergedValues = transform.MergeMaps(mergedValues, values)
	}
	return mergedValues, nil
}

func mergeFileValues(baseDir string, valuesRefs []string) (map[string]interface{}, error) {
	mergedValues := make(map[string]interface{})
	for _, p := range valuesRefs {
		secureP, err := securejoin.SecureJoin(baseDir, p)
		if err != nil {
			return nil, err
		}
		if f, err := os.Stat(secureP); os.IsNotExist(err) || !f.Mode().IsRegular() {
			return nil, fmt.Errorf("invalid values file path '%s'", p)
		}
		b, err := os.ReadFile(secureP)
		if err != nil {
			return nil, fmt.Errorf("could not read values from file '%s': %w", p, err)
		}
		values := make(map[string]interface{})
		err = yaml.Unmarshal(b, &values)
		if err != nil {
			return nil, fmt.Errorf("unmarshaling values from '%s' failed: %w", p, err)
		}
		mergedValues = transform.MergeMaps(mergedValues, values)
	}
	return mergedValues, nil
}
