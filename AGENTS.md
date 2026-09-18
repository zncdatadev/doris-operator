<!-- Generated: 2026-05-19 | Updated: 2026-09-15 -->

# doris-operator

## Purpose
Manages Apache Doris deployments on Kubernetes. Handles creation, configuration, and lifecycle management of Doris clusters in storage-compute-integrated mode with FE (Frontend), BE (Backend), and Broker components. Supports LDAP authentication, Vector logging, Prometheus metrics, and safe scale-up/scale-down with decommission gating.

## Key Files
| File | Description |
|------|-------------|
| `go.mod` | Go module dependencies (`github.com/zncdatadev/doris-operator`) |
| `Makefile` | Build and development commands |
| `PROJECT` | Kubebuilder project metadata |
| `Dockerfile` | Operator container image build |

## Subdirectories
| Directory | Purpose |
|-----------|---------|
| `api/v1alpha1/` | CRD types: `DorisCluster` |
| `cmd/` | Operator entry point (`main.go`) |
| `config/` | Kubernetes manifests and kustomize configs |
| `config/samples/` | Example CR manifests |
| `internal/controller/` | Reconciliation controllers |
| `internal/controller/doris_handler.go` | Gen 3 role declarations, config resolution, and resource shaping for FE/BE/Broker |
| `internal/controller/service_extension.go` | Cluster-level reconciliation of legacy role-wide Services |
| `internal/controller/constants/` | Component constants (ports, images, paths, labels) |
| `internal/controller/scale/` | Gen 3 scale/status extension (BE decommission/force-drop, FE drop-observer, timeout) |
| `internal/controller/doris_client/` | Doris MySQL protocol client for cluster management SQL operations |
| `test/e2e/` | End-to-end test suites (chainsaw) |

## For AI Agents

### Working In This Directory
- Standard Kubebuilder operator structure
- Uses the operator-go Gen 3 `GenericReconciler` and `RoleGroupHandler` framework
- Run `make test` for unit tests
- Run `make deploy IMG=<image>` to deploy to cluster (do not commit kustomization.yaml changes)
- CRD group: `doris.kubedoop.dev`
- Three roles: FE, BE, Broker — all are declared by `DorisRoleGroupHandler`

### Development Workflow
- Fork-based workflow: fork → branch → worktree → PR to upstream `zncdatadev/doris-operator`
- **Do not push directly to upstream repositories**
- CRD/logic/test changes must pass e2e regression before submitting PR
- Image strategy: currently uses Apache official component images (`apache/doris:fe-<ver>`, `apache/doris:be-<ver>`, `apache/doris:broker-<ver>`)

### Testing Requirements
- E2E tests in `test/e2e/` using chainsaw framework
- Requires a Kind cluster: `kind create cluster --config test/e2e/kind-config.yaml`
- Test images use Apache official Doris images from Docker Hub
- Pull requests run Chainsaw on Kubernetes 1.35; release verification covers 1.33, 1.34, and 1.35
- Broker depends on FE being ready (entrypoint waits for FE Master election with 60s timeout)

### Common Patterns
- Main wiring: `cmd/main.go` creates the operator-go `GenericReconciler`
- Product seam: `internal/controller/doris_handler.go` declares FE/BE/Broker and shapes each role group's resources
- Each role creates: ConfigMap + governing headless Service + compatibility Internal/Access Services + StatefulSet + Metrics Service
- Product-specific scale and node status handling is registered as a cluster extension from `internal/controller/scale/`
- Broker is stateless (no PVC, no init container), FE has PVC for metadata, BE has PVC for storage + init container for sysctl
- CRD spec uses independent fields: `spec.frontend`, `spec.backend`, `spec.broker` (type-safe, backward compatible)
- Scale management: `internal/controller/scale/` handles safe scale-down via Doris MySQL protocol
  - STS replica gating: prevents premature pod deletion during active BE decommission
  - Decommission timeout: automatic fallback to force-drop after configurable timeout (default 2h)
  - Decommission start time tracked via CR annotations (`doris.kubedoop.dev/decommission-start`)
  - FE scale-down limited to OBSERVER nodes (follower nodes are protected)

## Dependencies

### Internal
- `../operator-go` — Shared operator framework (`github.com/zncdatadev/operator-go v0.13.0`)

### External
- `sigs.k8s.io/controller-runtime` v0.23+
- `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/client-go` v0.35+
- Go 1.25+
- Kubernetes 1.26+

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
