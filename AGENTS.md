<!-- Generated: 2026-05-19 | Updated: 2026-05-19 -->

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
| `internal/controller/fe/` | FE role reconciler (StatefulSet, ConfigMap, Service, LDAP) |
| `internal/controller/be/` | BE role reconciler (StatefulSet, ConfigMap, Service) |
| `internal/controller/broker/` | Broker role reconciler (StatefulSet, ConfigMap, Service) |
| `internal/controller/common/` | Shared resources (configmap, statefulset, service, image helper) |
| `internal/controller/constants/` | Component constants (ports, images, paths, labels) |
| `internal/controller/scale/` | Scale management (BE decommission/force-drop, FE drop-observer, STS gate, timeout) |
| `internal/controller/doris_client/` | Doris MySQL protocol client for cluster management SQL operations |
| `test/e2e/` | End-to-end test suites (chainsaw) |

## For AI Agents

### Working In This Directory
- Standard Kubebuilder operator structure
- Uses `github.com/zncdatadev/operator-go` framework for reconciliation
- Run `make test` for unit tests
- Run `make deploy IMG=<image>` to deploy to cluster (do not commit kustomization.yaml changes)
- CRD group: `doris.kubedoop.dev`
- Three components: FE, BE, Broker — all use `BaseDorisRoleReconciler` pattern

### Development Workflow
- Fork-based workflow: fork → branch → worktree → PR to upstream `zncdatadev/doris-operator`
- **Do not push directly to upstream repositories**
- CRD/logic/test changes must pass e2e regression before submitting PR
- Image strategy: currently uses Apache official component images (`apache/doris:fe-<ver>`, `apache/doris:be-<ver>`, `apache/doris:broker-<ver>`)

### Testing Requirements
- E2E tests in `test/e2e/` using chainsaw framework
- Requires a Kind cluster: `kind create cluster --config test/e2e/kind-config.yaml`
- Test images use Apache official Doris images from Docker Hub
- Two test matrix: Kubernetes 1.26.x and 1.32.x
- Broker depends on FE being ready (entrypoint waits for FE Master election with 60s timeout)

### Common Patterns
- Main controller: `internal/controller/doriscluster_controller.go`
- Cluster reconciler: `internal/controller/cluster.go` — registers FE/BE/Broker role reconcilers
- Role reconcilers: `fe/role.go`, `be/role.go`, `broker/role.go`
- Each role creates: ConfigMap (component config) + Internal Service (headless) + Access Service + StatefulSet + Metrics Service
- Shared logic in `common/`: `BaseDorisRoleReconciler`, `RegisterStandardResources`, `StatefulSetBuilder`
- Broker is stateless (no PVC, no init container), FE has PVC for metadata, BE has PVC for storage + init container for sysctl
- CRD spec uses independent fields: `spec.frontend`, `spec.backend`, `spec.broker` (type-safe, backward compatible)
- Scale management: `internal/controller/scale/` handles safe scale-down via Doris MySQL protocol
  - STS replica gating: prevents premature pod deletion during active BE decommission
  - Decommission timeout: automatic fallback to force-drop after configurable timeout (default 2h)
  - Decommission start time tracked via CR annotations (`doris.kubedoop.dev/decommission-start`)
  - FE scale-down limited to OBSERVER nodes (follower nodes are protected)

## Dependencies

### Internal
- `../operator-go` — Shared operator framework (`github.com/zncdatadev/operator-go v0.12.6`)

### External
- `sigs.k8s.io/controller-runtime` v0.23+
- `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/client-go` v0.35+
- Go 1.25+
- Kubernetes 1.26+

### AI Worktree Development Mode

**IMPORTANT**: When making code changes, work in a worktree under `.worktree/`, NOT in the main working directory.

#### Workflow
1. Create worktree: `git worktree add .worktree/<branch-name> -b <branch-name>`
2. Work in `.worktree/<branch-name>/` directory
3. Test: `cd .worktree/<branch-name> && make lint && make test`
4. Commit changes in the worktree
5. Push and create PR from the worktree branch
6. Cleanup: `git worktree remove .worktree/<branch-name>`

#### Rules
- NEVER modify files directly in the main working directory
- Each task gets its own worktree with a descriptive branch name
- Run `make generate` if API structs are modified
- Run `make lint && make test` before committing

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
