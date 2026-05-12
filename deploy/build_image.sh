#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION_FILE="$ROOT_DIR/VERSION"
DOCKERFILE="$ROOT_DIR/deploy/Dockerfile"

VERSION="${BIOCIRCUITS_EXPLORER_VERSION:-}"
IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-${ECR_REPOSITORY_URI:-biocircuits-explorer}}"
PLATFORM="${PLATFORM:-linux/amd64}"
PUSH=0
LATEST=0
NO_CACHE=0
CREATE_ECR_REPO=0
REQUIRE_CLEAN=0
DRY_RUN=0
EXTRA_TAGS=()
HAS_BUILDX=0

usage() {
    cat <<'USAGE'
Usage:
  deploy/build_image.sh [options]

Common:
  deploy/build_image.sh
  deploy/build_image.sh --repo biocircuits-explorer
  deploy/build_image.sh --push --repo <account>.dkr.ecr.<region>.amazonaws.com/biocircuits-explorer
  deploy/build_image.sh --push --create-ecr-repo --latest --repo <account>.dkr.ecr.<region>.amazonaws.com/biocircuits-explorer

Options:
  --repo <uri>          Docker image repository. Env: IMAGE_REPOSITORY or ECR_REPOSITORY_URI.
  --version <semver>   Override VERSION file. Env: BIOCIRCUITS_EXPLORER_VERSION.
  --platform <value>   Build platform, default linux/amd64. Env: PLATFORM.
  --tag <tag>          Add an extra tag, repeatable.
  --latest             Also tag as latest.
  --push               Push with docker buildx instead of loading locally.
  --create-ecr-repo    Create the ECR repo if missing. Only works with ECR repo URIs.
  --no-cache           Pass --no-cache to docker buildx.
  --require-clean      Fail if the git worktree is dirty.
  --dry-run            Print commands without executing them.
  -h, --help           Show this help.
USAGE
}

shell_quote() {
    printf '%q' "$1"
}

print_cmd() {
    local first=1
    for arg in "$@"; do
        if [ "$first" -eq 0 ]; then
            printf ' '
        fi
        shell_quote "$arg"
        first=0
    done
    printf '\n'
}

run_cmd() {
    echo "+ $(print_cmd "$@")"
    if [ "$DRY_RUN" -eq 0 ]; then
        "$@"
    fi
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --repo)
            IMAGE_REPOSITORY="$2"
            shift 2
            ;;
        --version)
            VERSION="$2"
            shift 2
            ;;
        --platform)
            PLATFORM="$2"
            shift 2
            ;;
        --tag)
            EXTRA_TAGS+=("$2")
            shift 2
            ;;
        --latest)
            LATEST=1
            shift
            ;;
        --push)
            PUSH=1
            shift
            ;;
        --create-ecr-repo)
            CREATE_ECR_REPO=1
            shift
            ;;
        --no-cache)
            NO_CACHE=1
            shift
            ;;
        --require-clean)
            REQUIRE_CLEAN=1
            shift
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [ -z "$VERSION" ]; then
    if [ ! -f "$VERSION_FILE" ]; then
        echo "Missing VERSION file: $VERSION_FILE" >&2
        exit 1
    fi
    VERSION="$(tr -d '[:space:]' < "$VERSION_FILE")"
fi

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?$ ]]; then
    echo "Version must be semver-like, got: $VERSION" >&2
    exit 2
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required" >&2
    exit 1
fi

if docker buildx version >/dev/null 2>&1; then
    HAS_BUILDX=1
fi

if [ "$HAS_BUILDX" -eq 0 ] && [[ "$PLATFORM" == *,* ]]; then
    echo "Multi-platform builds require docker buildx; install the Docker buildx plugin or use a single platform." >&2
    exit 2
fi

REVISION="unknown"
DIRTY=0
if git -C "$ROOT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    REVISION="$(git -C "$ROOT_DIR" rev-parse --short=12 HEAD)"
    if ! git -C "$ROOT_DIR" diff --quiet --ignore-submodules -- || \
       ! git -C "$ROOT_DIR" diff --cached --quiet --ignore-submodules -- || \
       [ -n "$(git -C "$ROOT_DIR" ls-files --others --exclude-standard)" ]; then
        DIRTY=1
    fi
fi

if [ "$DIRTY" -eq 1 ]; then
    if [ "$REQUIRE_CLEAN" -eq 1 ]; then
        echo "Git worktree is dirty; commit/stash changes or omit --require-clean." >&2
        exit 1
    fi
    REVISION="${REVISION}-dirty"
    echo "Warning: building from a dirty worktree; image revision label will be $REVISION" >&2
fi

CREATED="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
TAGS=("$VERSION" "$VERSION-$REVISION")
if [ "$LATEST" -eq 1 ]; then
    TAGS+=("latest")
fi
if [ "${#EXTRA_TAGS[@]}" -gt 0 ]; then
    TAGS+=("${EXTRA_TAGS[@]}")
fi

is_ecr_repo=0
if [[ "$IMAGE_REPOSITORY" =~ ^[0-9]+\.dkr\.ecr\.[A-Za-z0-9-]+\.amazonaws\.com/.+ ]]; then
    is_ecr_repo=1
fi

ecr_region=""
ecr_registry=""
ecr_repo_name=""
if [ "$is_ecr_repo" -eq 1 ]; then
    ecr_registry="${IMAGE_REPOSITORY%%/*}"
    ecr_repo_name="${IMAGE_REPOSITORY#*/}"
    ecr_region="$(printf '%s' "$ecr_registry" | sed -E 's/^[0-9]+\.dkr\.ecr\.([A-Za-z0-9-]+)\.amazonaws\.com$/\1/')"
fi

if [ "$PUSH" -eq 1 ] && [ "$is_ecr_repo" -eq 1 ]; then
    if ! command -v aws >/dev/null 2>&1; then
        echo "aws CLI is required to push to ECR" >&2
        exit 1
    fi
    if [ "$CREATE_ECR_REPO" -eq 1 ]; then
        if [ "$DRY_RUN" -eq 1 ]; then
            echo "+ aws ecr describe-repositories --region $ecr_region --repository-names $ecr_repo_name || aws ecr create-repository --region $ecr_region --repository-name $ecr_repo_name"
        elif ! aws ecr describe-repositories --region "$ecr_region" --repository-names "$ecr_repo_name" >/dev/null 2>&1; then
            run_cmd aws ecr create-repository --region "$ecr_region" --repository-name "$ecr_repo_name"
        fi
    fi
    if [ "$DRY_RUN" -eq 1 ]; then
        echo "+ aws ecr get-login-password --region $ecr_region | docker login --username AWS --password-stdin $ecr_registry"
    else
        aws ecr get-login-password --region "$ecr_region" | docker login --username AWS --password-stdin "$ecr_registry"
    fi
fi

if [ "$HAS_BUILDX" -eq 1 ]; then
    BUILD_CMD=(
        docker buildx build
        --platform "$PLATFORM"
        -f "$DOCKERFILE"
        --build-arg "BIOCIRCUITS_EXPLORER_VERSION=$VERSION"
        --build-arg "BIOCIRCUITS_EXPLORER_REVISION=$REVISION"
        --build-arg "BIOCIRCUITS_EXPLORER_CREATED=$CREATED"
        --label "org.opencontainers.image.title=Biocircuits Explorer"
        --label "org.opencontainers.image.version=$VERSION"
        --label "org.opencontainers.image.revision=$REVISION"
        --label "org.opencontainers.image.created=$CREATED"
        --label "org.opencontainers.image.source=https://github.com/YanzhangJiang/Biocircuits-Explorer"
    )
else
    BUILD_CMD=(
        docker build
        --platform "$PLATFORM"
        -f "$DOCKERFILE"
        --build-arg "BIOCIRCUITS_EXPLORER_VERSION=$VERSION"
        --build-arg "BIOCIRCUITS_EXPLORER_REVISION=$REVISION"
        --build-arg "BIOCIRCUITS_EXPLORER_CREATED=$CREATED"
        --label "org.opencontainers.image.title=Biocircuits Explorer"
        --label "org.opencontainers.image.version=$VERSION"
        --label "org.opencontainers.image.revision=$REVISION"
        --label "org.opencontainers.image.created=$CREATED"
        --label "org.opencontainers.image.source=https://github.com/YanzhangJiang/Biocircuits-Explorer"
    )
fi

if [ "$NO_CACHE" -eq 1 ]; then
    BUILD_CMD+=(--no-cache)
fi

for tag in "${TAGS[@]}"; do
    BUILD_CMD+=(-t "$IMAGE_REPOSITORY:$tag")
done

if [ "$PUSH" -eq 1 ] && [ "$HAS_BUILDX" -eq 1 ]; then
    BUILD_CMD+=(--push)
elif [ "$HAS_BUILDX" -eq 1 ]; then
    BUILD_CMD+=(--load)
elif [ "$DRY_RUN" -eq 0 ]; then
    echo "Info: docker buildx is not available; using docker build for local image loading." >&2
fi

BUILD_CMD+=("$ROOT_DIR")

echo "Version:    $VERSION"
echo "Revision:   $REVISION"
echo "Platform:   $PLATFORM"
echo "Repository: $IMAGE_REPOSITORY"
echo "Tags:"
for tag in "${TAGS[@]}"; do
    echo "  - $IMAGE_REPOSITORY:$tag"
done

run_cmd "${BUILD_CMD[@]}"

if [ "$PUSH" -eq 1 ] && [ "$HAS_BUILDX" -eq 0 ]; then
    for tag in "${TAGS[@]}"; do
        run_cmd docker push "$IMAGE_REPOSITORY:$tag"
    done
fi
