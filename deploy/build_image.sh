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
  deploy/build_image.sh --push --create-ecr-repo --repo <account>.dkr.ecr.<region>.amazonaws.com/biocircuits-explorer

Options:
  --repo <uri>          Docker image repository. Env: IMAGE_REPOSITORY or ECR_REPOSITORY_URI.
  --version <semver>    Assert the VERSION file value. Env: BIOCIRCUITS_EXPLORER_VERSION.
  --platform <value>   Build platform, default linux/amd64. Env: PLATFORM.
  --tag <tag>          Add an extra tag, repeatable.
  --latest             Also tag as latest.
  --push               Push instead of loading locally; requires a clean Git worktree.
  --create-ecr-repo    Create the ECR repo if missing. Only works with ECR repo URIs.
  --no-cache           Pass --no-cache to docker buildx.
  --require-clean      Also require a clean Git worktree for a local build.
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

require_option_value() {
    if [ "$#" -lt 2 ] || [ -z "$2" ] || [[ "$2" == --* ]]; then
        echo "Option $1 requires a value." >&2
        usage >&2
        exit 2
    fi
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --repo)
            require_option_value "$@"
            IMAGE_REPOSITORY="$2"
            shift 2
            ;;
        --version)
            require_option_value "$@"
            VERSION="$2"
            shift 2
            ;;
        --platform)
            require_option_value "$@"
            PLATFORM="$2"
            shift 2
            ;;
        --tag)
            require_option_value "$@"
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

if [ ! -f "$VERSION_FILE" ]; then
    echo "Missing VERSION file: $VERSION_FILE" >&2
    exit 1
fi
CANONICAL_VERSION=""
extra_version_line=""
exec 3<"$VERSION_FILE"
IFS= read -r CANONICAL_VERSION <&3 || true
if IFS= read -r extra_version_line <&3 || [ -n "$extra_version_line" ]; then
    exec 3<&-
    echo "VERSION must contain exactly one semantic-version line." >&2
    exit 2
fi
exec 3<&-
CANONICAL_VERSION="${CANONICAL_VERSION%$'\r'}"

if [ -z "$VERSION" ]; then
    VERSION="$CANONICAL_VERSION"
elif [ "$VERSION" != "$CANONICAL_VERSION" ]; then
    echo "Requested image version $VERSION does not match authoritative VERSION $CANONICAL_VERSION." >&2
    echo "Run scripts/set_version.sh $VERSION and commit the synchronized owners before building." >&2
    exit 2
fi

numeric_identifier='(0|[1-9][0-9]*)'
prerelease_identifier='(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)'
semver_regex="^${numeric_identifier}\.${numeric_identifier}\.${numeric_identifier}(-${prerelease_identifier}(\.${prerelease_identifier})*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$"
if ! [[ "$VERSION" =~ $semver_regex ]]; then
    echo "Version must be Semantic Versioning 2.0.0, got: $VERSION" >&2
    exit 2
fi

if [ ! -x "$ROOT_DIR/scripts/set_version.sh" ]; then
    echo "Missing executable version consistency gate: $ROOT_DIR/scripts/set_version.sh" >&2
    exit 1
fi
"$ROOT_DIR/scripts/set_version.sh" --dry-run "$VERSION" >/dev/null

# OCI labels keep the exact SemVer. Docker tags do not permit '+', so the
# unambiguous tag projection uses '_' for the single build-metadata separator.
VERSION_TAG="${VERSION//+/_}"

is_valid_docker_tag() {
    [[ "$1" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]
}

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
REVISION_TAG="unknown"
DIRTY=0
IN_GIT_WORKTREE=0
if git -C "$ROOT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    IN_GIT_WORKTREE=1
    REVISION="$(git -C "$ROOT_DIR" rev-parse HEAD)"
    REVISION_TAG="$(git -C "$ROOT_DIR" rev-parse --short=12 HEAD)"
    if ! git -C "$ROOT_DIR" diff --quiet --ignore-submodules -- || \
       ! git -C "$ROOT_DIR" diff --cached --quiet --ignore-submodules -- || \
       [ -n "$(git -C "$ROOT_DIR" ls-files --others --exclude-standard)" ]; then
        DIRTY=1
    fi
fi

if [ "$PUSH" -eq 1 ] && [ "$IN_GIT_WORKTREE" -ne 1 ]; then
    echo "Refusing to push without a Git worktree and immutable commit revision." >&2
    exit 1
fi

if [ "$PUSH" -eq 1 ] && [ "$LATEST" -eq 1 ]; then
    echo "Refusing to push the mutable :latest tag; publish the version and version-commit tags instead." >&2
    exit 2
fi

if [ "$DIRTY" -eq 1 ]; then
    if [ "$PUSH" -eq 1 ]; then
        echo "Refusing to push an image from a dirty Git worktree." >&2
        exit 1
    fi
    if [ "$REQUIRE_CLEAN" -eq 1 ]; then
        echo "Git worktree is dirty; commit/stash changes or omit --require-clean for a local build." >&2
        exit 1
    fi
    REVISION="${REVISION}-dirty"
    REVISION_TAG="${REVISION_TAG}-dirty"
    echo "Warning: building from a dirty worktree; image revision label will be $REVISION" >&2
fi

CREATED="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
TAGS=("$VERSION_TAG" "$VERSION_TAG-$REVISION_TAG")
if [ "$LATEST" -eq 1 ]; then
    TAGS+=("latest")
fi
if [ "${#EXTRA_TAGS[@]}" -gt 0 ]; then
    TAGS+=("${EXTRA_TAGS[@]}")
fi

UNIQUE_TAGS=()
for candidate in "${TAGS[@]}"; do
    if ! is_valid_docker_tag "$candidate"; then
        echo "Invalid Docker tag (1-128 characters; letters, digits, _, ., -): $candidate" >&2
        exit 2
    fi
    duplicate=0
    if [ "${#UNIQUE_TAGS[@]}" -gt 0 ]; then
        for existing in "${UNIQUE_TAGS[@]}"; do
            if [ "$candidate" = "$existing" ]; then
                duplicate=1
                break
            fi
        done
    fi
    if [ "$duplicate" -eq 0 ]; then
        UNIQUE_TAGS+=("$candidate")
    fi
done
TAGS=("${UNIQUE_TAGS[@]}")

if [ "$PUSH" -eq 1 ]; then
    for tag in "${TAGS[@]}"; do
        if [ "$tag" = "latest" ]; then
            echo "Refusing to push the mutable :latest tag; publish the version and version-commit tags instead." >&2
            exit 2
        fi
    done
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
            echo "+ aws ecr describe-repositories --region $ecr_region --repository-names $ecr_repo_name || aws ecr create-repository --region $ecr_region --repository-name $ecr_repo_name --image-tag-mutability IMMUTABLE --image-scanning-configuration scanOnPush=true"
            echo "+ aws ecr put-image-tag-mutability --region $ecr_region --repository-name $ecr_repo_name --image-tag-mutability IMMUTABLE"
            echo "+ aws ecr put-image-scanning-configuration --region $ecr_region --repository-name $ecr_repo_name --image-scanning-configuration scanOnPush=true"
        elif ! aws ecr describe-repositories --region "$ecr_region" --repository-names "$ecr_repo_name" >/dev/null 2>&1; then
            run_cmd aws ecr create-repository \
                --region "$ecr_region" \
                --repository-name "$ecr_repo_name" \
                --image-tag-mutability IMMUTABLE \
                --image-scanning-configuration scanOnPush=true
        else
            run_cmd aws ecr put-image-tag-mutability \
                --region "$ecr_region" \
                --repository-name "$ecr_repo_name" \
                --image-tag-mutability IMMUTABLE
            run_cmd aws ecr put-image-scanning-configuration \
                --region "$ecr_region" \
                --repository-name "$ecr_repo_name" \
                --image-scanning-configuration scanOnPush=true
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
if [ "$VERSION_TAG" != "$VERSION" ]; then
    echo "Version tag: $VERSION_TAG (Docker-safe projection of SemVer build metadata)"
fi
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
