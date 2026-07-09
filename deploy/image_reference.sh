#!/usr/bin/env bash

# Accept either a content digest or the version-plus-commit tag emitted by
# deploy/build_image.sh. A commit-qualified tag is only overwrite-proof when the
# registry enforces immutable tags; build_image.sh does so for repositories it
# creates. Prefer a digest when consuming an independently managed registry.
require_release_image_reference() {
    local reference="$1"
    local label="${2:---image}"
    local leaf="${reference##*/}"
    local tag=""
    local numeric_identifier='(0|[1-9][0-9]*)'
    local prerelease_identifier='(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)'
    local build_identifier='[0-9A-Za-z-]+'
    local version_tag_regex="^${numeric_identifier}\\.${numeric_identifier}\\.${numeric_identifier}(-${prerelease_identifier}(\\.${prerelease_identifier})*)?(_${build_identifier}(\\.${build_identifier})*)?-[0-9a-f]{12,40}$"

    if [[ "$reference" =~ @sha256:[0-9a-f]{64}$ ]]; then
        return 0
    fi
    if [[ "$leaf" == *:* ]]; then
        tag="${leaf##*:}"
    fi
    if [[ "$tag" =~ $version_tag_regex ]]; then
        return 0
    fi

    echo "$label must use a sha256 digest or a version-commit tag emitted by deploy/build_image.sh: $reference" >&2
    return 2
}
