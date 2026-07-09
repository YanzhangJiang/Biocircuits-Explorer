#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=deploy/image_reference.sh
source "$ROOT_DIR/deploy/image_reference.sh"

NAME="${BIOCIRCUITS_EXPLORER_AWS_STACK_NAME:-biocircuits-explorer}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-}}"
BUCKET="${BIOCIRCUITS_EXPLORER_AWS_BUCKET:-}"
ARTIFACT_KEY_PREFIX="${BIOCIRCUITS_EXPLORER_AWS_ARTIFACT_KEY_PREFIX:-jobs}"
IMAGE="${BIOCIRCUITS_EXPLORER_IMAGE:-}"
MAX_VCPUS="${BIOCIRCUITS_EXPLORER_AWS_BATCH_MAX_VCPUS:-16}"
JOB_VCPUS="${BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_VCPUS:-4}"
JOB_MEMORY_MIB="${BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_MEMORY_MIB:-8192}"
SUBNETS="${BIOCIRCUITS_EXPLORER_AWS_BATCH_SUBNETS:-}"
SECURITY_GROUPS="${BIOCIRCUITS_EXPLORER_AWS_BATCH_SECURITY_GROUPS:-}"
INSTANCE_TYPES="${BIOCIRCUITS_EXPLORER_AWS_BATCH_INSTANCE_TYPES:-optimal}"
JOB_QUEUE="${BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE:-}"
JOB_DEFINITION="${BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION:-}"
COMPUTE_ENVIRONMENT="${BIOCIRCUITS_EXPLORER_AWS_BATCH_COMPUTE_ENVIRONMENT:-}"
OUTPUT_ENV_FILE="${BIOCIRCUITS_EXPLORER_AWS_OUTPUT_ENV_FILE:-$ROOT_DIR/deploy/aws-runtime.env}"
ATTACH_TO_ROLE="${BIOCIRCUITS_EXPLORER_AWS_ATTACH_SUBMITTER_TO_ROLE:-}"
WITH_COGNITO=0
WITH_QUOTA_TABLE=0
COGNITO_USER_POOL_NAME="${BIOCIRCUITS_EXPLORER_COGNITO_USER_POOL_NAME:-}"
COGNITO_CLIENT_NAME="${BIOCIRCUITS_EXPLORER_COGNITO_CLIENT_NAME:-}"
COGNITO_DOMAIN_PREFIX="${BIOCIRCUITS_EXPLORER_COGNITO_DOMAIN_PREFIX:-}"
COGNITO_CALLBACK_URLS="${BIOCIRCUITS_EXPLORER_COGNITO_CALLBACK_URLS:-}"
COGNITO_LOGOUT_URLS="${BIOCIRCUITS_EXPLORER_COGNITO_LOGOUT_URLS:-}"
QUOTA_TABLE_NAME="${BIOCIRCUITS_EXPLORER_QUOTA_TABLE_NAME:-}"
DRY_RUN=0
SKIP_COMPUTE=0
WAIT=1

usage() {
    cat <<'USAGE'
Usage:
  deploy/setup_aws_batch.sh [options]

Creates the AWS resources needed by Biocircuits Explorer cloud jobs:
S3 artifact bucket, IAM roles, CloudWatch log group, AWS Batch compute
environment, job queue, and job definition.

Options:
  --name <name>              Resource name prefix. Default: biocircuits-explorer.
  --region <region>          AWS region. Defaults to AWS_REGION/AWS_DEFAULT_REGION/aws config.
  --bucket <bucket>          S3 bucket. Default: <name>-<account>-<region>.
  --artifact-prefix <prefix> S3 key prefix. Default: jobs.
  --image <uri:tag|digest>   Digest or version-commit worker image. Required
                             unless --skip-compute.
  --max-vcpus <n>            Max AWS Batch EC2 vCPUs. Default: 16.
  --job-vcpus <n>            Default job vCPUs. Default: 4.
  --job-memory-mib <n>       Default job memory. Default: 8192.
  --subnets <csv>            Subnet IDs. Default: all subnets in the default VPC.
  --security-groups <csv>    Security group IDs. Default: default SG in the default VPC.
  --instance-types <csv>     Batch EC2 instance types. Default: optimal.
  --output-env <path>        Write runtime env file. Default: deploy/aws-runtime.env.
  --attach-to-role <role>    Attach the generated submitter policy to this
                             IAM role (typically the EC2 instance role of the
                             website host). Default: do not attach.
  --with-cognito             Provision a Cognito User Pool, App Client, and
                             User Pool Domain for end-user sign-up / sign-in.
  --cognito-domain-prefix <p>
                             Hosted UI subdomain prefix (must be globally
                             unique across AWS). Default: <name>-<account>.
  --cognito-callback-urls <csv>
                             Comma-separated OAuth callback URLs (https only,
                             plus biocircuitsexplorer:// for the macOS app).
                             Required with --with-cognito.
  --cognito-logout-urls <csv>
                             Comma-separated OAuth logout origins. Required
                             with --with-cognito; must match window.location.origin.
  --with-quota-table         Provision a DynamoDB on-demand table for
                             per-user submission quotas.
  --quota-table-name <name>  Override the quota table name.
                             Default: <name>-quotas.
  --skip-compute             Only create S3/IAM/log resources and env file.
  --no-wait                  Do not poll the compute environment to VALID.
  --dry-run                  Print actions without changing AWS resources.
  -h, --help                 Show this help.

The caller needs setup permissions. A starter IAM policy is provided at:
  deploy/aws_setup_permissions_policy.json
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

aws_text() {
    aws "$@" --output text
}

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "$1 is required" >&2
        exit 1
    fi
}

require_option_value() {
    if [ "$#" -lt 2 ] || [ -z "$2" ] || [[ "$2" == --* ]]; then
        echo "Option $1 requires a value." >&2
        usage >&2
        exit 2
    fi
}

csv_to_json_array() {
    local csv="$1"
    local out="["
    local first=1
    local item
    IFS=',' read -r -a items <<< "$csv"
    for item in "${items[@]}"; do
        item="$(printf '%s' "$item" | xargs)"
        [ -z "$item" ] && continue
        if [ "$first" -eq 0 ]; then
            out+=","
        fi
        out+="\"$item\""
        first=0
    done
    out+="]"
    printf '%s' "$out"
}

words_to_csv() {
    local out=""
    local first=1
    local item
    for item in $1; do
        [ "$item" = "None" ] && continue
        if [ "$first" -eq 0 ]; then
            out+=","
        fi
        out+="$item"
        first=0
    done
    printf '%s' "$out"
}

json_escape() {
    printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --name)
            require_option_value "$@"
            NAME="$2"
            shift 2
            ;;
        --region)
            require_option_value "$@"
            REGION="$2"
            shift 2
            ;;
        --bucket)
            require_option_value "$@"
            BUCKET="$2"
            shift 2
            ;;
        --artifact-prefix)
            require_option_value "$@"
            ARTIFACT_KEY_PREFIX="$2"
            shift 2
            ;;
        --image)
            require_option_value "$@"
            IMAGE="$2"
            shift 2
            ;;
        --max-vcpus)
            require_option_value "$@"
            MAX_VCPUS="$2"
            shift 2
            ;;
        --job-vcpus)
            require_option_value "$@"
            JOB_VCPUS="$2"
            shift 2
            ;;
        --job-memory-mib)
            require_option_value "$@"
            JOB_MEMORY_MIB="$2"
            shift 2
            ;;
        --subnets)
            require_option_value "$@"
            SUBNETS="$2"
            shift 2
            ;;
        --security-groups)
            require_option_value "$@"
            SECURITY_GROUPS="$2"
            shift 2
            ;;
        --instance-types)
            require_option_value "$@"
            INSTANCE_TYPES="$2"
            shift 2
            ;;
        --output-env)
            require_option_value "$@"
            OUTPUT_ENV_FILE="$2"
            shift 2
            ;;
        --attach-to-role)
            require_option_value "$@"
            ATTACH_TO_ROLE="$2"
            shift 2
            ;;
        --with-cognito)
            WITH_COGNITO=1
            shift
            ;;
        --cognito-domain-prefix)
            require_option_value "$@"
            COGNITO_DOMAIN_PREFIX="$2"
            shift 2
            ;;
        --cognito-callback-urls)
            require_option_value "$@"
            COGNITO_CALLBACK_URLS="$2"
            shift 2
            ;;
        --cognito-logout-urls)
            require_option_value "$@"
            COGNITO_LOGOUT_URLS="$2"
            shift 2
            ;;
        --with-quota-table)
            WITH_QUOTA_TABLE=1
            shift
            ;;
        --quota-table-name)
            require_option_value "$@"
            QUOTA_TABLE_NAME="$2"
            shift 2
            ;;
        --skip-compute)
            SKIP_COMPUTE=1
            shift
            ;;
        --no-wait)
            WAIT=0
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

require_cmd aws
require_cmd python3

if [ -z "$REGION" ]; then
    REGION="$(aws configure get region || true)"
fi
if [ -z "$REGION" ]; then
    REGION="us-west-2"
fi
export AWS_REGION="$REGION"
export AWS_DEFAULT_REGION="$REGION"

ACCOUNT_ID="$(aws_text sts get-caller-identity --query Account)"
if [ -z "$ACCOUNT_ID" ] || [ "$ACCOUNT_ID" = "None" ]; then
    echo "Unable to resolve AWS account id." >&2
    exit 1
fi

if [ -z "$BUCKET" ]; then
    BUCKET="${NAME}-${ACCOUNT_ID}-${REGION}"
fi

ARTIFACT_KEY_PREFIX="${ARTIFACT_KEY_PREFIX#/}"
ARTIFACT_KEY_PREFIX="${ARTIFACT_KEY_PREFIX%/}"
ARTIFACT_PREFIX="s3://${BUCKET}/${ARTIFACT_KEY_PREFIX}"

if [ -z "$IMAGE" ] && [ "$SKIP_COMPUTE" -eq 0 ]; then
    echo "--image <uri:version-commit|uri@sha256:digest> is required unless --skip-compute is set." >&2
    exit 2
fi
if [ -n "$IMAGE" ]; then
    require_release_image_reference "$IMAGE" "--image"
fi

JOB_QUEUE="${JOB_QUEUE:-${NAME}-queue}"
JOB_DEFINITION="${JOB_DEFINITION:-${NAME}-worker}"
COMPUTE_ENVIRONMENT="${COMPUTE_ENVIRONMENT:-${NAME}-ce}"
LOG_GROUP="/aws/batch/${NAME}"
ECS_INSTANCE_ROLE="${NAME}-ecs-instance-role"
ECS_INSTANCE_PROFILE="${NAME}-ecs-instance-profile"
JOB_ROLE="${NAME}-batch-job-role"
SUBMITTER_POLICY="${NAME}-submitter-policy"
COGNITO_USER_POOL_NAME="${COGNITO_USER_POOL_NAME:-${NAME}-users}"
COGNITO_CLIENT_NAME="${COGNITO_CLIENT_NAME:-${NAME}-client}"
COGNITO_DOMAIN_PREFIX="${COGNITO_DOMAIN_PREFIX:-${NAME}-${ACCOUNT_ID}}"
QUOTA_TABLE_NAME="${QUOTA_TABLE_NAME:-${NAME}-quotas}"
COGNITO_USER_POOL_ID=""
COGNITO_APP_CLIENT_ID=""

echo "AWS region:        $REGION"
echo "AWS account:       $ACCOUNT_ID"
echo "Image:             ${IMAGE:-<not configured; compute skipped>}"
echo "S3 artifact prefix:$ARTIFACT_PREFIX"
echo "Batch queue:       $JOB_QUEUE"
echo "Job definition:    $JOB_DEFINITION"
echo "Compute env:       $COMPUTE_ENVIRONMENT"

ensure_bucket() {
    if [ "$DRY_RUN" -eq 1 ]; then
        if [ "$REGION" = "us-east-1" ]; then
            run_cmd aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
        else
            run_cmd aws s3api create-bucket \
                --bucket "$BUCKET" \
                --region "$REGION" \
                --create-bucket-configuration "LocationConstraint=$REGION"
        fi
        run_cmd aws s3api put-public-access-block \
            --bucket "$BUCKET" \
            --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
        run_cmd aws s3api put-bucket-encryption \
            --bucket "$BUCKET" \
            --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
        return
    fi

    if aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
        echo "S3 bucket exists: $BUCKET"
    else
        if [ "$REGION" = "us-east-1" ]; then
            run_cmd aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
        else
            run_cmd aws s3api create-bucket \
                --bucket "$BUCKET" \
                --region "$REGION" \
                --create-bucket-configuration "LocationConstraint=$REGION"
        fi
    fi

    run_cmd aws s3api put-public-access-block \
        --bucket "$BUCKET" \
        --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    run_cmd aws s3api put-bucket-encryption \
        --bucket "$BUCKET" \
        --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
}

assume_role_policy_file() {
    local service="$1"
    local path="$2"
    cat > "$path" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "$service" },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

ensure_role() {
    local role_name="$1"
    local service="$2"
    local assume_file
    assume_file="$(mktemp)"
    assume_role_policy_file "$service" "$assume_file"
    if [ "$DRY_RUN" -eq 1 ]; then
        run_cmd aws iam create-role \
            --role-name "$role_name" \
            --assume-role-policy-document "file://$assume_file"
        rm -f "$assume_file"
        return
    fi
    if aws iam get-role --role-name "$role_name" >/dev/null 2>&1; then
        echo "IAM role exists: $role_name"
    else
        run_cmd aws iam create-role \
            --role-name "$role_name" \
            --assume-role-policy-document "file://$assume_file"
    fi
    rm -f "$assume_file"
}

ensure_instance_profile() {
    if [ "$DRY_RUN" -eq 1 ]; then
        run_cmd aws iam create-instance-profile --instance-profile-name "$ECS_INSTANCE_PROFILE"
        run_cmd aws iam add-role-to-instance-profile \
            --instance-profile-name "$ECS_INSTANCE_PROFILE" \
            --role-name "$ECS_INSTANCE_ROLE"
        return
    fi

    if aws iam get-instance-profile --instance-profile-name "$ECS_INSTANCE_PROFILE" >/dev/null 2>&1; then
        echo "IAM instance profile exists: $ECS_INSTANCE_PROFILE"
    else
        run_cmd aws iam create-instance-profile --instance-profile-name "$ECS_INSTANCE_PROFILE"
    fi

    local attached
    attached="$(aws_text iam get-instance-profile \
        --instance-profile-name "$ECS_INSTANCE_PROFILE" \
        --query "InstanceProfile.Roles[?RoleName=='$ECS_INSTANCE_ROLE'].RoleName" 2>/dev/null || true)"
    if [ "$attached" = "$ECS_INSTANCE_ROLE" ]; then
        echo "Role already in instance profile: $ECS_INSTANCE_ROLE"
    else
        run_cmd aws iam add-role-to-instance-profile \
            --instance-profile-name "$ECS_INSTANCE_PROFILE" \
            --role-name "$ECS_INSTANCE_ROLE"
    fi
}

put_job_role_policy() {
    local policy_file
    policy_file="$(mktemp)"
    # The worker container reads input.json and writes status.json /
    # result.json under ${ARTIFACT_KEY_PREFIX}/users/<sub>/jobs/<id>/. The
    # wildcard below already covers that layout.
    cat > "$policy_file" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": "arn:aws:s3:::$BUCKET/$ARTIFACT_KEY_PREFIX/users/*/jobs/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::$BUCKET",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "$ARTIFACT_KEY_PREFIX",
            "$ARTIFACT_KEY_PREFIX/*",
            "$ARTIFACT_KEY_PREFIX/users/*"
          ]
        }
      }
    }
  ]
}
EOF
    run_cmd aws iam put-role-policy \
        --role-name "$JOB_ROLE" \
        --policy-name "${NAME}-s3-artifacts" \
        --policy-document "file://$policy_file"
    rm -f "$policy_file"
}

ensure_submitter_policy() {
    local policy_arn="arn:aws:iam::${ACCOUNT_ID}:policy/${SUBMITTER_POLICY}"
    local job_role_arn
    if [ "$DRY_RUN" -eq 1 ]; then
        job_role_arn="arn:aws:iam::${ACCOUNT_ID}:role/${JOB_ROLE}"
    else
        job_role_arn="$(aws_text iam get-role --role-name "$JOB_ROLE" --query 'Role.Arn')"
    fi
    local policy_file
    policy_file="$(mktemp)"
    cat > "$policy_file" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "batch:SubmitJob",
        "batch:DescribeJobs",
        "batch:CancelJob",
        "batch:TerminateJob"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "$job_role_arn"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": "arn:aws:s3:::$BUCKET/$ARTIFACT_KEY_PREFIX/users/*/jobs/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::$BUCKET",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "$ARTIFACT_KEY_PREFIX",
            "$ARTIFACT_KEY_PREFIX/*",
            "$ARTIFACT_KEY_PREFIX/users/*"
          ]
        }
      }
    },
    {
      "Effect": "Allow",
      "Action": [
        "ecr:BatchCheckLayerAvailability",
        "ecr:BatchGetImage",
        "ecr:GetAuthorizationToken",
        "ecr:GetDownloadUrlForLayer"
      ],
      "Resource": "*"
    }$( [ "$WITH_QUOTA_TABLE" -eq 1 ] && cat <<QUOTA
,
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:UpdateItem",
        "dynamodb:PutItem"
      ],
      "Resource": "arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/${QUOTA_TABLE_NAME}"
    }
QUOTA
)
  ]
}
EOF

    if [ "$DRY_RUN" -eq 1 ]; then
        run_cmd aws iam create-policy \
            --policy-name "$SUBMITTER_POLICY" \
            --policy-document "file://$policy_file"
    elif aws iam get-policy --policy-arn "$policy_arn" >/dev/null 2>&1; then
        echo "IAM policy exists: $policy_arn"
        # AWS allows at most 5 versions per managed policy. Prune all
        # non-default versions before creating a new one so repeated runs
        # of this script never fail with LimitExceeded.
        local old_versions
        old_versions="$(aws iam list-policy-versions \
            --policy-arn "$policy_arn" \
            --query 'Versions[?IsDefaultVersion==`false`].VersionId' \
            --output text 2>/dev/null || true)"
        for old_version in $old_versions; do
            [ -z "$old_version" ] && continue
            [ "$old_version" = "None" ] && continue
            run_cmd aws iam delete-policy-version \
                --policy-arn "$policy_arn" \
                --version-id "$old_version"
        done
        run_cmd aws iam create-policy-version \
            --policy-arn "$policy_arn" \
            --policy-document "file://$policy_file" \
            --set-as-default
    else
        run_cmd aws iam create-policy \
            --policy-name "$SUBMITTER_POLICY" \
            --policy-document "file://$policy_file"
    fi
    rm -f "$policy_file"
}

attach_submitter_to_role() {
    local target_role="$1"
    [ -z "$target_role" ] && return 0
    local policy_arn="arn:aws:iam::${ACCOUNT_ID}:policy/${SUBMITTER_POLICY}"
    if [ "$DRY_RUN" -eq 1 ]; then
        run_cmd aws iam attach-role-policy \
            --role-name "$target_role" \
            --policy-arn "$policy_arn"
        return
    fi
    local already
    already="$(aws_text iam list-attached-role-policies \
        --role-name "$target_role" \
        --query "AttachedPolicies[?PolicyArn=='$policy_arn'].PolicyArn" 2>/dev/null || true)"
    if [ "$already" = "$policy_arn" ]; then
        echo "Submitter policy already attached to role: $target_role"
        return
    fi
    run_cmd aws iam attach-role-policy \
        --role-name "$target_role" \
        --policy-arn "$policy_arn"
    echo "Attached submitter policy to role: $target_role"
}

ensure_log_group() {
    if [ "$DRY_RUN" -eq 1 ]; then
        run_cmd aws logs create-log-group --region "$REGION" --log-group-name "$LOG_GROUP"
        run_cmd aws logs put-retention-policy --region "$REGION" --log-group-name "$LOG_GROUP" --retention-in-days 14
        return
    fi

    local existing
    existing="$(aws_text logs describe-log-groups \
        --region "$REGION" \
        --log-group-name-prefix "$LOG_GROUP" \
        --query "logGroups[?logGroupName=='$LOG_GROUP'].logGroupName" 2>/dev/null || true)"
    if [ "$existing" = "$LOG_GROUP" ]; then
        echo "CloudWatch log group exists: $LOG_GROUP"
    else
        run_cmd aws logs create-log-group --region "$REGION" --log-group-name "$LOG_GROUP"
    fi
    run_cmd aws logs put-retention-policy --region "$REGION" --log-group-name "$LOG_GROUP" --retention-in-days 14
}

discover_network() {
    if [ -n "$SUBNETS" ] && [ -n "$SECURITY_GROUPS" ]; then
        return
    fi
    if [ "$DRY_RUN" -eq 1 ]; then
        SUBNETS="${SUBNETS:-subnet-example1,subnet-example2}"
        SECURITY_GROUPS="${SECURITY_GROUPS:-sg-example}"
        return
    fi
    local vpc_id
    vpc_id="$(aws_text ec2 describe-vpcs \
        --region "$REGION" \
        --filters Name=is-default,Values=true \
        --query 'Vpcs[0].VpcId')"
    if [ -z "$vpc_id" ] || [ "$vpc_id" = "None" ]; then
        echo "No default VPC found. Pass --subnets and --security-groups explicitly." >&2
        exit 1
    fi

    if [ -z "$SUBNETS" ]; then
        local subnet_words
        subnet_words="$(aws_text ec2 describe-subnets \
            --region "$REGION" \
            --filters "Name=vpc-id,Values=$vpc_id" \
            --query 'Subnets[].SubnetId')"
        SUBNETS="$(words_to_csv "$subnet_words")"
    fi
    if [ -z "$SECURITY_GROUPS" ]; then
        SECURITY_GROUPS="$(aws_text ec2 describe-security-groups \
            --region "$REGION" \
            --filters "Name=vpc-id,Values=$vpc_id" "Name=group-name,Values=default" \
            --query 'SecurityGroups[0].GroupId')"
    fi
}

ensure_compute_environment() {
    discover_network
    if [ "$DRY_RUN" -eq 1 ]; then
        run_cmd aws iam create-service-linked-role --aws-service-name batch.amazonaws.com
        local dry_profile_arn="arn:aws:iam::${ACCOUNT_ID}:instance-profile/${ECS_INSTANCE_PROFILE}"
        local dry_compute_file
        dry_compute_file="$(mktemp)"
        cat > "$dry_compute_file" <<EOF
{
  "computeEnvironmentName": "$COMPUTE_ENVIRONMENT",
  "type": "MANAGED",
  "state": "ENABLED",
  "computeResources": {
    "type": "EC2",
    "allocationStrategy": "BEST_FIT_PROGRESSIVE",
    "minvCpus": 0,
    "maxvCpus": $MAX_VCPUS,
    "desiredvCpus": 0,
    "instanceTypes": $(csv_to_json_array "$INSTANCE_TYPES"),
    "subnets": $(csv_to_json_array "$SUBNETS"),
    "securityGroupIds": $(csv_to_json_array "$SECURITY_GROUPS"),
    "instanceRole": "$dry_profile_arn"
  }
}
EOF
        run_cmd aws batch create-compute-environment \
            --region "$REGION" \
            --cli-input-json "file://$dry_compute_file"
        rm -f "$dry_compute_file"
        return
    fi

    local profile_arn
    profile_arn="$(aws_text iam get-instance-profile \
        --instance-profile-name "$ECS_INSTANCE_PROFILE" \
        --query 'InstanceProfile.Arn')"

    local existing
    existing="$(aws_text batch describe-compute-environments \
        --region "$REGION" \
        --compute-environments "$COMPUTE_ENVIRONMENT" \
        --query 'computeEnvironments[0].computeEnvironmentArn' 2>/dev/null || true)"
    if [ -n "$existing" ] && [ "$existing" != "None" ]; then
        local existing_file
        existing_file="$(mktemp)"
        aws batch describe-compute-environments \
            --region "$REGION" \
            --compute-environments "$COMPUTE_ENVIRONMENT" \
            --output json > "$existing_file"
        if ! python3 "$ROOT_DIR/deploy/validate_aws_batch_state.py" compute "$existing_file" \
            --environment-type MANAGED \
            --compute-type EC2 \
            --allocation-strategy BEST_FIT_PROGRESSIVE \
            --min-vcpus 0 \
            --max-vcpus "$MAX_VCPUS" \
            --instance-role "$profile_arn" \
            --subnets "$SUBNETS" \
            --security-groups "$SECURITY_GROUPS" \
            --instance-types "$INSTANCE_TYPES"; then
            rm -f "$existing_file"
            exit 1
        fi
        rm -f "$existing_file"
        echo "AWS Batch compute environment exists and matches the requested contract: $COMPUTE_ENVIRONMENT"
        return
    fi

    run_cmd aws iam create-service-linked-role --aws-service-name batch.amazonaws.com || true

    local compute_file
    compute_file="$(mktemp)"
    cat > "$compute_file" <<EOF
{
  "computeEnvironmentName": "$COMPUTE_ENVIRONMENT",
  "type": "MANAGED",
  "state": "ENABLED",
  "computeResources": {
    "type": "EC2",
    "allocationStrategy": "BEST_FIT_PROGRESSIVE",
    "minvCpus": 0,
    "maxvCpus": $MAX_VCPUS,
    "desiredvCpus": 0,
    "instanceTypes": $(csv_to_json_array "$INSTANCE_TYPES"),
    "subnets": $(csv_to_json_array "$SUBNETS"),
    "securityGroupIds": $(csv_to_json_array "$SECURITY_GROUPS"),
    "instanceRole": "$profile_arn"
  }
}
EOF
    run_cmd aws batch create-compute-environment \
        --region "$REGION" \
        --cli-input-json "file://$compute_file"
    rm -f "$compute_file"
}

wait_compute_environment() {
    [ "$DRY_RUN" -eq 0 ] || return 0
    [ "$WAIT" -eq 1 ] || return 0
    [ "$SKIP_COMPUTE" -eq 0 ] || return 0
    echo "Waiting for compute environment to become VALID..."
    local status
    local reason
    for _ in $(seq 1 60); do
        status="$(aws_text batch describe-compute-environments \
            --region "$REGION" \
            --compute-environments "$COMPUTE_ENVIRONMENT" \
            --query 'computeEnvironments[0].status' 2>/dev/null || true)"
        reason="$(aws_text batch describe-compute-environments \
            --region "$REGION" \
            --compute-environments "$COMPUTE_ENVIRONMENT" \
            --query 'computeEnvironments[0].statusReason' 2>/dev/null || true)"
        if [ "$status" = "VALID" ]; then
            echo "Compute environment is VALID."
            return
        fi
        if [ "$status" = "INVALID" ]; then
            echo "Compute environment is INVALID: $reason" >&2
            exit 1
        fi
        sleep 10
    done
    echo "Compute environment did not reach VALID within the wait window." >&2
    exit 1
}

ensure_job_queue() {
    if [ "$DRY_RUN" -eq 1 ]; then
        local dry_ce_arn="arn:aws:batch:${REGION}:${ACCOUNT_ID}:compute-environment/${COMPUTE_ENVIRONMENT}"
        run_cmd aws batch create-job-queue \
            --region "$REGION" \
            --job-queue-name "$JOB_QUEUE" \
            --state ENABLED \
            --priority 1 \
            --compute-environment-order "order=1,computeEnvironment=$dry_ce_arn"
        return
    fi

    local ce_arn
    ce_arn="$(aws_text batch describe-compute-environments \
        --region "$REGION" \
        --compute-environments "$COMPUTE_ENVIRONMENT" \
        --query 'computeEnvironments[0].computeEnvironmentArn')"

    local existing
    existing="$(aws_text batch describe-job-queues \
        --region "$REGION" \
        --job-queues "$JOB_QUEUE" \
        --query 'jobQueues[0].jobQueueArn' 2>/dev/null || true)"
    if [ -n "$existing" ] && [ "$existing" != "None" ]; then
        local existing_file
        existing_file="$(mktemp)"
        aws batch describe-job-queues \
            --region "$REGION" \
            --job-queues "$JOB_QUEUE" \
            --output json > "$existing_file"
        if ! python3 "$ROOT_DIR/deploy/validate_aws_batch_state.py" queue "$existing_file" \
            --priority 1 \
            --compute-environment "$ce_arn"; then
            rm -f "$existing_file"
            exit 1
        fi
        rm -f "$existing_file"
        echo "AWS Batch job queue exists and matches the requested contract: $JOB_QUEUE"
        return
    fi

    run_cmd aws batch create-job-queue \
        --region "$REGION" \
        --job-queue-name "$JOB_QUEUE" \
        --state ENABLED \
        --priority 1 \
        --compute-environment-order "order=1,computeEnvironment=$ce_arn"
}

wait_job_queue() {
    [ "$DRY_RUN" -eq 0 ] || return 0
    [ "$WAIT" -eq 1 ] || return 0
    [ "$SKIP_COMPUTE" -eq 0 ] || return 0
    echo "Waiting for job queue to become VALID..."
    local status
    local reason
    for _ in $(seq 1 60); do
        status="$(aws_text batch describe-job-queues \
            --region "$REGION" \
            --job-queues "$JOB_QUEUE" \
            --query 'jobQueues[0].status' 2>/dev/null || true)"
        reason="$(aws_text batch describe-job-queues \
            --region "$REGION" \
            --job-queues "$JOB_QUEUE" \
            --query 'jobQueues[0].statusReason' 2>/dev/null || true)"
        if [ "$status" = "VALID" ]; then
            echo "Job queue is VALID."
            return
        fi
        if [ "$status" = "INVALID" ]; then
            echo "Job queue is INVALID: $reason" >&2
            exit 1
        fi
        sleep 10
    done
    echo "Job queue did not reach VALID within the wait window." >&2
    exit 1
}

register_job_definition() {
    local job_role_arn
    if [ "$DRY_RUN" -eq 1 ]; then
        job_role_arn="arn:aws:iam::${ACCOUNT_ID}:role/${JOB_ROLE}"
    else
        job_role_arn="$(aws_text iam get-role --role-name "$JOB_ROLE" --query 'Role.Arn')"
    fi
    local command_json
    command_json='["julia","-t","auto","--project=webapp","webapp/scripts/run_batch_job.jl"]'
    local container_file
    container_file="$(mktemp)"
    cat > "$container_file" <<EOF
{
  "image": "$(json_escape "$IMAGE")",
  "jobRoleArn": "$job_role_arn",
  "executionRoleArn": "$job_role_arn",
  "command": $command_json,
  "resourceRequirements": [
    { "type": "VCPU", "value": "$JOB_VCPUS" },
    { "type": "MEMORY", "value": "$JOB_MEMORY_MIB" }
  ],
  "environment": [
    { "name": "AWS_REGION", "value": "$REGION" },
    { "name": "AWS_DEFAULT_REGION", "value": "$REGION" }
  ],
  "logConfiguration": {
    "logDriver": "awslogs",
    "options": {
      "awslogs-group": "$LOG_GROUP",
      "awslogs-region": "$REGION",
      "awslogs-stream-prefix": "worker"
    }
  }
}
EOF
    run_cmd aws batch register-job-definition \
        --region "$REGION" \
        --job-definition-name "$JOB_DEFINITION" \
        --type container \
        --container-properties "file://$container_file"
    rm -f "$container_file"
}

ensure_cognito() {
    [ "$WITH_COGNITO" -eq 1 ] || return 0
    if [ -z "$COGNITO_CALLBACK_URLS" ]; then
        echo "--with-cognito requires --cognito-callback-urls <csv>" >&2
        exit 2
    fi
    if [ -z "$COGNITO_LOGOUT_URLS" ]; then
        echo "--with-cognito requires --cognito-logout-urls <csv> matching the frontend origins" >&2
        exit 2
    fi

    if [ "$DRY_RUN" -eq 1 ]; then
        echo "Would create Cognito user pool: $COGNITO_USER_POOL_NAME"
        echo "Would create app client:        $COGNITO_CLIENT_NAME"
        echo "Would create domain prefix:     $COGNITO_DOMAIN_PREFIX"
        COGNITO_USER_POOL_ID="us-west-2_DRYRUN"
        COGNITO_APP_CLIENT_ID="dryrunclientid000000000000"
        return
    fi

    # Reuse if a pool with the same name already exists.
    COGNITO_USER_POOL_ID="$(aws_text cognito-idp list-user-pools \
        --region "$REGION" \
        --max-results 60 \
        --query "UserPools[?Name=='$COGNITO_USER_POOL_NAME'].Id | [0]" 2>/dev/null || true)"
    if [ -z "$COGNITO_USER_POOL_ID" ] || [ "$COGNITO_USER_POOL_ID" = "None" ]; then
        echo "Creating Cognito user pool: $COGNITO_USER_POOL_NAME"
        COGNITO_USER_POOL_ID="$(aws cognito-idp create-user-pool \
            --region "$REGION" \
            --pool-name "$COGNITO_USER_POOL_NAME" \
            --auto-verified-attributes email \
            --username-attributes email \
            --policies 'PasswordPolicy={MinimumLength=10,RequireUppercase=true,RequireLowercase=true,RequireNumbers=true,RequireSymbols=false}' \
            --account-recovery-setting 'RecoveryMechanisms=[{Priority=1,Name=verified_email}]' \
            --query 'UserPool.Id' --output text)"
        echo "Created pool: $COGNITO_USER_POOL_ID"
    else
        echo "Cognito user pool exists: $COGNITO_USER_POOL_ID"
    fi

    # App client (public SPA + macOS app: no client secret, PKCE-only).
    COGNITO_APP_CLIENT_ID="$(aws_text cognito-idp list-user-pool-clients \
        --region "$REGION" \
        --user-pool-id "$COGNITO_USER_POOL_ID" \
        --max-results 60 \
        --query "UserPoolClients[?ClientName=='$COGNITO_CLIENT_NAME'].ClientId | [0]" 2>/dev/null || true)"
    local callbacks logouts
    callbacks="$(csv_to_json_array "$COGNITO_CALLBACK_URLS")"
    logouts="$(csv_to_json_array "$COGNITO_LOGOUT_URLS")"
    local supported_flows='["code"]'
    local supported_scopes='["openid","email","profile"]'

    if [ -z "$COGNITO_APP_CLIENT_ID" ] || [ "$COGNITO_APP_CLIENT_ID" = "None" ]; then
        echo "Creating Cognito app client: $COGNITO_CLIENT_NAME"
        COGNITO_APP_CLIENT_ID="$(aws cognito-idp create-user-pool-client \
            --region "$REGION" \
            --user-pool-id "$COGNITO_USER_POOL_ID" \
            --client-name "$COGNITO_CLIENT_NAME" \
            --no-generate-secret \
            --allowed-o-auth-flows-user-pool-client \
            --supported-identity-providers '["COGNITO"]' \
            --callback-urls "$callbacks" \
            --logout-urls "$logouts" \
            --allowed-o-auth-flows "$supported_flows" \
            --allowed-o-auth-scopes "$supported_scopes" \
            --explicit-auth-flows '["ALLOW_USER_SRP_AUTH","ALLOW_REFRESH_TOKEN_AUTH"]' \
            --prevent-user-existence-errors ENABLED \
            --query 'UserPoolClient.ClientId' --output text)"
    else
        echo "Cognito app client exists: $COGNITO_APP_CLIENT_ID"
        run_cmd aws cognito-idp update-user-pool-client \
            --region "$REGION" \
            --user-pool-id "$COGNITO_USER_POOL_ID" \
            --client-id "$COGNITO_APP_CLIENT_ID" \
            --no-generate-secret \
            --allowed-o-auth-flows-user-pool-client \
            --supported-identity-providers '["COGNITO"]' \
            --callback-urls "$callbacks" \
            --logout-urls "$logouts" \
            --allowed-o-auth-flows "$supported_flows" \
            --allowed-o-auth-scopes "$supported_scopes" \
            --explicit-auth-flows '["ALLOW_USER_SRP_AUTH","ALLOW_REFRESH_TOKEN_AUTH"]' \
            --prevent-user-existence-errors ENABLED >/dev/null
    fi

    # User pool domain (Hosted UI).
    local existing_domain
    existing_domain="$(aws_text cognito-idp describe-user-pool-domain \
        --region "$REGION" \
        --domain "$COGNITO_DOMAIN_PREFIX" \
        --query 'DomainDescription.Status' 2>/dev/null || true)"
    if [ -z "$existing_domain" ] || [ "$existing_domain" = "None" ]; then
        run_cmd aws cognito-idp create-user-pool-domain \
            --region "$REGION" \
            --user-pool-id "$COGNITO_USER_POOL_ID" \
            --domain "$COGNITO_DOMAIN_PREFIX"
    else
        echo "Cognito user pool domain exists: $COGNITO_DOMAIN_PREFIX ($existing_domain)"
    fi
}

ensure_quota_table() {
    [ "$WITH_QUOTA_TABLE" -eq 1 ] || return 0
    if [ "$DRY_RUN" -eq 1 ]; then
        echo "Would create DynamoDB quota table: $QUOTA_TABLE_NAME"
        return
    fi

    if aws dynamodb describe-table --region "$REGION" --table-name "$QUOTA_TABLE_NAME" >/dev/null 2>&1; then
        echo "DynamoDB quota table exists: $QUOTA_TABLE_NAME"
        return
    fi

    run_cmd aws dynamodb create-table \
        --region "$REGION" \
        --table-name "$QUOTA_TABLE_NAME" \
        --attribute-definitions \
            AttributeName=user_sub,AttributeType=S \
            AttributeName=window,AttributeType=S \
        --key-schema \
            AttributeName=user_sub,KeyType=HASH \
            AttributeName=window,KeyType=RANGE \
        --billing-mode PAY_PER_REQUEST \
        --tags Key=App,Value="$NAME"

    echo "Waiting for DynamoDB table to become ACTIVE..."
    aws dynamodb wait table-exists --region "$REGION" --table-name "$QUOTA_TABLE_NAME"

    run_cmd aws dynamodb update-time-to-live \
        --region "$REGION" \
        --table-name "$QUOTA_TABLE_NAME" \
        --time-to-live-specification "Enabled=true,AttributeName=expires_at"
}

write_runtime_env() {
    if [ "$DRY_RUN" -eq 1 ]; then
        echo "Would write runtime env file: $OUTPUT_ENV_FILE"
        return
    fi
    mkdir -p "$(dirname "$OUTPUT_ENV_FILE")"
    local runtime_job_queue="$JOB_QUEUE"
    local runtime_job_definition="$JOB_DEFINITION"
    if [ "$SKIP_COMPUTE" -eq 1 ]; then
        runtime_job_queue=""
        runtime_job_definition=""
    fi
    local temporary_env
    temporary_env="$(mktemp "${OUTPUT_ENV_FILE}.tmp.XXXXXX")"
    if [ -f "$OUTPUT_ENV_FILE" ]; then
        # Preserve operator-owned settings (TLS paths, domains, limits, etc.)
        # while replacing only the keys owned by this generator.
        grep -Ev '^(# Generated by deploy/setup_aws_batch\.sh|AWS_REGION=|AWS_DEFAULT_REGION=|BIOCIRCUITS_EXPLORER_IMAGE=|BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE=|BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION=|BIOCIRCUITS_EXPLORER_AWS_BATCH_ARTIFACT_PREFIX=|BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_NAME_PREFIX=|BIOCIRCUITS_EXPLORER_COGNITO_REGION=|BIOCIRCUITS_EXPLORER_COGNITO_USER_POOL_ID=|BIOCIRCUITS_EXPLORER_COGNITO_APP_CLIENT_ID=|BIOCIRCUITS_EXPLORER_COGNITO_DOMAIN=|BIOCIRCUITS_EXPLORER_QUOTA_TABLE=)' \
            "$OUTPUT_ENV_FILE" > "$temporary_env" || true
        printf '\n' >> "$temporary_env"
    fi
    cat >> "$temporary_env" <<EOF
# Generated by deploy/setup_aws_batch.sh
AWS_REGION=$REGION
AWS_DEFAULT_REGION=$REGION
BIOCIRCUITS_EXPLORER_IMAGE=$IMAGE
BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE=$runtime_job_queue
BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION=$runtime_job_definition
BIOCIRCUITS_EXPLORER_AWS_BATCH_ARTIFACT_PREFIX=$ARTIFACT_PREFIX
BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_NAME_PREFIX=biocircuits
EOF
    if [ -n "$COGNITO_USER_POOL_ID" ]; then
        cat >> "$temporary_env" <<EOF
BIOCIRCUITS_EXPLORER_COGNITO_REGION=$REGION
BIOCIRCUITS_EXPLORER_COGNITO_USER_POOL_ID=$COGNITO_USER_POOL_ID
BIOCIRCUITS_EXPLORER_COGNITO_APP_CLIENT_ID=$COGNITO_APP_CLIENT_ID
BIOCIRCUITS_EXPLORER_COGNITO_DOMAIN=${COGNITO_DOMAIN_PREFIX}.auth.${REGION}.amazoncognito.com
EOF
    fi
    if [ "$WITH_QUOTA_TABLE" -eq 1 ]; then
        cat >> "$temporary_env" <<EOF
BIOCIRCUITS_EXPLORER_QUOTA_TABLE=$QUOTA_TABLE_NAME
EOF
    fi
    chmod 600 "$temporary_env"
    mv -f "$temporary_env" "$OUTPUT_ENV_FILE"
    echo "Wrote runtime env file: $OUTPUT_ENV_FILE"
}

ensure_bucket
ensure_role "$ECS_INSTANCE_ROLE" ec2.amazonaws.com
run_cmd aws iam attach-role-policy \
    --role-name "$ECS_INSTANCE_ROLE" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role
ensure_instance_profile

ensure_role "$JOB_ROLE" ecs-tasks.amazonaws.com
run_cmd aws iam attach-role-policy \
    --role-name "$JOB_ROLE" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
put_job_role_policy
ensure_cognito
ensure_quota_table
ensure_submitter_policy
attach_submitter_to_role "$ATTACH_TO_ROLE"
ensure_log_group

if [ "$SKIP_COMPUTE" -eq 0 ]; then
    ensure_compute_environment
    wait_compute_environment
    ensure_job_queue
    wait_job_queue
    register_job_definition
fi

write_runtime_env

RUNTIME_JOB_QUEUE="$JOB_QUEUE"
RUNTIME_JOB_DEFINITION="$JOB_DEFINITION"
if [ "$SKIP_COMPUTE" -eq 1 ]; then
    RUNTIME_JOB_QUEUE="<not configured: --skip-compute>"
    RUNTIME_JOB_DEFINITION="<not configured: --skip-compute>"
fi

cat <<EOF

AWS Batch setup complete.

Runtime environment:
  source $OUTPUT_ENV_FILE

Submitter policy ARN:
  arn:aws:iam::${ACCOUNT_ID}:policy/${SUBMITTER_POLICY}
$( [ -n "$ATTACH_TO_ROLE" ] \
    && printf 'Already attached to role: %s' "$ATTACH_TO_ROLE" \
    || printf 'Attach it to the EC2 instance role running the website backend
(re-run with --attach-to-role <role> to do this automatically).' )

The website and macOS local backend use these variables:
  BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE=$RUNTIME_JOB_QUEUE
  BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION=$RUNTIME_JOB_DEFINITION
  BIOCIRCUITS_EXPLORER_AWS_BATCH_ARTIFACT_PREFIX=$ARTIFACT_PREFIX
EOF
