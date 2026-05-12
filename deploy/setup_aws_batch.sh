#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

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
  --image <uri:tag>          Worker image. Default: current account ECR :latest.
  --max-vcpus <n>            Max AWS Batch EC2 vCPUs. Default: 16.
  --job-vcpus <n>            Default job vCPUs. Default: 4.
  --job-memory-mib <n>       Default job memory. Default: 8192.
  --subnets <csv>            Subnet IDs. Default: all subnets in the default VPC.
  --security-groups <csv>    Security group IDs. Default: default SG in the default VPC.
  --instance-types <csv>     Batch EC2 instance types. Default: optimal.
  --output-env <path>        Write runtime env file. Default: deploy/aws-runtime.env.
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
            NAME="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --bucket)
            BUCKET="$2"
            shift 2
            ;;
        --artifact-prefix)
            ARTIFACT_KEY_PREFIX="$2"
            shift 2
            ;;
        --image)
            IMAGE="$2"
            shift 2
            ;;
        --max-vcpus)
            MAX_VCPUS="$2"
            shift 2
            ;;
        --job-vcpus)
            JOB_VCPUS="$2"
            shift 2
            ;;
        --job-memory-mib)
            JOB_MEMORY_MIB="$2"
            shift 2
            ;;
        --subnets)
            SUBNETS="$2"
            shift 2
            ;;
        --security-groups)
            SECURITY_GROUPS="$2"
            shift 2
            ;;
        --instance-types)
            INSTANCE_TYPES="$2"
            shift 2
            ;;
        --output-env)
            OUTPUT_ENV_FILE="$2"
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

if [ -z "$IMAGE" ]; then
    IMAGE="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/biocircuits-explorer:latest"
fi

JOB_QUEUE="${JOB_QUEUE:-${NAME}-queue}"
JOB_DEFINITION="${JOB_DEFINITION:-${NAME}-worker}"
COMPUTE_ENVIRONMENT="${COMPUTE_ENVIRONMENT:-${NAME}-ce}"
LOG_GROUP="/aws/batch/${NAME}"
ECS_INSTANCE_ROLE="${NAME}-ecs-instance-role"
ECS_INSTANCE_PROFILE="${NAME}-ecs-instance-profile"
JOB_ROLE="${NAME}-batch-job-role"
SUBMITTER_POLICY="${NAME}-submitter-policy"

echo "AWS region:        $REGION"
echo "AWS account:       $ACCOUNT_ID"
echo "Image:             $IMAGE"
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
      "Resource": "arn:aws:s3:::$BUCKET/$ARTIFACT_KEY_PREFIX/*"
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
            "$ARTIFACT_KEY_PREFIX/*"
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
      "Resource": "arn:aws:s3:::$BUCKET/$ARTIFACT_KEY_PREFIX/*"
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
            "$ARTIFACT_KEY_PREFIX/*"
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
    }
  ]
}
EOF

    if [ "$DRY_RUN" -eq 1 ]; then
        run_cmd aws iam create-policy \
            --policy-name "$SUBMITTER_POLICY" \
            --policy-document "file://$policy_file"
    elif aws iam get-policy --policy-arn "$policy_arn" >/dev/null 2>&1; then
        echo "IAM policy exists: $policy_arn"
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

    local existing
    existing="$(aws_text batch describe-compute-environments \
        --region "$REGION" \
        --compute-environments "$COMPUTE_ENVIRONMENT" \
        --query 'computeEnvironments[0].computeEnvironmentArn' 2>/dev/null || true)"
    if [ -n "$existing" ] && [ "$existing" != "None" ]; then
        echo "AWS Batch compute environment exists: $COMPUTE_ENVIRONMENT"
        return
    fi

    run_cmd aws iam create-service-linked-role --aws-service-name batch.amazonaws.com || true

    local profile_arn
    profile_arn="$(aws_text iam get-instance-profile \
        --instance-profile-name "$ECS_INSTANCE_PROFILE" \
        --query 'InstanceProfile.Arn')"

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
    echo "Compute environment did not reach VALID within the wait window; continuing." >&2
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

    local existing
    existing="$(aws_text batch describe-job-queues \
        --region "$REGION" \
        --job-queues "$JOB_QUEUE" \
        --query 'jobQueues[0].jobQueueArn' 2>/dev/null || true)"
    if [ -n "$existing" ] && [ "$existing" != "None" ]; then
        echo "AWS Batch job queue exists: $JOB_QUEUE"
        return
    fi

    local ce_arn
    ce_arn="$(aws_text batch describe-compute-environments \
        --region "$REGION" \
        --compute-environments "$COMPUTE_ENVIRONMENT" \
        --query 'computeEnvironments[0].computeEnvironmentArn')"

    run_cmd aws batch create-job-queue \
        --region "$REGION" \
        --job-queue-name "$JOB_QUEUE" \
        --state ENABLED \
        --priority 1 \
        --compute-environment-order "order=1,computeEnvironment=$ce_arn"
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

write_runtime_env() {
    if [ "$DRY_RUN" -eq 1 ]; then
        echo "Would write runtime env file: $OUTPUT_ENV_FILE"
        return
    fi
    mkdir -p "$(dirname "$OUTPUT_ENV_FILE")"
    cat > "$OUTPUT_ENV_FILE" <<EOF
# Generated by deploy/setup_aws_batch.sh
AWS_REGION=$REGION
AWS_DEFAULT_REGION=$REGION
BIOCIRCUITS_EXPLORER_IMAGE=$IMAGE
BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE=$JOB_QUEUE
BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION=$JOB_DEFINITION
BIOCIRCUITS_EXPLORER_AWS_BATCH_ARTIFACT_PREFIX=$ARTIFACT_PREFIX
BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_NAME_PREFIX=biocircuits
EOF
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
ensure_submitter_policy
ensure_log_group

if [ "$SKIP_COMPUTE" -eq 0 ]; then
    ensure_compute_environment
    wait_compute_environment
    ensure_job_queue
    register_job_definition
fi

write_runtime_env

cat <<EOF

AWS Batch setup complete.

Runtime environment:
  source $OUTPUT_ENV_FILE

Attach this policy to the EC2 instance role that runs the website backend:
  arn:aws:iam::${ACCOUNT_ID}:policy/${SUBMITTER_POLICY}

The website and macOS local backend use these variables:
  BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE=$JOB_QUEUE
  BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION=$JOB_DEFINITION
  BIOCIRCUITS_EXPLORER_AWS_BATCH_ARTIFACT_PREFIX=$ARTIFACT_PREFIX
EOF
