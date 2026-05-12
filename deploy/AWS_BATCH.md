# AWS Batch Deployment

This project uses three AWS surfaces for cloud compute:

- ECR stores the Docker image.
- S3 stores per-job artifacts: `input.json`, `status.json`, and `result.json`.
- AWS Batch runs the same image as a worker when the UI cloud-compute toggle submits a long job.

S3 is not for image files. The image stays in ECR.

## 1. Grant Setup Permissions

The setup script needs permission to create S3, IAM, CloudWatch Logs, and AWS
Batch resources. Attach `deploy/aws_setup_permissions_policy.json` to the IAM
user/role that runs the setup.

The current ECR-only permissions are not enough; the script also needs actions
such as `ec2:DescribeVpcs`, `s3:CreateBucket`, and
`batch:CreateComputeEnvironment`.

## 2. Create The Batch Stack

After the image has been pushed to ECR:

```bash
deploy/setup_aws_batch.sh \
  --region us-west-2 \
  --image 234270344246.dkr.ecr.us-west-2.amazonaws.com/biocircuits-explorer:latest
```

By default this creates an EC2-backed managed Batch compute environment with
`minvCpus=0`, so it should not launch compute instances until jobs are queued.

The script writes:

```text
deploy/aws-runtime.env
```

That file contains the runtime variables used by both the website backend and
the macOS local backend.

## 3. Connect The EC2 Website Backend

Attach the generated submitter policy to the EC2 instance role that runs the
website. The script prints the policy ARN, usually:

```text
arn:aws:iam::<account>:policy/biocircuits-explorer-submitter-policy
```

On the EC2 host:

```bash
cd /opt/Biocircuits-Explorer/deploy
cp aws-runtime.env.example aws-runtime.env
# or copy the generated deploy/aws-runtime.env from the setup machine
sudo -E ./deploy.sh
```

If `BIOCIRCUITS_EXPLORER_IMAGE` is set, `deploy.sh` logs Docker into ECR, pulls
that image, and starts the backend behind Nginx. If it is not set, it builds
the image locally.

## 4. Connect The macOS Local Backend

The native macOS shell still talks to a local backend. To make that local
backend submit AWS Batch jobs, run it with AWS credentials and the same runtime
variables:

```bash
cd Biocircuits-Explorer
set -a
. deploy/aws-runtime.env
set +a
cd webapp
julia -t auto --project=. server.jl
```

The top-bar cloud-compute toggle in the UI controls whether supported long jobs
use `execution.mode = "aws_batch"` or run locally.

## 5. Runtime Variables

Required for cloud jobs:

```text
AWS_REGION
AWS_DEFAULT_REGION
BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE
BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_DEFINITION
BIOCIRCUITS_EXPLORER_AWS_BATCH_ARTIFACT_PREFIX
```

Optional:

```text
BIOCIRCUITS_EXPLORER_IMAGE
BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_NAME_PREFIX
BIOCIRCUITS_EXPLORER_AWS_CLI
BIOCIRCUITS_EXPLORER_JOB_STORE
```
