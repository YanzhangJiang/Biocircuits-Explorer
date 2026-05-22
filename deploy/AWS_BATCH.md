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

The script is re-runnable: it prunes old non-default versions of the submitter
policy before publishing a new one, and registers the job definition with
`executionRoleArn` set so ECR image pulls and CloudWatch log delivery succeed
on both EC2-launch and Fargate-launch Batch compute environments.

If you already know the EC2 instance role that runs the website backend, pass
`--attach-to-role <role>` so the script attaches the submitter policy to it
for you:

```bash
deploy/setup_aws_batch.sh \
  --region us-west-2 \
  --image 234270344246.dkr.ecr.us-west-2.amazonaws.com/biocircuits-explorer:latest \
  --attach-to-role biocircuits-explorer-web-instance-role
```

### 2.1 Add user identity + per-user quotas (SaaS rollout)

For a public-facing deployment add Cognito + DynamoDB to the same setup call:

```bash
deploy/setup_aws_batch.sh \
  --region us-west-2 \
  --image 234270344246.dkr.ecr.us-west-2.amazonaws.com/biocircuits-explorer:latest \
  --attach-to-role biocircuits-explorer-web-instance-role \
  --with-cognito \
  --cognito-callback-urls 'https://app.yourdomain.com/auth/callback,biocircuitsexplorer://auth/callback' \
  --cognito-logout-urls   'https://app.yourdomain.com/auth/logout' \
  --cognito-domain-prefix biocircuits-explorer \
  --with-quota-table
```

What this creates in your account, on top of the Batch resources:

| Resource | Default name | Purpose |
|---|---|---|
| Cognito User Pool | `biocircuits-explorer-users` | Email-based sign-up / sign-in, MFA-ready, password policy MinLength=10 |
| App Client (public, no secret, PKCE) | `biocircuits-explorer-client` | Used by the web SPA and the macOS Swift app |
| User Pool Domain | `biocircuits-explorer.auth.<region>.amazoncognito.com` | Hosted UI for sign-in and OAuth code flow |
| DynamoDB table | `biocircuits-explorer-quotas` | Per-user daily submission counter, PAY_PER_REQUEST, TTL on `expires_at` |

The submitter policy gets `dynamodb:UpdateItem/GetItem/PutItem` automatically
when `--with-quota-table` is set, scoped to that one table.

After the script finishes, `deploy/aws-runtime.env` will include:

```text
BIOCIRCUITS_EXPLORER_COGNITO_REGION=us-west-2
BIOCIRCUITS_EXPLORER_COGNITO_USER_POOL_ID=us-west-2_xxxxxxxxx
BIOCIRCUITS_EXPLORER_COGNITO_APP_CLIENT_ID=xxxxxxxxxxxxxxxxxxxxxxxxxx
BIOCIRCUITS_EXPLORER_COGNITO_DOMAIN=biocircuits-explorer.auth.us-west-2.amazoncognito.com
BIOCIRCUITS_EXPLORER_QUOTA_TABLE=biocircuits-explorer-quotas
```

Once these variables are present in `aws-runtime.env`, **the next `deploy.sh`
run flips the backend into "JWT-required" mode**: every `POST /api/jobs` call
must carry `Authorization: Bearer <Cognito JWT>` or the request is rejected
with 400 "Missing Authorization Bearer token". The frontend will integrate the
Cognito Hosted UI sign-in flow in a follow-up commit.

To raise / lower the daily per-user quota set
`BIOCIRCUITS_EXPLORER_QUOTA_DAILY_LIMIT=<n>` in `aws-runtime.env` (default 50).

### 2.2 Callback URLs you must register

The frontend Cognito flow lives at `/auth-callback.html` on whatever origin
serves the SPA. Make sure `--cognito-callback-urls` covers every origin you
intend to run from:

| Surface | URL to register |
|---|---|
| Production web | `https://app.yourdomain.com/auth-callback.html` |
| Local web dev  | `http://localhost:8088/auth-callback.html` |
| Swift macOS WebView | `http://127.0.0.1:18088/auth-callback.html` (default port from `BiocircuitsBackendController.swift`) |

Cognito accepts comma-separated values:

```bash
--cognito-callback-urls 'https://app.yourdomain.com/auth-callback.html,http://127.0.0.1:18088/auth-callback.html,http://localhost:8088/auth-callback.html'
```

The Swift macOS app **does not** need any native code change for sign-in —
the WebView runs `auth.js` exactly like the browser and stores tokens in its
own `localStorage`. If later you want native ASWebAuthenticationSession +
Keychain on macOS, that becomes a Swift-side follow-up.

The script writes:

```text
deploy/aws-runtime.env
```

That file contains the runtime variables used by both the website backend and
the macOS local backend.

## 3. Connect The EC2 Website Backend

Three things must be true for the in-container Julia backend to reach AWS:

**a) The submitter policy is attached to the EC2 instance role.** Either pass
`--attach-to-role` to `setup_aws_batch.sh` (preferred), or attach it manually
in the IAM console. The script prints the policy ARN, usually:

```text
arn:aws:iam::<account>:policy/biocircuits-explorer-submitter-policy
```

**b) The EC2 instance metadata hop-limit is at least 2.** By default EC2
ships with `HttpPutResponseHopLimit=1`, which prevents containers (one hop
behind the host network) from reading instance-role credentials from IMDSv2.
Run once per host:

```bash
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
aws ec2 modify-instance-metadata-options \
  --instance-id "$INSTANCE_ID" \
  --http-endpoint enabled \
  --http-put-response-hop-limit 2 \
  --http-tokens required
```

If you cannot raise the hop-limit, set `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` (or `AWS_PROFILE` plus a mounted
`~/.aws`) in `deploy/aws-runtime.env` — `docker-compose.yml` already passes
those variables into the `julia-app` container.

**c) The runtime env is in place.** On the EC2 host:

```bash
cd /opt/Biocircuits-Explorer/deploy
cp aws-runtime.env.example aws-runtime.env
# or copy the generated deploy/aws-runtime.env from the setup machine
sudo -E ./deploy.sh
```

If `BIOCIRCUITS_EXPLORER_IMAGE` is set, `deploy.sh` logs Docker into ECR, pulls
that image, and starts the backend behind Nginx. If it is not set, it builds
the image locally.

To verify the chain end-to-end after `deploy.sh`:

```bash
sudo docker compose exec julia-app aws sts get-caller-identity
sudo docker compose exec julia-app aws s3 ls "$BIOCIRCUITS_EXPLORER_AWS_BATCH_ARTIFACT_PREFIX/"
sudo docker compose exec julia-app aws batch describe-job-queues \
  --job-queues "$BIOCIRCUITS_EXPLORER_AWS_BATCH_JOB_QUEUE"
```

All three must succeed — if the first one returns "Unable to locate
credentials" the hop-limit fix above hasn't been applied (or the policy isn't
attached to the right role).

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
BIOCIRCUITS_EXPLORER_AWS_BATCH_DESCRIBE_MIN_INTERVAL   # default 3 (seconds)
BIOCIRCUITS_EXPLORER_ALLOW_AWS_BATCH_REQUEST_CONFIG    # set to 1 to let the
                                                       # UI choose vcpus,
                                                       # memory, queue, etc.
```
