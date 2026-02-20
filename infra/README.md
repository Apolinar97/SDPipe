# Infra Setup (GitHub OIDC + SAM Deploy)

This folder contains:
- `bootstrap/github-oidc-bootstrap.yaml`: IAM/OIDC bootstrap stack for GitHub Actions.
- Active workflow file: `.github/workflows/deploy-sam.yaml`.

## 1) Bootstrap AWS OIDC role

Deploy the bootstrap stack once per AWS account:

```bash
aws cloudformation deploy \
  --template-file infra/bootstrap/github-oidc-bootstrap.yaml \
  --stack-name sdpipe-github-oidc-bootstrap \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    GitHubOrg=<your-github-org-or-username> \
    GitHubRepo=SDPipe \
    BranchName=main \
    DeployRoleName=GitHubActionsSamDeployRole
```

Fetch the role ARN output:

```bash
aws cloudformation describe-stacks \
  --stack-name sdpipe-github-oidc-bootstrap \
  --query "Stacks[0].Outputs[?OutputKey=='DeployRoleArn'].OutputValue" \
  --output text
```

Set that value in GitHub repository secret:
- `AWS_DEPLOY_ROLE_ARN`

## 2) Configure required GitHub repository variables

Required:
- `WEATHER_BUCKET_NAME`

Optional:
- `MAPPING_FILE_KEY` (default: `beat_station_mapping_V1.json`)
- `TEMP_OBSERVATION_FILE_PREFIX` (default: `nws_observations`)
- `LAMBDA_LOG_LEVEL` (default: `INFO`)
- `SCHEDULE_EXPRESSION` (default: `rate(1 hour)`)
- `SCHEDULE_ENABLED` (default: `true`)

## 3) Deploy with GitHub Actions

Push to `main` or run workflow manually:
- Workflow: `deploy-sam`
- File: `.github/workflows/deploy-sam.yaml`

## 4) Local SAM deploy option

```bash
sam build --template-file template.yaml
sam deploy \
  --stack-name sdpipe-weather-dev \
  --region us-west-2 \
  --capabilities CAPABILITY_IAM \
  --resolve-s3 \
  --parameter-overrides \
    EnvironmentName=dev \
    WeatherBucketName=<bucket-name>
```
