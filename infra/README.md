# infra

Terraform configuration for deploying all GCP resources: Cloud Run job, Cloud Run service, GCS bucket, service accounts, and IAM bindings.

## GCP Resources

- **Cloud Run Job** -- fuel-ingestor batch pipeline (`ingestor.tf`)
- **Cloud Run Service** -- fuel-dashboard web app (`dashboard.tf`)
- **GCS Bucket** -- fuel price data storage (`bucket.tf`)
- **Service accounts and IAM** -- least-privilege access for each service

## Configuration

| Variable | Description |
|----------|-------------|
| `PROJECT` | GCP project ID (`travel-assistant-417315`) |
| `APP_NAME` | Application name |
| `REGION` | GCP region (`europe-southwest1`) |
| `DOCKER_HUB_USERNAME` | Docker Hub username for image pulls |
| `DOCKER_IMAGE_TAG` | Image tag to deploy |

Variables defined in `varibles.tf`.

## Structure

```
infra/
  backend.tf           Remote state configuration
  provider.tf          GCP provider setup
  varibles.tf          Input variables
  locals.tf            Derived values (resource prefix: travass)
  ingestor.tf          Cloud Run Job definition
  dashboard.tf         Cloud Run Service definition
  bucket.tf            GCS bucket
  outputs.tf           Exported values
  backend_support/     Bootstrap resources
    bucket.tf          Terraform state bucket
    cicd.tf            GitHub Actions service account + IAM
    variables.tf       Bootstrap variables
    provider.tf        Provider config
    output.tf          Bootstrap outputs
```

## Backend Bootstrap

The `backend_support/` subdirectory must be applied first to create the Terraform state bucket and CI/CD service account:

```bash
make backend.init
make backend.plan
make backend.apply
```

Or all at once: `make backend.run`

## Usage

Infrastructure is normally managed via CI (`deploy.yaml`). For manual operations:

```bash
cd infra
terraform init
terraform plan -var "DOCKER_IMAGE_TAG=<tag>" ...
terraform apply
```
