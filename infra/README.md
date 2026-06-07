# infra

Terraform configuration for deploying GCP resources: GCS bucket and IAM bindings. The
fuel-ingestor pipeline runs in GitHub Actions (`trigger-ingestor.yaml`), not Cloud Run.

## GCP Resources

- **GCS Bucket** -- fuel price data storage (`bucket.tf`)
- **Service accounts and IAM** -- least-privilege access (CI/CD SA in `backend_support/cicd.tf`)

## Configuration

| Variable   | Description                                |
| ---------- | ------------------------------------------ |
| `PROJECT`  | GCP project ID (`travel-assistant-417315`) |
| `APP_NAME` | Application name                           |
| `REGION`   | GCP region (`europe-southwest1`)           |

Variables defined in `varibles.tf`.

## Structure

```
infra/
  backend.tf           Remote state configuration
  provider.tf          GCP provider setup
  varibles.tf          Input variables
  locals.tf            Derived values (resource prefix: travass)
  dashboard.tf         Cloud Run Service definition (commented out; dashboard runs on Raspberry Pi)
  bucket.tf            GCS bucket
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
terraform plan
terraform apply
```
