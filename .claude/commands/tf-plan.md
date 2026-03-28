Validate and explain Terraform changes in the `infra/` directory.

## Steps

1. **Show what changed**: Run `git diff HEAD -- infra/` and summarize the changes.

2. **Format check**: Run `cd infra && terraform fmt -check` to verify formatting.

3. **Validate**: Run `cd infra && terraform validate` to check configuration syntax.

4. **Explain impact**: Based on the diff, explain in plain language:
   - Which GCP resources are being added, modified, or destroyed
   - Any potential downtime or service disruption
   - Whether IAM permissions are changing
   - Any cost implications (new resources, scaling changes)

5. **Summary**: Provide a clear verdict — is this safe to apply, or are there concerns?

Note: Full `terraform plan` requires GCP credentials. This command focuses on local validation and change analysis.
