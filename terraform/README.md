
# Prerequisite
- enable compute engine API, artifact registry API

to provision:

```
terraform init        # download providers, set up backend — run once per project or after provider changes
terraform plan
terraform apply
```

to destroy:

```
terraform destroy
```

to show current state
```
terraform show                  # show current state of all resources
terraform state list            # list all resources terraform is tracking
terraform state show google_compute_instance.airflow_vm   # inspect a specific resource
```