# aws-emr-launch
A simple application that launches AWS EMR

## Quick start
```shell script
python3 launch_aws_emr.py {region} {cluster name prefix} {use spot or not}
```

## Settings
### Spark configurations
Use the correct information of vCPU and RAM (GB) for optimization.

### Network
Specify subnets and security grouops.

### EC2 key name
Specify EC2 key name to access to EC2 directly.

### LOG Uri
Sepcify S3 path to let EMR store logs.
