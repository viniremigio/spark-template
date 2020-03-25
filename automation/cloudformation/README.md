# Bootstrap EMR Cluster with AWS Cloudformation

## Create EMR Cluster

```
aws cloudformation create-stack --stack-name EMRSpark --template-body file://automation/cloudformation/bootstrap_emr.yaml
```

## Delete EMR Cluster

```
aws cloudformation delete-stack --stack-name EMRSpark
```

## Describe Stack

```
aws cloudformation describe-stacks  --stack-name EMRSpark
```
