Command to run cloud formation template

1. Create MySQL DB
`aws cloudformation create-stack --stack-name OrkaMySQLStack --template-body file://mysql_rds_template.yml  --parameters ParameterKey=vpcsg,ParameterValue=sg-0732127d8a2b06498`

2. Create DynamoDB table
`aws cloudformation create-stack --stack-name MatchesDynamoDBTableStack --template-body file://aurora_rds_template.yaml`


