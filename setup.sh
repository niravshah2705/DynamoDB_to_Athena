#Consideration - 
# aws cli installation done with: https://docs.aws.amazon.com/cli/latest/userguide/awscli-install-bundle.html
# aws cli setup done with: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html 

#Setup environment it didnt worked for my machine so i have mentioned profile with each command
aws configure set region ap-southeast-1 --profile prod

#Create s3 bucket for script & data
aws s3api create-bucket --bucket ddb-with-athena --region ap-southeast-1 --create-bucket-configuration LocationConstraint=ap-southeast-1 --profile prod

#Create a user to execute python script 
#[ I am setting this user as I have to deploy job remotely. One can skip if they have admin user ]
aws iam create-user --user-name DYNAMODBGLUEJOB --profile prod
aws iam create-access-key --user-name DYNAMODBGLUEJOB --profile prod
aws iam attach-user-policy --user-name DYNAMODBGLUEJOB --policy-arn arn:aws:iam::aws:policy/IAMFullAccess --profile prod
aws iam attach-user-policy --user-name DYNAMODBGLUEJOB --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess --profile prod
aws iam attach-user-policy --user-name DYNAMODBGLUEJOB --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole --profile prod
aws iam put-user-policy  --user-name DYNAMODBGLUEJOB  --policy-name glue-permission  --policy-document file://glue-s3-policy.json --profile prod

#Create role for Glue service
aws iam create-role --role-name DYNAMODBGLUEROLE --assume-role-policy-document file://glue-role-policy.json --profile prod
aws iam attach-role-policy  --role-name DYNAMODBGLUEROLE --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole --profile prod 
aws iam attach-role-policy  --role-name DYNAMODBGLUEROLE --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess --profile prod
aws iam put-role-policy  --role-name DYNAMODBGLUEROLE  --policy-name dynamoglue-permission  --policy-document file://dynamoglue-policy.json --profile prod

#Create Glue Database
aws glue create-database --database-input '{"Name":"DDB"}' --profile prod
aws glue create-database --database-input '{"Name":"Athena"}' --profile prod

# Copy script to s3 location
aws s3 cp Script.py s3://ddb-with-athena/niravshah2705@gmail.com/ --profile prod

# [Optionally] Create user for query Athena services, we are using redash for query
aws iam create-user --user-name Athena-redash --profile prod
aws iam create-access-key --user-name Athena-redash --profile prod

aws iam attach-user-policy --user-name Athena-redash --policy-arn arn:aws:iam::aws:policy/service-role/AWSQuicksightAthenaAccess --profile prod
#create bucket for temp store
aws s3api create-bucket --bucket redash-tmp --region ap-southeast-1 --create-bucket-configuration LocationConstraint=ap-southeast-1 --profile prod
aws iam put-user-policy  --user-name Athena-redash  --policy-name redash-s3-permission  --policy-document file://redash-s3-policy.json --profile prod
