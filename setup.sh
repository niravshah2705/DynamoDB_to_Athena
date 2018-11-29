#Setup environment it didnt worked for my machine so i have mentioned profile with each command
aws configure set region ap-southeast-1 --profile prod

#Create a user to execute python script 
#[ I am setting this user as I have to deploy job remotely. One can skip if they have admin user ]
aws iam create-user --user-name DYNAMODBGLUEJOB --profile prod
aws iam create-access-key --user-name DYNAMODBGLUEJOB --profile prod
aws iam attach-user-policy --user-name DYNAMODBGLUEJOB --policy-arn arn:aws:iam::aws:policy/IAMFullAccess --profile prod
aws iam attach-user-policy --user-name DYNAMODBGLUEJOB --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess --profile prod
aws iam attach-user-policy --user-name DYNAMODBGLUEJOB --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole --profile prod

#Create s3 bucket & related access
