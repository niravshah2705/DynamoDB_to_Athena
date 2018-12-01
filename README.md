# DynamoDB_to_Athena
AWS has Athena service which can query structured data from S3. The DynamoDB is managed NoSQL database. So we have to convert Unstructured data to Structured data. The code written in python &amp; performs this objective.

# Steps to use script
git clone https://github.com/niravshah2705/DynamoDB_to_Athena
cd DynamoDB_to_Athena
./setup.sh
python awsglue.py <table Name>

# Future Steps
Would provide cloudformation script along with setup.sh
Would prepare Terraform script 
More customization with external parameterized command
Increase security by further reduce access granularity
