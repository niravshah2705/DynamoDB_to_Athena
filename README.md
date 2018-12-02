# DynamoDB_to_Athena
AWS has Athena service which can query structured data from S3. The DynamoDB is managed NoSQL database. So we have to convert Unstructured data to Structured data. The code written in python &amp; performs this objective.

# Steps to use script
```sh
git clone https://github.com/niravshah2705/DynamoDB_to_Athena
cd DynamoDB_to_Athena
./setup.sh
python awsglue.py <table Name>
```

# Design Diagram

![Design Diagram](https://d2908q01vomqb2.cloudfront.net/887309d048beef83ad3eabf2a79a64a389ab1c9f/2018/09/12/simplify-amazon-dynamodb-glue-athena-1-2.gif)

# Future Steps
- Would provide cloudformation script along with setup.sh
- Would prepare Terraform script 
- More customization with external parameterized command
- Increase security by further reduce access granularity

# Reference Document 
- https://aws.amazon.com/blogs/database/simplify-amazon-dynamodb-data-extraction-and-analysis-by-using-aws-glue-and-amazon-athena/
- https://niravshah2705-software-engineering.blogspot.com/2018/12/dynamodb-to-s3-athena.html
