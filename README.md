
# Serverless ETL for covid19 data

This is code for a Lambda function that runs daily at 12 AM and downloads data from two sources and commits the data to a postgreSQL database
The code for the function is in the code directory and the definition of the infrastructure is in the cdk folder

## Tech stack
**Compute**: Python application on AWS Lambda  
**Storage**: S3  
**Database**: PostgreSQL on RDS  
**Reporting**: Amazon Quicksight  
**IaaC and CI/CD**: Github actions + AWS CDK

## Code libraries
**boto3**: AWS SDK for python  
**pandas**: Data transformation and manipulation  
**psycopg2**: PostgreSQL client for python  
**requests**: HTTP requests in python

## Useful commands

 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation
