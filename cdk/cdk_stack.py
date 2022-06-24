from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
    aws_sns as sns,
    aws_events as events,
    aws_events_targets as targets,
    Duration
)
from constructs import Construct

class CdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Define Lambda layers
        pandas = lambda_.LayerVersion.from_layer_version_attributes(self, 'Pandas',
            layer_version_arn="arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p39-pandas:4")

        requests = lambda_.LayerVersion.from_layer_version_attributes(self, 'Requests',
            layer_version_arn="arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p39-requests-html:4")

        psycopg = lambda_.LayerVersion.from_layer_version_attributes(self, 'Psycopg',
            layer_version_arn="arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p39-psycopg2-binary:1")
        
        # instantiate DB credentials using secrets manager
        db_secrets = rds.DatabaseSecret(self, 'postgres-secret',
                    username='postgres',
                    secret_name='postgres-credentials'
                    )

        # Create the database
        db = rds.DatabaseInstance(self, "db",
                    engine=rds.DatabaseInstanceEngine.postgres(version=rds.PostgresEngineVersion.VER_13_4),
                    instance_type=ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.MICRO),
                    credentials=rds.Credentials.from_secret(db_secrets),
                    vpc=ec2.Vpc(self, 'vpc'),
                    vpc_subnets=ec2.SubnetSelection(
                        subnet_type=ec2.SubnetType.PUBLIC
                    )
                    )
        
        # Allow public connection to the db
        db.connections.allow_default_port_from_any_ipv4()

        # Define the function's execution role
        lambda_role = iam.Role(self, "lambda_role",
                    assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                    )
        lambda_role.add_to_policy(iam.PolicyStatement(
                    resources=["*"],
                    actions=[
                            "s3:GetObject",
                            "s3:PutObject",
                            "sns:Publish",
                            "secretsmanager:GetSecretValue"]
                    ))
        # Create the bucket used to store the data
        s3_bucket = s3.Bucket(self, 'dataBucket')

        # Create the delivery topic
        topic = sns.Topic(self, 'deliveryTopic')


        # Create the function
        function = lambda_.Function(self, "Serverless-ETL",
                    runtime=lambda_.Runtime.PYTHON_3_9,
                    code=lambda_.Code.from_asset("./code"),
                    handler="lambda_function.lambda_handler",
                    layers=[pandas, requests, psycopg],
                    timeout=Duration.minutes(5),
                    role=lambda_role,
                    memory_size=512,
                    environment={
                        'S3_BUCKET':s3_bucket.bucket_name,
                        'SNS_TOPIC': topic.topic_arn
                    }
                    )

        # Allow the function to publish to the topic
        topic.grant_publish(function)

        # Create the event rule and schedule
        rule = events.Rule(self, "Rule",
                    schedule=events.Schedule.expression('cron(0 0 * * ? *)'),
                    )        
        rule.add_target(targets.LambdaFunction(function))


