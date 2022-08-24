from cProfile import run
from enum import auto
from aws_cdk import (
    BundlingOptions,
    Duration,
    Stack,
    RemovalPolicy,
    aws_sqs as sqs,
    aws_s3 as s3,
    aws_dynamodb as ddb,
    aws_lambda,
    aws_lambda_destinations as destinations,
    aws_lambda_event_sources as sources
)
from constructs import Construct
from pathlib import Path


class MentorMatchAwsNativeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        # example resource
        queue = sqs.Queue(
            self,
            "MentorMatchMatchQueue",
            visibility_timeout=Duration.seconds(300),
        )

        publish_queue = sqs.Queue(
            self,
            "MentorMatchStatusUpdateQueue",
            visibility_timeout=Duration.seconds(300),
        )

        bucket = s3.Bucket(
            self,
            "MentorMatchSourceCSVs",
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        job_state_ddb = ddb.Table(
            self,
            "MentorMatchJobState",
            removal_policy=RemovalPolicy.DESTROY,
            partition_key={"name": "id", "type": ddb.AttributeType.STRING},
        )

        upload_function = aws_lambda.Function(
            self,
            "MentorMatchUploadFunction",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            code=aws_lambda.Code.from_asset(
                str(Path.joinpath(Path(__file__).parent.parent, "mentor_match_upload")),
                bundling=BundlingOptions(
                    image=aws_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            handler="upload.handler",
            environment={
                "ddb": job_state_ddb.table_name,
                "mapping_queue": queue.queue_url,
                "upload_bucket": bucket.bucket_name,
                "AWS_LAMBDA_EXEC_WRAPPER": "/opt/otel-instrument"
            },
            tracing=aws_lambda.Tracing.ACTIVE,
            memory_size=1024
        )
        upload_function.add_function_url(
            auth_type=aws_lambda.FunctionUrlAuthType.NONE,
            cors=aws_lambda.FunctionUrlCorsOptions(allowed_origins=["*"]),
        )
        upload_function.add_layers(
            aws_lambda.LayerVersion.from_layer_version_arn(
                self,
                "upload_opentelemetry",
                "arn:aws:lambda:eu-west-2:901920570463:layer:aws-otel-python-amd64-ver-1-11-1:2",
            )
        )

        upload_function_role = upload_function.role
        bucket.grant_write(upload_function_role)
        queue.grant_send_messages(upload_function_role)
        job_state_ddb.grant_read_write_data(upload_function_role)

        match_function = aws_lambda.Function(
            self,
            "MentorMatchMatchFunction",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            code=aws_lambda.Code.from_asset(
                str(Path.joinpath(Path(__file__).parent.parent, "mentor_match_generate_matches")),
                bundling=BundlingOptions(
                    image=aws_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            handler="match.handler",
            environment={
                "ddb": job_state_ddb.table_name,
                "mapping_queue": queue.queue_url,
                "upload_bucket": bucket.bucket_name,
                "AWS_LAMBDA_EXEC_WRAPPER": "/opt/otel-instrument"
            },
            tracing=aws_lambda.Tracing.ACTIVE,
            memory_size=10240,
            timeout=Duration.seconds(300)
        )
        match_function.add_layers(
            aws_lambda.LayerVersion.from_layer_version_arn(
                self,
                "match_opentelemetry",
                "arn:aws:lambda:eu-west-2:901920570463:layer:aws-otel-python-amd64-ver-1-11-1:2",
            )
        )
        match_function.add_event_source(sources.SqsEventSource(queue, batch_size=1))
        match_function_role = match_function.role
        bucket.grant_read(match_function_role)
        job_state_ddb.grant_read_write_data(match_function_role)
