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
    aws_lambda_event_sources as sources,
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

        reduce_queue = sqs.Queue(
            self,
            "MentorMatchReduceQueue",
            visibility_timeout=Duration.seconds(300),
        )

        work_queue = sqs.Queue(
            self,
            "MentorMatchWorkQueue",
            visibility_timeout=Duration.seconds(300),
        )

        bucket = s3.Bucket(
            self,
            "MentorMatchSourceCSVs",
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        results_bucket = s3.Bucket(
            self,
            "MentorMatchResults",
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        best_bucket = s3.Bucket(
            self,
            "MentorMatchBest",
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
                "AWS_LAMBDA_EXEC_WRAPPER": "/opt/otel-instrument",
            },
            tracing=aws_lambda.Tracing.ACTIVE,
            memory_size=1024,
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
                str(
                    Path.joinpath(
                        Path(__file__).parent.parent,
                        "mentor_match_generate_match_tasks",
                    )
                ),
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
                "mapping_queue": work_queue.queue_url,
                "upload_bucket": bucket.bucket_name,
                "AWS_LAMBDA_EXEC_WRAPPER": "/opt/otel-instrument",
            },
            tracing=aws_lambda.Tracing.ACTIVE,
            timeout=Duration.seconds(300),
            memory_size=384,
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
        work_queue.grant_send_messages(match_function_role)

        match_worker_function = aws_lambda.Function(
            self,
            "MentorMatchWorkerFunction",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            code=aws_lambda.Code.from_asset(
                str(
                    Path.joinpath(
                        Path(__file__).parent.parent, "mentor_match_generate_matches"
                    )
                ),
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
                "reduce_queue": reduce_queue.queue_url,
                "upload_bucket": bucket.bucket_name,
                "results_bucket": results_bucket.bucket_name,
                "AWS_LAMBDA_EXEC_WRAPPER": "/opt/otel-instrument",
            },
            tracing=aws_lambda.Tracing.ACTIVE,
            memory_size=2048,
            timeout=Duration.seconds(300),
        )
        match_worker_function.add_layers(
            aws_lambda.LayerVersion.from_layer_version_arn(
                self,
                "match_generate_opentelemetry",
                "arn:aws:lambda:eu-west-2:901920570463:layer:aws-otel-python-amd64-ver-1-11-1:2",
            )
        )
        match_worker_function.add_event_source(
            sources.SqsEventSource(work_queue, batch_size=1)
        )
        match_worker_function_role = match_worker_function.role
        job_state_ddb.grant_read_write_data(match_worker_function_role)
        bucket.grant_read(match_worker_function_role)
        results_bucket.grant_read_write(match_worker_function_role)
        reduce_queue.grant_send_messages(match_worker_function_role)

        reduce_function = aws_lambda.Function(
            self,
            "MentorMatchReduceFunction",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            code=aws_lambda.Code.from_asset(
                str(Path.joinpath(Path(__file__).parent.parent, "mentor_match_reduce")),
                bundling=BundlingOptions(
                    image=aws_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            handler="reduce.handler",
            environment={
                "ddb": job_state_ddb.table_name,
                "reduce_queue": reduce_queue.queue_url,
                "results_bucket": results_bucket.bucket_name,
                "best_bucket": best_bucket.bucket_name,
                "AWS_LAMBDA_EXEC_WRAPPER": "/opt/otel-instrument",
            },
            tracing=aws_lambda.Tracing.ACTIVE,
            memory_size=1024,
            timeout=Duration.seconds(300),
        )
        reduce_function.add_layers(
            aws_lambda.LayerVersion.from_layer_version_arn(
                self,
                "reduce_opentelemetry",
                "arn:aws:lambda:eu-west-2:901920570463:layer:aws-otel-python-amd64-ver-1-11-1:2",
            )
        )
        reduce_function.add_event_source(
            sources.SqsEventSource(reduce_queue, batch_size=1)
        )
        reduce_function_role = reduce_function.role
        results_bucket.grant_read_write(reduce_function_role)
        job_state_ddb.grant_read_write_data(reduce_function_role)
        best_bucket.grant_read_write(reduce_function_role)
