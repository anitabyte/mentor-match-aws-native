import os
import uuid
import boto3
import json
import io


def handler(event, context):
    ddb_table = os.environ["ddb"]
    bucket = os.environ["upload_bucket"]
    queue = os.environ["mapping_queue"]

    match_id = uuid.uuid4().hex

    # get data from payload
    mentors = event["mentors"]
    mentees = event["mentees"]
    mentors_key = f"{match_id}-mentors.json"
    mentees_key = f"{match_id}-mentors.json"

    bucket_connection = boto3.client("s3")
    bucket_connection.upload_fileobj(
        io.BytesIO(json.dumps(mentees).encode("utf-8")), bucket, mentees_key
    )
    bucket_connection.upload_fileobj(
        io.BytesIO(json.dumps(mentors).encode("utf-8")), bucket, mentors_key
    )

    ddb_connection = boto3.client("dynamodb")
    response = ddb_connection.put_item(
        TableName=ddb_table, Item={"id": {"S": match_id}, "status": {"S": "PENDING"}}
    )

    sqs_connection = boto3.client("sqs")
    response = sqs_connection.send_message(QueueUrl=queue, MessageBody=match_id)

    return {"uuid": match_id}
