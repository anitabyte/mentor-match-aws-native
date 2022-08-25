import boto3
import json
import os
import matching.rules.rule as rl
import operator


def base_rules() -> list[rl.Rule]:
    return [
        rl.Disqualify(
            lambda match: match.mentee.organisation == match.mentor.organisation
        ),
        rl.Disqualify(rl.Grade(target_diff=2, logical_operator=operator.gt).evaluate),
        rl.Disqualify(rl.Grade(target_diff=0, logical_operator=operator.le).evaluate),
        rl.Disqualify(lambda match: match.mentee in match.mentor.mentees),
        rl.Grade(2, operator.eq, {True: 12, False: 0}),
        rl.Grade(1, operator.eq, {True: 9, False: 0}),
        rl.Generic(
            {True: 10, False: 0},
            lambda match: match.mentee.target_profession
            == match.mentor.current_profession,
        ),
        rl.Generic(
            {True: 6, False: 0},
            lambda match: match.mentee.characteristic in match.mentor.characteristics
            and match.mentee.characteristic != "",
        ),
    ]


def handler(event, context):
    print(event)
    ddb_table = os.environ["ddb"]
    bucket = os.environ["upload_bucket"]
    queue = os.environ["mapping_queue"]

    match_id = event["Records"][0]["body"]

    max_score = sum(rule.results.get(True) for rule in base_rules())
    copies = ((match_id, i) for i in range(max_score))

    queue_connection = boto3.client("sqs")

    for copy in copies:
        response = queue_connection.send_message(
            QueueUrl=queue,
            MessageBody=json.dumps({"match_id": match_id, "unmatched": copy[1]}),
        )

    ddb_connection = boto3.client("dynamodb")
    response = ddb_connection.update_item(
        TableName=ddb_table,
        Key={"id": {"S": match_id}},
        UpdateExpression="SET #status = :status, total_tasks = :total_tasks, remaining_tasks = :total_tasks",
        ExpressionAttributeValues={
            ":status": {"S": "DISTRIBUTED"},
            ":total_tasks": {"N": f"{max_score}"},
        },
        ExpressionAttributeNames={"#status": "status"},
    )
    return {"id": match_id, "status": "COMPLETED"}
