import boto3
import json

from typing import Union

import functools
import matching.process as process
from matching.factory import ParticipantFactory
from matching.mentee import Mentee
from matching.mentor import Mentor
from matching.person import Person
import matching.rules.rule as rl
from matching.rules.rule import UnmatchedBonus
import operator
import os
import io
from datetime import datetime


class CSPerson(Person):
    """
    This interface contains methods for mapping CS-specific grades, as well as the specific fields in the CS spreadsheet
    """

    grade_mapping = [
        "AA",
        "AO",
        "EO",
        "HEO",
        "SEO",
        "Grade 7",
        "Grade 6",
        "SCS1",
        "SCS2",
        "SCS3",
        "SCS4",
    ]

    def __init__(self, **kwargs):
        self.biography = kwargs.get("biography")
        self.both = True if kwargs.get("both mentor and mentee") == "yes" else False
        kwargs = self.map_input_to_model(kwargs)
        super(CSPerson, self).__init__(**kwargs)
        self._connections: list[CSPerson] = []

    @classmethod
    def str_grade_to_val(cls, grade: str):
        return cls.grade_mapping.index(grade)

    @classmethod
    def val_grade_to_str(cls, grade: int):
        return cls.grade_mapping[grade]

    @property
    def connections(self) -> list["CSPerson"]:
        return self._connections

    @connections.setter
    def connections(self, new_connections):
        self._connections = new_connections

    @staticmethod
    def map_input_to_model(data: dict):
        data["role"] = data["job title"]
        data["email"] = data.get("email address", data.get("email"))
        data["grade"] = CSPerson.str_grade_to_val(data.get("grade", ""))
        return data

    def map_model_to_output(self, data: dict):
        data["job title"] = data.pop("role")
        data["grade"] = self.val_grade_to_str(int(data.get("grade", "0")))
        data["biography"] = self.biography
        data["both mentor and mentee"] = "yes" if self.both else "no"
        return data

    def to_dict_for_output(self, depth=1) -> dict:
        return {
            "type": self.class_name(),
            "first name": self.first_name,
            "last name": self.last_name,
            "email address": self.email,
            "number of matches": len(self.connections),
            "mentor only": "no",
            "mentee only": "no",
            "both mentor and mentee": "yes" if self.both else "no",
            **{
                f"match {i} biography": match.biography
                for i, match in enumerate(self.connections)
            },
            "match details": "\n".join([match.biography for match in self.connections])
            if self.connections
            else None,
        }


class CSMentee(CSPerson, Mentee):
    def __init__(self, **kwargs):
        self.characteristic = kwargs.get("identity to match")
        super(CSMentee, self).__init__(**kwargs)

    @property
    def target_profession(self):
        return self.profession

    @target_profession.setter
    def target_profession(self, profession: str):
        self.profession = profession

    def core_to_dict(self) -> dict[str, dict[str, Union[str, list]]]:
        core = super(CSMentee, self).core_to_dict()
        data = core[self.class_name()]
        self.map_model_to_output(data)
        data["identity to match"] = self.characteristic
        return core

    def to_dict_for_output(self, depth=1) -> dict:
        output = super(CSMentee, self).to_dict_for_output()
        if not self.both:
            output["mentee only"] = "yes"
        return output


class CSMentor(CSPerson, Mentor):
    def __init__(self, **kwargs):
        self.characteristics: list[str] = kwargs.get("characteristics", "").split(", ")
        super(CSMentor, self).__init__(**kwargs)

    @property
    def current_profession(self):
        return self.profession

    @current_profession.setter
    def current_profession(self, profession: str):
        self.profession = profession

    def core_to_dict(self) -> dict[str, dict[str, Union[str, list]]]:
        core = super(CSMentor, self).core_to_dict()
        data = core[self.class_name()]
        self.map_model_to_output(data)
        data["characteristics"] = ", ".join(self.characteristics)
        return core

    def to_dict_for_output(self, depth=1) -> dict:
        output = super(CSMentor, self).to_dict_for_output()
        if not self.both:
            output["mentor only"] = "yes"
        return output


class CSParticipantFactory(ParticipantFactory):
    participant_types = {"csmentee": CSMentee, "csmentor": CSMentor}


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
    event_json = json.loads(event["Records"][0]["body"])
    print(event_json)
    match_id = event_json["match_id"]
    unmatched_bonus = event_json["unmatched"]

    bucket = os.environ["upload_bucket"]
    results_bucket = os.environ["results_bucket"]
    ddb = os.environ["ddb"]
    queue = os.environ["reduce_queue"]

    mentors_key = f"{match_id}-mentors.json"
    mentees_key = f"{match_id}-mentees.json"

    s3_connection = boto3.client("s3")
    mentors = json.loads(
        s3_connection.get_object(Bucket=bucket, Key=mentors_key)["Body"]
        .read()
        .decode("utf-8")
    )
    mentees = json.loads(
        s3_connection.get_object(Bucket=bucket, Key=mentees_key)["Body"]
        .read()
        .decode("utf-8")
    )

    cs_mentees = [CSMentee(**x) for x in mentees]
    cs_mentors = [CSMentor(**x) for x in mentors]

    all_rules = [base_rules() for _ in range(3)]
    for ruleset in all_rules:
        ruleset.append(UnmatchedBonus(unmatched_bonus))
    matched_mentors, matched_mentees = process.process_data(
        cs_mentors, cs_mentees, all_rules=all_rules
    )
    match_key = f"{match_id}/{unmatched_bonus}.json"
    match_object = io.BytesIO(
        json.dumps(
            {
                "matched_mentors": [x.to_dict() for x in matched_mentors],
                "matched_mentees": [x.to_dict() for x in matched_mentees],
                "unmatched_bonus": unmatched_bonus,
            }
        ).encode("utf-8")
    )
    s3_results_response = s3_connection.upload_fileobj(
        match_object, results_bucket, match_key
    )
    ddb_connection = boto3.client("dynamodb")
    response = ddb_connection.update_item(
        TableName=ddb,
        Key={"id": {"S": match_id}},
        UpdateExpression="SET remaining_tasks = remaining_tasks - :i",
        ExpressionAttributeValues={":i": {"N": "1"}},
        ReturnValues="ALL_NEW",
    )
    print(response)
    remaining = response["Attributes"]["remaining_tasks"]["N"]
    print(remaining)
    tasks = int(response["Attributes"]["total_tasks"]["N"])
    if remaining == "0":

        sqs_connection = boto3.client("sqs")
        queue_message = json.dumps({"match_id": match_id, "total_tasks": tasks})
        response = sqs_connection.send_message(
            QueueUrl=queue, MessageBody=queue_message
        )

    return 1
