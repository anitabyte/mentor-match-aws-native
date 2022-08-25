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
        self._connections: list[CSPerson] = [
            CSMentee(**value) if key == "csmentee" else CSMentor(**value)
            for x in kwargs.get("connections", [])
            for key, value in x.items()
        ]

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
    total_tasks = event_json["total_tasks"]
    results_bucket = os.environ["results_bucket"]
    best_bucket = os.environ["best_bucket"]
    ddb = os.environ["ddb"]

    matches_list = []

    s3_connection = boto3.client("s3")
    for i in range(total_tasks):
        key = f"{match_id}/{i}.json"
        print(f"Fetching {key}")
        json_from_matches = json.loads(
            s3_connection.get_object(Bucket=results_bucket, Key=key)["Body"]
            .read()
            .decode("utf-8")
        )
        mentors = [
            CSMentor(**x["csmentor"]) for x in json_from_matches["matched_mentors"]
        ]
        mentees = [
            CSMentee(**x["csmentee"]) for x in json_from_matches["matched_mentees"]
        ]
        unmatched_bonus = json_from_matches["unmatched_bonus"]
        matches_list.append((mentors, mentees, unmatched_bonus))

    print("Matches list length: " + str(len(matches_list)))
    highest_count = {"mentors": 0, "mentees": 0}
    best_outcome = matches_list[0]
    for participant_tuple in matches_list:
        mentors, mentees, unmatched_bonus = participant_tuple
        one_connection_min_func = functools.partial(
            map, lambda participant: len(participant.connections) > 0
        )
        current_count = {
            "mentors": sum(one_connection_min_func(mentors)),
            "mentees": sum(one_connection_min_func(mentees)),
        }
        print(
            f"For unmatched bonus {unmatched_bonus} - Mentors: {current_count['mentors']}, Mentees: {current_count['mentees']}"
        )
        if (current_count["mentees"] > highest_count["mentees"]) or (  # type: ignore
            current_count["mentees"] == highest_count["mentees"]
            and current_count["mentors"] > highest_count["mentors"]  # type: ignore
        ):  # type: ignore
            print("New best outcome found")
            best_outcome = participant_tuple
            highest_count = current_count  # type: ignore

    best_object = io.BytesIO(
        json.dumps(
            {
                "matched_mentors": [x.to_dict_for_output() for x in best_outcome[0]],
                "matched_mentees": [x.to_dict_for_output() for x in best_outcome[1]],
                "unmatched_bonus": best_outcome[2],
            }
        ).encode("utf-8")
    )

    s3_results_response = s3_connection.upload_fileobj(
        best_object, best_bucket, f"{match_id}.json"
    )

    ddb_connection = boto3.client("dynamodb")
    dt = datetime.now()
    ddb_connection.update_item(
        TableName=ddb,
        Key={"id": {"S": match_id}},
        UpdateExpression="SET end_time = :i, #status = :j",
        ExpressionAttributeValues={
            ":i": {"S": dt.isoformat()},
            ":j": {"S": "COMPLETE"},
        },
        ExpressionAttributeNames={"#status": "status"},
        ReturnValues="UPDATED_NEW",
    )
    return 1
