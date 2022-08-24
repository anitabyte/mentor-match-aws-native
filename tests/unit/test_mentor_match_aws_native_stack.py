import aws_cdk as core
import aws_cdk.assertions as assertions

from mentor_match_aws_native.mentor_match_aws_native_stack import (
    MentorMatchAwsNativeStack,
)

# example tests. To run these tests, uncomment this file along with the example
# resource in mentor_match_aws_native/mentor_match_aws_native_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = MentorMatchAwsNativeStack(app, "mentor-match-aws-native")
    template = assertions.Template.from_stack(stack)


#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
