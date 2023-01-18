import aws_cdk as core
import aws_cdk.assertions as assertions

from rio_tracker_lambda.rio_tracker_lambda_stack import RioTrackerLambdaStack

# example tests. To run these tests, uncomment this file along with the example
# resource in rio_tracker_lambda/rio_tracker_lambda_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = RioTrackerLambdaStack(app, "rio-tracker-lambda")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
