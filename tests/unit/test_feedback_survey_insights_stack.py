import aws_cdk as core
import aws_cdk.assertions as assertions

from feedback_survey_insights.feedback_survey_insights_stack import FeedbackSurveyInsightsStack

# example tests. To run these tests, uncomment this file along with the example
# resource in feedback_survey_insights/feedback_survey_insights_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = FeedbackSurveyInsightsStack(app, "feedback-survey-insights")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
