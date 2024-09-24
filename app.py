#!/usr/bin/env python3
import os

import aws_cdk as cdk

from feedback_survey_insights.feedback_survey_insights_stack import FeedbackSurveyInsightsStack
from feedback_survey_insights.feedback_survey_state_machine_stack import FeedbackSurveyStateMachineStack
from feedback_survey_insights.feedback_survey_api_stack import FeedbackSurveyApiStack
# from feedback_survey_insights.feedback_survey_processing_stack import FeedbackSurveyProcessingStack


app = cdk.App()
FeedbackSurveyInsightsStack(app, "FeedbackSurveyInsightsStack",)
# FeedbackSurveyProcessingStack(app, "FeedbackSurveyProcessingStack")
project_name = app.node.try_get_context("project_name") or "FeedbackSurveyProject"
bucket_name = app.node.try_get_context("bucket_name")

# Instantiate the State Machine Stack
state_machine_stack = FeedbackSurveyStateMachineStack(
    app, "FeedbackSurveyStateMachineStack",
    project_name=project_name,
    bucket_name=bucket_name,
)

# Instantiate the API Stack
api_stack = FeedbackSurveyApiStack(
    app, "FeedbackSurveyApiStack",
    project_name=project_name,
    state_machine_arn=state_machine_stack.state_machine_arn,
    bucket_name=bucket_name,
)

# Add dependency
api_stack.add_dependency(state_machine_stack)

app.synth()
