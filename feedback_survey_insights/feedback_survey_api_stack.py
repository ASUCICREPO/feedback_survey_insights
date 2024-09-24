# feedback_survey_api_stack.py

from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_apigateway as apigateway,
    CfnOutput,
)
from constructs import Construct


class FeedbackSurveyApiStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, *, project_name: str, state_machine_arn: str, bucket_name: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # IAM Roles and Policies
        lambda_role = iam.Role(
            self, "LambdaExecutionRoleApi",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )

        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        )

        lambda_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "logs:*",
                "states:StartExecution",
                "states:DescribeExecution",
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket",
            ],
            resources=["*"]
        ))

        # Define Lambda Functions
        start_query_lambda = _lambda.Function(
            self, "StartQueryFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="start_query.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions/start_query"),
            role=lambda_role,
            environment={
                'STEP_FUNCTION_ARN': state_machine_arn,
                'BEDROCK_MODEL_ID': 'your-bedrock-model-id',
                'REGION': self.region
            },
            function_name=f"{project_name}-StartQueryFunction"
        )

        check_status_lambda = _lambda.Function(
            self, "CheckStatusFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="check_status.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions/check_status"),
            role=lambda_role,
            environment={
                'STEP_FUNCTION_ARN': state_machine_arn,
                'REGION': self.region
            },
            function_name=f"{project_name}-CheckStatusFunction"
        )

        # Define API Gateway
        api = apigateway.RestApi(
            self, "FeedbackSurveyProcessingApi",
            rest_api_name=f"{project_name} Processing API",
            description="API for processing feedback survey queries."
        )

        # /process-query Endpoint
        process_query_resource = api.root.add_resource("process-query")
        process_query_integration = apigateway.LambdaIntegration(start_query_lambda)
        process_query_resource.add_method("POST", process_query_integration)

        # /check-status Endpoint
        check_status_resource = api.root.add_resource("check-status")
        check_status_integration = apigateway.LambdaIntegration(check_status_lambda, proxy=True)
        check_status_resource.add_method("GET", check_status_integration)

        # Outputs
        CfnOutput(self, "APIEndpoint", value=api.url)
