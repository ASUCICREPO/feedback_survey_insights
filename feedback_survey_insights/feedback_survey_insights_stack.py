from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_apigateway as apigateway,
    CfnOutput,
)
from constructs import Construct
import os

class FeedbackSurveyInsightsStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Retrieve context variables
        project_name = self.node.try_get_context("project_name")
        bucket_name = self.node.try_get_context("bucket_name")
        athena_table_name = self.node.try_get_context("athena_table_name")
        file_name = self.node.try_get_context("file_name")
        file_type = self.node.try_get_context("file_type")

        # Create S3 bucket
        data_bucket = s3.Bucket(
            self, "DataBucket",
            bucket_name=bucket_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # Create the 'raw/' folder by uploading an empty object
        s3_deployment.BucketDeployment(
            self, "CreateRawFolder",
            destination_bucket=data_bucket,
            destination_key_prefix="raw/",
            sources=[s3_deployment.Source.data("placeholder.txt", "This is a placeholder file.")],
            retain_on_delete=False
        )
        s3_deployment.BucketDeployment(
            self, "CreateFilteredFolder",
            destination_bucket=data_bucket,
            destination_key_prefix="filter/",
            sources=[s3_deployment.Source.data("placeholder.txt", "This is a placeholder file.")],
            retain_on_delete=False
        )
        s3_deployment.BucketDeployment(
            self, "CreateProcessedFolder",
            destination_bucket=data_bucket,
            destination_key_prefix="processed/",
            sources=[s3_deployment.Source.data("placeholder.txt", "This is a placeholder file.")],
            retain_on_delete=False
        )

        script_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'processing_script'))
        s3_deployment.BucketDeployment(
            self, "CreateScriptsFolder",
            destination_bucket=data_bucket,
            destination_key_prefix="scripts/",
            sources=[s3_deployment.Source.asset(script_directory, exclude=["**", "!processing_script.py"])],
            retain_on_delete=False
        )
        

        # Create a single IAM role for all Lambda functions
        lambda_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )

        # Attach policies to the role
        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        )

        # Policy for S3 access
        lambda_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "s3:CreateMultipartUpload",
                "s3:UploadPart",
                "s3:CompleteMultipartUpload",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploads",
                "s3:ListParts",
                "s3:GetObject",
                "s3:PutObject"
            ],
            resources=[
                data_bucket.bucket_arn,
                f"{data_bucket.bucket_arn}/*"
            ]
        ))

        # Environment variables for Lambda functions
        lambda_env = {
            "BUCKET_NAME": data_bucket.bucket_name,
            "FILE_NAME": file_name,
            "FILE_TYPE": file_type
        }

        # Define Lambda functions
        initiate_upload_lambda = _lambda.Function(
            self, "InitiateUploadFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="initiate_upload.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions/initiate_upload"),
            role=lambda_role,
            environment=lambda_env,
            function_name=f"{project_name}-InitiateUploadFunction"
        )

        generate_presigned_urls_lambda = _lambda.Function(
            self, "ProcessUploadFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="process_upload.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions/process_upload"),
            role=lambda_role,
            environment=lambda_env,
            function_name=f"{project_name}-ProcessUploadFunction"
        )

        complete_upload_lambda = _lambda.Function(
            self, "CompleteUploadFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="complete_upload.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions/complete_upload"),
            role=lambda_role,
            environment=lambda_env,
            function_name=f"{project_name}-CompleteUploadFunction"
        )

        # Create API Gateway and define endpoints
        api = apigateway.RestApi(
            self, "FeedbackSurveyApi",
            rest_api_name=f"{project_name} API",
            description="API for handling multipart uploads."
        )

        # /initiate-upload
        initiate_upload_resource = api.root.add_resource("initiate-upload")
        initiate_upload_integration = apigateway.LambdaIntegration(initiate_upload_lambda)
        initiate_upload_resource.add_method("POST", initiate_upload_integration)

        # /generate-presigned-urls
        generate_urls_resource = api.root.add_resource("generate-presigned-urls")
        generate_urls_integration = apigateway.LambdaIntegration(generate_presigned_urls_lambda)
        generate_urls_resource.add_method("POST", generate_urls_integration)

        # /complete-upload
        complete_upload_resource = api.root.add_resource("complete-upload")
        complete_upload_integration = apigateway.LambdaIntegration(complete_upload_lambda)
        complete_upload_resource.add_method("POST", complete_upload_integration)

        # Outputs
        CfnOutput(self, "APIEndpoint", value=api.url)
