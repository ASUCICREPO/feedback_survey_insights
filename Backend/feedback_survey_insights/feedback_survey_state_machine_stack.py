# feedback_survey_state_machine_stack.py

from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_glue as glue,
    aws_ec2 as ec2,
    Duration,
    Size,
    CfnOutput,
)
from constructs import Construct
import os
import json
from aws_cdk.aws_stepfunctions import DefinitionBody


class FeedbackSurveyStateMachineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, *, project_name: str, bucket_name: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Context variables
        athena_database_name = self.node.try_get_context("athena_database_name") or "employee_surveydata"
        athena_table_name = self.node.try_get_context("athena_table_name") or "survey_data"
        docker_image_uri = self.node.try_get_context("docker_image_uri")
        headers = self.node.try_get_context("headers") or []

        # Process headers: lowercase and replace spaces with underscores
        processed_headers = [col.lower().replace(" ", "_").replace(":", "_") for col in headers]

        # Identify comment columns
        comment_columns = [col for col in processed_headers if col.startswith("comment_")]

        # Use existing bucket
        data_bucket = s3.Bucket.from_bucket_name(self, "DataBucket", bucket_name)

        # Create Glue Database and Table
        glue_database = glue.CfnDatabase(
            self, "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=athena_database_name
            )
        )

        glue_table = glue.CfnTable(
            self, "GlueTable",
            catalog_id=self.account,
            database_name=athena_database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name=athena_table_name,
                description="Table for survey data",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "csv",
                    "skip.header.line.count": "1",
                    "compressionType": "none",
                    "typeOfData": "file"
                },
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[glue.CfnTable.ColumnProperty(name=col_name, type="string") for col_name in processed_headers],
                    location=f"s3://{bucket_name}/raw/",
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                        parameters={
                            "field.delim": ",",
                            "escape.delim": "\\"
                        }
                    )
                )
            )
        )

        glue_table.add_dependency(glue_database)

        # IAM Roles and Policies
        lambda_role = iam.Role(
            self, "LambdaExecutionRoleStateMachine",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )

        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        )

        lambda_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "s3:*",
                "athena:*",
                "glue:*",
                "logs:*",
                "bedrock:*",
                "sagemaker:CreateProcessingJob",
                "sagemaker:DescribeProcessingJob",
                "sagemaker:*",
                "ecr:*"
            ],
            resources=["*"]
        ))

        sagemaker_role = iam.Role(
            self, "SageMakerProcessingRole",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com")
        )

        sagemaker_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess")
        )

        sagemaker_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "ecr:GetDownloadUrlForLayer",
                "ecr:BatchGetImage",
                "ecr:BatchCheckLayerAvailability",
                "s3:*"
            ],
            resources=["*"]  # Or restrict to the specific ECR repository ARN
        ))

        # Define Lambda Functions used in the state machine

        # Define the AWS Built-in Pandas Layer
        pandas_layer = _lambda.LayerVersion.from_layer_version_arn(
            self, "AWSSDKPandasLayer",
            layer_version_arn="arn:aws:lambda:us-west-2:336392948345:layer:AWSSDKPandas-Python312:13"
        )

        # Common timeout configuration
        lambda_timeout = Duration.seconds(3600)  # Adjust the timeout as needed

        process_query_lambda = _lambda.Function(
            self, "ProcessQueryFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="process_query.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions/process_query"),
            role=lambda_role,
            environment={
                'BUCKET_NAME': data_bucket.bucket_name,
                'ATHENA_DATABASE': athena_database_name,
                'ATHENA_TABLE': athena_table_name,
                'COMMENT_COLUMNS': json.dumps(comment_columns),
                'REGION': self.region
            },
            function_name=f"{project_name}-ProcessQueryFunction",
            timeout=lambda_timeout  # Increased timeout
        )

        generate_insights_lambda = _lambda.Function(
            self, "GenerateInsightsFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="generate_insights.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions/generate_insights"),
            role=lambda_role,
            environment={
                'BUCKET_NAME': data_bucket.bucket_name,
                'REGION': self.region
            },
            function_name=f"{project_name}-GenerateInsightsFunction",
            timeout=lambda_timeout,  # Increased timeout
            layers=[pandas_layer]  # Attach the Pandas layer
        )

        start_sagemaker_processing_lambda = _lambda.Function(
            self, "StartSageMakerProcessingJobFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="start_sagemaker_processing.lambda_handler",
            code=_lambda.Code.from_asset("lambda_functions/start_sagemaker_processing"),
            role=lambda_role,
            environment={
                'BUCKET_NAME': data_bucket.bucket_name,
                'DOCKER_IMAGE_URI': docker_image_uri,
                'SAGEMAKER_ROLE_ARN': sagemaker_role.role_arn,
                'REGION': self.region
            },
            function_name=f"{project_name}-StartSageMakerProcessingJobFunction",
            timeout=lambda_timeout  # Increased timeout
        )

        # Define Step Function Tasks
        process_query_task = tasks.LambdaInvoke(
            self, "StartProcessingJob",
            lambda_function=process_query_lambda,
            output_path="$.Payload",
            result_path="$.processing_job"
        )

        process_query_task.add_catch(
            handler=self.create_handle_general_error("ProcessQueryTask"),
            errors=["States.ALL"],
            result_path="$.error_info"
        )

        sagemaker_task = tasks.LambdaInvoke(
            self, "StartSageMakerProcessingJob",
            lambda_function=start_sagemaker_processing_lambda,
            input_path="$.processing_job",
            output_path="$.Payload",
            result_path="$.sagemaker_job"
        )

        sagemaker_task.add_catch(
            handler=self.create_handle_general_error("SageMakerTask"),
            errors=["States.ALL"],
            result_path="$.error_info"
        )

        generate_insights_task = tasks.LambdaInvoke(
            self, "InvokeLambda2",
            lambda_function=generate_insights_lambda,
            output_path="$.Payload",
            result_path="$.lambda2_result"
        )

        generate_insights_task.add_catch(
            handler=self.create_handle_general_error("GenerateInsightsTask"),
            errors=["States.ALL"],
            result_path="$.error_info"
        )

        # Define the Step Function Workflow
        definition = process_query_task.next(sagemaker_task).next(generate_insights_task)

        # Create the State Machine using definition_body
        state_machine = sfn.StateMachine(
            self, "FeedbackSurveyStateMachine",
            definition_body=DefinitionBody.from_chainable(definition),
            timeout=Duration.minutes(30)
        )

        # Store the state machine ARN as an instance variable
        self.state_machine_arn = state_machine.state_machine_arn

        # Optionally, output the state machine ARN
        CfnOutput(self, "StateMachineArnOutput", value=state_machine.state_machine_arn, export_name="StateMachineArn")

    def create_handle_general_error(self, id_suffix: str):
        # Define a Pass state for handling general errors with a unique name
        handle_general_error = sfn.Pass(
            self, f"HandleGeneralError_{id_suffix}",
            parameters={
                "Error.$": "$.Error",
                "Cause.$": "$.Cause"
            },
            result_path="$.error_info"
        )

        # Define a Fail state with a unique name
        fail_with_message = sfn.Fail(
            self, f"FailWithMessage_{id_suffix}",
            error="GeneralProcessingError",
            cause="Possible scenarios: The filters you selected returned no data. Please try changing the filters and try again. Alternatively, there may be an internal server issue. Please try again later."
        )

        # Link the Pass state to the Fail state
        handle_general_error.next(fail_with_message)

        return handle_general_error
