from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_glue as glue,
    Duration,
    CfnOutput,
)
from constructs import Construct
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
                "ecs:*"
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
                "s3:*",
                "sagemaker:*"
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
        lambda_timeout = Duration.seconds(600)  # Adjust the timeout as needed

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

        # -------------------------------------------------------------------------
        state_machine_role = iam.Role(
            self, "StateMachineExecutionRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        )

        # Attach policies to allow state machine creation and CloudWatch event rule management
        state_machine_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AWSStepFunctionsFullAccess")
        )

        state_machine_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess")
        )

        state_machine_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "events:PutRule",
                    "events:DeleteRule",
                    "events:DescribeRule",
                    "events:PutTargets",
                    "events:RemoveTargets"
                ],
                resources=["*"]
            )
        )

        # Define the state machine definition with dynamic Lambda ARNs and SageMaker
        state_machine_definition = f"""
        {{
          "Comment": "State Machine for Employee Survey Processing",
          "StartAt": "StartProcessingJob",
          "States": {{
            "StartProcessingJob": {{
              "Type": "Task",
              "Resource": "{process_query_lambda.function_arn}",
              "Parameters": {{
                "query.$": "$.query",
                "filters.$": "$.filters"
              }},
              "Catch": [
                {{
                  "ErrorEquals": [
                    "States.ALL"
                  ],
                  "Next": "HandleGeneralError"
                }}
              ],
              "ResultSelector": {{
                "job_id.$": "$.job_id",
                "query.$": "$.query",
                "filters.$": "$.filters",
                "object_name.$": "$.object_name"
              }},
              "ResultPath": "$.processing_job",
              "Next": "SageMakerCreateProcessingJob"
            }},
            "SageMakerCreateProcessingJob": {{
              "Type": "Task",
              "Resource": "arn:aws:states:::sagemaker:createProcessingJob.sync",
              "Parameters": {{
                "AppSpecification": {{
                  "ImageUri": "{docker_image_uri}",
                  "ContainerEntrypoint": [
                    "python3",
                    "/opt/ml/processing/input/code/processing_script.py"
                  ],
                  "ContainerArguments.$": "States.Array('--input-data', '/opt/ml/processing/input/data', '--output-data', '/opt/ml/processing/output', '--object-name', $.processing_job.object_name)"
                }},
                "ProcessingInputs": [
                  {{
                    "InputName": "input-data",
                    "S3Input": {{
                      "S3Uri": "s3://{bucket_name}/filter/",
                      "LocalPath": "/opt/ml/processing/input/data",
                      "S3DataType": "S3Prefix",
                      "S3InputMode": "File"
                    }}
                  }},
                  {{
                    "InputName": "code",
                    "S3Input": {{
                      "S3Uri": "https://{bucket_name}.s3.us-west-2.amazonaws.com/scripts/processing_script.py",
                      "LocalPath": "/opt/ml/processing/input/code",
                      "S3DataType": "S3Prefix",
                      "S3InputMode": "File"
                    }}
                  }}
                ],
                "ProcessingOutputConfig": {{
                  "Outputs": [
                    {{
                      "OutputName": "output-data",
                      "S3Output": {{
                        "S3Uri": "s3://{bucket_name}/processed/",
                        "LocalPath": "/opt/ml/processing/output",
                        "S3UploadMode": "EndOfJob"
                      }}
                    }}
                  ]
                }},
                "ProcessingResources": {{
                  "ClusterConfig": {{
                    "InstanceCount": 1,
                    "InstanceType": "ml.c5.xlarge",
                    "VolumeSizeInGB": 10
                  }}
                }},
                "RoleArn": "{sagemaker_role.role_arn}",
                "ProcessingJobName.$": "States.Format('processing-job-{{}}', $.processing_job.job_id)",
                "StoppingCondition": {{
                  "MaxRuntimeInSeconds": 3600
                }}
              }},
              "ResultPath": "$.sagemaker_job",
              "Next": "InvokeLambda2",
              "Catch": [
                {{
                  "ErrorEquals": [
                    "States.Runtime"
                  ],
                  "ResultPath": "$.error_info",
                  "Next": "HandleGeneralError"
                }}
              ]
            }},
            "InvokeLambda2": {{
              "Type": "Task",
              "Resource": "{generate_insights_lambda.function_arn}",
              "Parameters": {{
                "job_id.$": "$.processing_job.job_id",
                "query.$": "$.processing_job.query",
                "filters.$": "$.processing_job.filters"
              }},
              "ResultPath": "$.lambda2_result",
              "End": true,
              "Catch": [
                {{
                  "ErrorEquals": [
                    "States.ALL"
                  ],
                  "ResultPath": "$.error_info",
                  "Next": "HandleGeneralError"
                }}
              ]
            }},
            "HandleGeneralError": {{
              "Type": "Pass",
              "Result": "Possible scenarios: The filters you selected returned no data. Please try changing the filters and try again. Alternatively, there may be an internal server issue. Please try again later.",
              "Next": "FailWithMessage"
            }},
            "FailWithMessage": {{
              "Type": "Fail",
              "Error": "GeneralProcessingError",
              "Cause": "Possible scenarios: The filters you selected returned no data. Please try changing the filters and try again. Alternatively, there may be an internal server issue. Please try again later."
            }}
          }}
        }}
        """

        # Create the State Machine using the hardcoded JSON
        state_machine = sfn.StateMachine(
            self, "FeedbackSurveyStateMachine",
            definition_body=DefinitionBody.from_string(state_machine_definition),
            timeout=Duration.minutes(30),
            role=state_machine_role
        )

        # Store the state machine ARN as an instance variable
        self.state_machine_arn = state_machine.state_machine_arn

        # Output the state machine ARN
        CfnOutput(self, "StateMachineArnOutput", value=state_machine.state_machine_arn, export_name="StateMachineArn")
