import json
import shutil
import subprocess  # nosec B404 - runs pip with a fixed argument list for local bundling
from pathlib import Path

import jsii
from aws_cdk import (
    BundlingOptions,
    CfnCondition,
    CfnOutput,
    CfnParameter,
    Duration,
    Fn,
    ILocalBundling,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_eventschemas as eventschemas,
    aws_sqs as sqs,
)
from constructs import Construct

# Target runtime/architecture for all functions. Durable functions require
# Python 3.13+; these run on Python 3.14 / ARM64 (Graviton).
_RUNTIME = lambda_.Runtime.PYTHON_3_14
_ARCHITECTURE = lambda_.Architecture.ARM_64

# pip's manylinux platform tag and CPython "python-version" for the target,
# derived from _RUNTIME/_ARCHITECTURE so they can't drift apart. Resolving
# wheels for the target (not the build host) lets us bundle on macOS/x86 while
# still pulling Linux wheels for the Lambda architecture.
_PIP_MACHINE_BY_ARCHITECTURE = {
    lambda_.Architecture.ARM_64.name: "aarch64",
    lambda_.Architecture.X86_64.name: "x86_64",
}
_PIP_PLATFORM = f"manylinux2014_{_PIP_MACHINE_BY_ARCHITECTURE[_ARCHITECTURE.name]}"
# Runtime.name is e.g. "python3.14" -> "3.14".
_PIP_PYTHON_VERSION = _RUNTIME.name.removeprefix("python")

# Shared requirements file (the durable execution SDK) bundled into every
# durable function's deployment package.
_LAMBDA_REQUIREMENTS = "lambda/requirements.txt"

# The EventBridge schema registry the Lambda console reads its saved/shareable
# "Test" events from. The console creates it lazily, so it may or may not
# already exist in a given account/region.
_TEST_EVENT_REGISTRY_NAME = "lambda-testevent-schemas"


def _bundling_command(extra_packages: tuple[str, ...]) -> str:
    """Docker-image bundling command: install shared reqs (+ extras), then copy source.

    The image already matches the target architecture, so a plain install pulls
    the right wheels.
    """
    parts = ["pip install -r /asset-input/../requirements.txt -t /asset-output"]
    if extra_packages:
        parts.append(f"pip install {' '.join(extra_packages)} -t /asset-output")
    parts.append("cp -au /asset-input/. /asset-output")
    return " && ".join(parts)


@jsii.implements(ILocalBundling)
class _LocalPipBundling:
    """Bundle a function locally (no Docker) by pip-installing deps + copying source.

    CDK calls ``try_bundle`` first; returning True skips the Docker image entirely.
    Because the local host arch may differ from the target (e.g. macOS/x86 vs
    Lambda ARM64), pip is told to fetch wheels for the *target* platform rather
    than the build host.
    """

    def __init__(
        self,
        source_dir: Path,
        requirements: Path,
        extra_packages: tuple[str, ...] = (),
    ) -> None:
        self._source_dir = source_dir
        self._requirements = requirements
        self._extra_packages = extra_packages

    def _pip_install(self, args: list[str], output_dir: str) -> None:
        # Static argument list with no shell. "python" is the active virtual
        # environment's interpreter (the README activates .venv before cdk);
        # the remaining inputs are this project's own requirements file and
        # package names.
        subprocess.run(  # nosec B603 B607
            [
                "python", "-m", "pip", "install",
                *args,
                "-t", output_dir,
                # Cross-resolve wheels for the Lambda target, not the build host.
                "--platform", _PIP_PLATFORM,
                "--implementation", "cp",
                "--python-version", _PIP_PYTHON_VERSION,
                # Required: with a non-native --platform, pip can only use wheels.
                "--only-binary=:all:",
            ],
            check=True,
        )

    def try_bundle(self, output_dir: str, *, image=None, **_kwargs) -> bool:  # noqa: ARG002
        self._pip_install(["-r", str(self._requirements)], output_dir)
        if self._extra_packages:
            self._pip_install(list(self._extra_packages), output_dir)
        # Copy the function source into the output root.
        shutil.copytree(self._source_dir, output_dir, dirs_exist_ok=True)
        return True


class DistributedFunctionsForDataStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Created on first use by _test_event_registry(); shared by every
        # saved console test event.
        self._test_event_registry_name_token: str | None = None

        # Physical resource names derive from the stack name, so deploying
        # under a different stack name produces correspondingly named
        # functions, queue, and table.
        prefix = self.stack_name
        # The longest derived function name adds 22 characters to the prefix,
        # and Lambda function names are limited to 64 characters.
        if len(prefix) > 64 - len("-airport-reference-api"):
            raise ValueError(
                f"Stack name '{prefix}' is too long: derived Lambda function "
                "names would exceed 64 characters. Use a stack name of 42 "
                "characters or fewer."
            )

        # Shared data lake for flight input, S3-offloaded chunks, enriched
        # output, and run manifests.
        data_bucket = s3.Bucket(
            self,
            "FlightDataBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            # Demo bucket: tear down cleanly with the stack.
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # The mock "downstream" Airport reference API. Low reserved concurrency
        # so the map's max_concurrency knob has something real to protect: dial
        # it too high and the Function URL starts throttling (HTTP 429), which
        # the enrichment step's retry strategy then absorbs.
        airport_api = lambda_.Function(
            self,
            "AirportApi",
            function_name=f"{prefix}-airport-reference-api",
            runtime=_RUNTIME,
            architecture=_ARCHITECTURE,
            handler="index.lambda_handler",
            timeout=Duration.seconds(15),
            memory_size=256,
            reserved_concurrent_executions=5,
            # Plain asset: stdlib + bundled airports.json only, no pip needed.
            code=lambda_.Code.from_asset("lambda/airport_api"),
            environment={
                # Demo knob: simulate a slow downstream (ms); defaults to off.
                "DEMO_LATENCY_MS": "0",
            },
        )
        airport_api_url = airport_api.add_function_url(
            # Secure by default: callers must SigV4-sign as the invoking role.
            # For a throwaway public demo you could use FunctionUrlAuthType.NONE.
            auth_type=lambda_.FunctionUrlAuthType.AWS_IAM,
        )
        CfnOutput(
            self,
            "AirportApiUrl",
            value=airport_api_url.url,
            description="Function URL of the mock Airport reference API (AWS_IAM auth)",
        )
        CfnOutput(
            self,
            "AirportApiFunctionName",
            value=airport_api.function_name,
            description="Name of the mock Airport reference API function",
        )

        # Durable, high-throughput flight enrichment using the MAP operation.
        # Reads/writes the data bucket, invokes the Airport API, and gets a
        # ready-to-run console "Test" event.
        enrichment_function_name = f"{prefix}-flight-map-enrichment"
        enrichment = self._durable_function(
            construct_id="LargeScaleProcessing",
            function_name=enrichment_function_name,
            source_dir="lambda/large_scale_processing",
            memory_size=1024,
            timeout=Duration.minutes(5),
            # boto3 for S3 + SigV4 signing; tzdata so zoneinfo has the IANA
            # db regardless of what the base image ships.
            extra_packages=("boto3", "tzdata"),
            environment={
                "DATA_BUCKET": data_bucket.bucket_name,
                "AIRPORT_API_URL": airport_api_url.url,
            },
        )
        # Least privilege: the pipeline only reads and writes objects.
        enrichment.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject"],
                resources=[data_bucket.arn_for_objects("*")],
            )
        )
        airport_api.grant_invoke_url(enrichment)

        # A ready-to-run "Test" event in the Lambda console, so the function
        # can be invoked from a dropdown instead of pasting JSON.
        self._saved_test_event(
            construct_id="EnrichmentTestEvent",
            function_name=enrichment_function_name,
            event_name="flights-1000-conc8",
            payload={
                "input_key": "input/flights.jsonl",
                "chunk_size": 1000,
                "max_concurrency": 8,
            },
        )

        # Second durable example: flight delay/cancellation compensation claims
        # (error handling, parallel checks on a single item, branching, callback
        # pattern) — efficiency at the single-unit-of-work level, which precedes
        # fanning that same unit out across millions of them (see the enrichment
        # example above).
        #
        # Queue where the claims workflow publishes claims that need human
        # review, along with the durable-execution callback id. An adjudicator
        # (or a review UI/worker) consumes from this queue and resolves the
        # execution via SendDurableExecutionCallbackSuccess/Failure.
        claim_review_queue = sqs.Queue(
            self,
            "ClaimReviewQueue",
            queue_name=f"{prefix}-flight-claims-human-review",
            # Reviews can take a while; give the workflow room before timeout.
            retention_period=Duration.days(14),
            visibility_timeout=Duration.minutes(5),
            enforce_ssl=True,
        )

        # Per-claim status, keyed by claim id. Each workflow step records where
        # the claim is (extracted, pending_review, approved, rejected, ...) so
        # its progress is queryable outside the durable execution history.
        claim_status_table = dynamodb.Table(
            self,
            "ClaimStatusTable",
            table_name=f"{prefix}-flight-claims-status",
            partition_key=dynamodb.Attribute(
                name="claim_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
            # Demo table: tear down cleanly with the stack.
            removal_policy=RemovalPolicy.DESTROY,
        )

        # The claims workflow enqueues review requests and records status.
        flight_claims = self._durable_function(
            construct_id="FlightClaims",
            function_name=f"{prefix}-flight-claims",
            source_dir="lambda/flight_claims",
            environment={
                "CLAIM_REVIEW_QUEUE_URL": claim_review_queue.queue_url,
                "CLAIM_TABLE_NAME": claim_status_table.table_name,
                # Large step results (the customer flight history) are
                # offloaded here; only the S3 key is checkpointed.
                "DATA_BUCKET": data_bucket.bucket_name,
            },
        )
        claim_review_queue.grant_send_messages(flight_claims)
        # Least privilege: _record_status only calls UpdateItem.
        flight_claims.add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:UpdateItem"],
                resources=[claim_status_table.table_arn],
            )
        )
        # Scoped to the offload prefix (matches FLIGHT_HISTORY_PREFIX); the
        # step writes the history and the duplicate check reads it back.
        flight_claims.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject"],
                resources=[data_bucket.arn_for_objects("flight-history/*")],
            )
        )

        CfnOutput(
            self,
            "ClaimStatusTableName",
            value=claim_status_table.table_name,
            description="DynamoDB table holding each claim's status, keyed by claim id",
        )

        CfnOutput(
            self,
            "ClaimReviewQueueUrl",
            value=claim_review_queue.queue_url,
            description="Queue that receives human-review requests (with callback ids)",
        )

        CfnOutput(
            self,
            "FlightDataBucketName",
            value=data_bucket.bucket_name,
            description="S3 bucket for flight input, chunks, enriched output, and manifests",
        )
        CfnOutput(
            self,
            "GenerateAndUploadCommand",
            value=(
                "python scripts/generate_flights.py --count 100000 --seed 42 "
                f"--bucket {data_bucket.bucket_name} --key input/flights.jsonl"
            ),
            description="Generate a sample dataset and upload it to the data bucket",
        )

    def _test_event_registry_name(self) -> str:
        """Registry name for the console's saved test events — created on opt-in.

        The console creates ``lambda-testevent-schemas`` lazily, the first time
        someone saves a shareable test event in that account/region. In a fresh
        account it does not exist, and a CfnSchema pointing at a missing
        registry fails the deploy with:

            Resource of type 'AWS::EventSchemas::Schema' with identifier
            'lambda-testevent-schemas' was not found.

        CloudFormation cannot ask whether a resource already exists — Conditions
        are evaluated from stack inputs, not live account state — so this is an
        explicit opt-in. On the first deploy into an account/region that has
        never saved a shareable console test event:

            cdk deploy --parameters CreateTestEventRegistry=true

        It defaults to false so that deploying into an account that already has
        the registry doesn't fail with "already exists", and so `cdk destroy`
        never deletes a registry that may hold other functions' test events.
        (CloudFormation reuses a parameter's previous value on later updates, so
        you only pass it once per stack.)
        """
        if self._test_event_registry_name_token is None:
            create_registry = CfnParameter(
                self,
                "CreateTestEventRegistry",
                type="String",
                allowed_values=["true", "false"],
                default="false",
                description=(
                    f"Create the {_TEST_EVENT_REGISTRY_NAME} schema registry that "
                    "backs the Lambda console's saved test events. Set to true on "
                    "the first deploy into an account/region where the console has "
                    "never saved a shareable test event; leave false if the "
                    "registry already exists."
                ),
            )
            should_create = CfnCondition(
                self,
                "ShouldCreateTestEventRegistry",
                expression=Fn.condition_equals(create_registry.value_as_string, "true"),
            )
            registry = eventschemas.CfnRegistry(
                self,
                "LambdaTestEventRegistry",
                registry_name=_TEST_EVENT_REGISTRY_NAME,
                description="Shareable Lambda console test events",
            )
            registry.cfn_options.condition = should_create

            # Fn::GetAtt against a resource whose condition is false is invalid,
            # so fall back to the literal name in that case. When the condition
            # is true the GetAtt also makes CloudFormation order the schemas
            # after the registry.
            self._test_event_registry_name_token = Fn.condition_if(
                should_create.logical_id,
                registry.attr_registry_name,
                _TEST_EVENT_REGISTRY_NAME,
            ).to_string()
        return self._test_event_registry_name_token

    def _saved_test_event(
        self,
        *,
        construct_id: str,
        function_name: str,
        event_name: str,
        payload: dict,
    ) -> None:
        """Create a shareable Lambda console "Test" event for a function.

        The Lambda console reads saved/shareable test events from an EventBridge
        Schema Registry named ``lambda-testevent-schemas``; each event is an
        OpenApi3 schema named ``_<function-name>-schema``. The console only lists
        an event when the schema defines BOTH an ``components.schemas.Event``
        object (the field types) AND a matching entry under
        ``components.examples`` (the example's ``value`` is the invoke payload).
        We reproduce exactly the shape the console itself writes so the event
        shows up in the Test dropdown, ready to run.

        Note: a given schema holds all events for one function. If several
        callers target the same function they must share one schema; here each
        function has a single event, so one schema per function is fine.
        """
        # Derive a JSON-schema type for each top-level payload field — the
        # console's own saved events carry this schemas.Event block, and the
        # Test dropdown keys off it to recognize the event.
        def _json_type(value) -> str:
            if isinstance(value, bool):
                return "boolean"
            if isinstance(value, (int, float)):
                return "number"
            if isinstance(value, list):
                return "array"
            if isinstance(value, dict):
                return "object"
            return "string"

        schema_content = {
            "openapi": "3.0.0",
            "info": {"version": "1.0.0", "title": "Event"},
            "paths": {},
            "components": {
                "schemas": {
                    "Event": {
                        "type": "object",
                        "required": list(payload.keys()),
                        "properties": {
                            key: {"type": _json_type(val)}
                            for key, val in payload.items()
                        },
                    },
                },
                "examples": {
                    event_name: {
                        "value": payload,
                        "x-metadata": {"invocationType": "RequestResponse"},
                    },
                },
            },
        }
        eventschemas.CfnSchema(
            self,
            construct_id,
            # Reference the registry we create, so CloudFormation creates it
            # first instead of assuming the console already did.
            registry_name=self._test_event_registry_name(),
            type="OpenApi3",
            schema_name=f"_{function_name}-schema",
            description=f"Saved Lambda console test event(s) for {function_name}",
            content=json.dumps(schema_content),
        )

    def _durable_function(
        self,
        *,
        construct_id: str,
        function_name: str,
        source_dir: str,
        memory_size: int = 512,
        timeout: Duration = Duration.seconds(30),
        extra_packages: tuple[str, ...] = (),
        environment: dict[str, str] | None = None,
    ) -> lambda_.Function:
        """Create a durable Lambda function with a published version and alias.

        Returns the underlying Function so callers can attach IAM grants; a
        ``prod`` alias is published and its ARN emitted as a stack output.
        """
        function = lambda_.Function(
            self,
            construct_id,
            function_name=function_name,
            runtime=_RUNTIME,
            architecture=_ARCHITECTURE,
            handler="index.lambda_handler",
            timeout=timeout,
            memory_size=memory_size,
            environment=environment,
            code=lambda_.Code.from_asset(
                source_dir,
                bundling=BundlingOptions(
                    image=_RUNTIME.bundling_image,
                    command=["bash", "-c", _bundling_command(extra_packages)],
                    local=_LocalPipBundling(
                        source_dir=Path(source_dir).resolve(),
                        requirements=Path(_LAMBDA_REQUIREMENTS).resolve(),
                        extra_packages=extra_packages,
                    ),
                ),
            ),
            # Enable durable execution. This can only be set at creation time.
            durable_config=lambda_.DurableConfig(
                execution_timeout=Duration.hours(1),
                retention_period=Duration.days(14),
            ),
        )

        # Grant the checkpoint permissions durable execution requires.
        function.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicDurableExecutionRolePolicy"
            )
        )

        # Durable functions must be invoked via a qualified ARN (version/alias).
        alias = lambda_.Alias(
            self,
            f"{construct_id}Alias",
            alias_name="prod",
            version=function.current_version,
        )

        CfnOutput(
            self,
            f"{construct_id}AliasArn",
            value=alias.function_arn,
            description=f"Qualified ARN to invoke the {function_name} durable function",
        )
        CfnOutput(
            self,
            f"{construct_id}FunctionName",
            value=function.function_name,
            description=f"Name of the {function_name} durable function",
        )

        return function
