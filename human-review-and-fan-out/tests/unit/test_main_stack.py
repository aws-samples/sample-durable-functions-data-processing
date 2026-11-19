import aws_cdk as core
import aws_cdk.assertions as assertions
import pytest

from distributed_functions_for_data.main_stack import DistributedFunctionsForDataStack


_STACK_NAME = "test-stack"


def _template() -> assertions.Template:
    # Skip asset bundling during tests: synth still emits the template with
    # placeholder assets, which is all the resource assertions below need.
    app = core.App(context={"aws:cdk:bundling-stacks": []})
    stack = DistributedFunctionsForDataStack(app, _STACK_NAME)
    return assertions.Template.from_stack(stack)


# Physical names derive from the stack name.
_DURABLE_FUNCTION_NAMES = {
    f"{_STACK_NAME}-flight-map-enrichment",
    f"{_STACK_NAME}-flight-claims",
}


def test_resource_names_derive_from_stack_name():
    template = _template()
    template.has_resource_properties(
        "AWS::Lambda::Function",
        {"FunctionName": f"{_STACK_NAME}-airport-reference-api"},
    )
    template.has_resource_properties(
        "AWS::SQS::Queue",
        {"QueueName": f"{_STACK_NAME}-flight-claims-human-review"},
    )
    template.has_resource_properties(
        "AWS::DynamoDB::Table",
        {"TableName": f"{_STACK_NAME}-flight-claims-status"},
    )


def test_overlong_stack_name_is_rejected():
    app = core.App(context={"aws:cdk:bundling-stacks": []})
    with pytest.raises(ValueError, match="too long"):
        DistributedFunctionsForDataStack(app, "x" * 43)


def test_durable_functions_created():
    template = _template()
    # Only durable functions carry DurableConfig; this excludes the plain
    # Airport API function and CDK's auto-delete-objects provider.
    durable = template.find_resources(
        "AWS::Lambda::Function",
        {"Properties": {"DurableConfig": assertions.Match.any_value()}},
    )
    names = {r["Properties"]["FunctionName"] for r in durable.values()}
    assert names == _DURABLE_FUNCTION_NAMES


def test_functions_use_python_314_on_arm64():
    template = _template()
    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Runtime": "python3.14",
            "Architectures": ["arm64"],
            "Handler": "index.lambda_handler",
        },
    )


def test_durable_execution_enabled():
    template = _template()
    # DurableConfig is what enables durable execution on the function.
    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "DurableConfig": assertions.Match.object_like(
                {"ExecutionTimeout": 3600}
            )
        },
    )


_REGISTRY_NAME = "lambda-testevent-schemas"
_CONDITION = "ShouldCreateTestEventRegistry"


def test_test_event_registry_creation_is_opt_in():
    template = _template()
    # The Lambda console creates `lambda-testevent-schemas` lazily, so a fresh
    # account won't have it and the saved test events fail to deploy with
    # "Resource of type 'AWS::EventSchemas::Schema' with identifier
    # 'lambda-testevent-schemas' was not found." CloudFormation can't query
    # whether it exists, so creating it is an explicit opt-in that defaults to
    # off (an account that already has the registry would fail on create).
    template.has_parameter(
        "CreateTestEventRegistry",
        {"Default": "false", "AllowedValues": ["true", "false"]},
    )
    template.has_condition(
        _CONDITION,
        {"Fn::Equals": [{"Ref": "CreateTestEventRegistry"}, "true"]},
    )
    registries = template.find_resources("AWS::EventSchemas::Registry")
    assert len(registries) == 1, f"expected exactly one registry, got {list(registries)}"
    (registry,) = registries.values()
    assert registry["Properties"]["RegistryName"] == _REGISTRY_NAME
    assert registry.get("Condition") == _CONDITION, (
        "registry must be gated on the opt-in condition"
    )


def test_test_event_schemas_resolve_registry_in_both_branches():
    template = _template()
    # The schemas are always created, so their RegistryName has to be valid
    # whether or not we created the registry: GetAtt when we did (which also
    # orders them after it), and the literal name when we didn't — a GetAtt
    # against a condition-false resource is invalid.
    schemas = template.find_resources("AWS::EventSchemas::Schema")
    assert schemas, "expected at least one saved console test event"
    for logical_id, schema in schemas.items():
        assert schema.get("Condition") is None, (
            f"{logical_id} should be created regardless of the registry opt-in"
        )
        assert schema["Properties"]["RegistryName"] == {
            "Fn::If": [
                _CONDITION,
                {"Fn::GetAtt": ["LambdaTestEventRegistry", "RegistryName"]},
                _REGISTRY_NAME,
            ]
        }, f"{logical_id} registry reference is not branch-safe"


def test_version_and_alias_per_function():
    template = _template()
    # Durable functions require a qualified ARN (version + alias) to invoke.
    template.resource_count_is("AWS::Lambda::Version", len(_DURABLE_FUNCTION_NAMES))
    template.resource_count_is("AWS::Lambda::Alias", len(_DURABLE_FUNCTION_NAMES))
    template.has_resource_properties(
        "AWS::Lambda::Alias", {"Name": "prod"}
    )


def test_checkpoint_managed_policy_attached():
    template = _template()
    # The execution role must carry the durable-execution checkpoint policy.
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "ManagedPolicyArns": assertions.Match.array_with(
                [
                    {
                        "Fn::Join": [
                            "",
                            assertions.Match.array_with(
                                [
                                    assertions.Match.string_like_regexp(
                                        "AWSLambdaBasicDurableExecutionRolePolicy"
                                    )
                                ]
                            ),
                        ]
                    }
                ]
            )
        },
    )


def test_inline_policies_use_no_wildcard_actions():
    template = _template()
    # Least privilege: every inline policy names its actions explicitly.
    for logical_id, policy in template.find_resources("AWS::IAM::Policy").items():
        for statement in policy["Properties"]["PolicyDocument"]["Statement"]:
            actions = statement["Action"]
            actions = [actions] if isinstance(actions, str) else actions
            wildcards = [a for a in actions if "*" in a]
            assert not wildcards, f"{logical_id} grants wildcard actions {wildcards}"
