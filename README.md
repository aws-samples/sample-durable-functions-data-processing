# Data processing with AWS Lambda durable functions

Samples that use AWS Lambda durable functions to build fault-tolerant data
processing. A durable execution checkpoints each step, so a workflow can
retry failed work, wait for an external event or a person without paying for
idle compute, and resume after an interruption without redoing completed
steps. Each sample deploys with the AWS CDK and runs end to end in an AWS
account.

These samples are for learning and are not intended for production use
without further review.

## Samples

| Sample | Patterns |
| --- | --- |
| [human-review-and-fan-out](human-review-and-fan-out/) | A single-item workflow with step retries, parallel checks, and a human-review callback that suspends without compute charges. Fan-out/fan-in batch processing with the `map` operation, failure isolation, concurrency control, and a retry pass over only the failed items. |

Each sample folder is a self-contained CDK project with its own README,
dependencies, and tests.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more
information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE)
file.
