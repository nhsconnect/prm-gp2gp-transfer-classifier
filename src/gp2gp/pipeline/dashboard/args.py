from argparse import ArgumentParser


def _list_str(values):
    return values.split(",")


def parse_dashboard_pipeline_arguments(argument_list):
    parser = ArgumentParser(description="GP2GP Service dashboard data pipeline")
    parser.add_argument("--month", type=int)
    parser.add_argument("--year", type=int)
    parser.add_argument("--practice-list-file", type=str)
    parser.add_argument("--input-files", type=_list_str)
    parser.add_argument("--output-bucket", type=str)

    metrics_output_group = parser.add_mutually_exclusive_group(required=True)
    metadata_output_group = parser.add_mutually_exclusive_group(required=True)

    metrics_output_group.add_argument("--practice-metrics-output-file", type=str)
    metrics_output_group.add_argument("--practice-metrics-output-key", type=str)

    metadata_output_group.add_argument("--practice-metadata-output-file", type=str)
    metadata_output_group.add_argument("--practice-metadata-output-key", type=str)

    args = parser.parse_args(argument_list)

    if args.practice_metadata_output_key is not None and args.output_bucket is None:
        parser.error("--practice-metadata-output-key requires --output-bucket to be set")

    if args.practice_metrics_output_key is not None and args.output_bucket is None:
        parser.error("--practice-metrics-output-key requires --output-bucket to be set")

    return args
