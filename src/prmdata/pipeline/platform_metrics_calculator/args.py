from argparse import ArgumentParser


def _list_str(values):
    return values.split(",")


def parse_platform_metrics_calculator_pipeline_arguments(argument_list):
    parser = ArgumentParser(description="GP2GP Data Platform pipeline")
    parser.add_argument("--month", type=int, required=True, help="The target month.")
    parser.add_argument("--year", type=int, required=True, help="The target year.")
    parser.add_argument(
        "--organisation-list-file",
        type=str,
        required=True,
        help="The list of organisations you want to generate metrics for. \
        This list is the output of the ODS portal pipeline.",
    )
    parser.add_argument(
        "--input-files",
        type=_list_str,
        required=True,
        help="The spine data file(s) used for analysis. \
        These files must be gzipped. Separate files with ','.",
    )
    parser.add_argument(
        "--s3-endpoint-url",
        type=str,
        required=False,
        help="The endpoint used to upload output data (optional).",
    )

    output_group = parser.add_mutually_exclusive_group(required=True)

    output_group.add_argument(
        "--output-bucket", type=str, help="The S3 bucket where the output data will be uploaded."
    )
    output_group.add_argument(
        "--output-directory",
        type=str,
        help="The local directory where the output data will be saved.",
    )

    args = parser.parse_args(argument_list)

    return args
