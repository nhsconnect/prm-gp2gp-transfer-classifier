from argparse import ArgumentParser


def parse_ods_portal_pipeline_arguments(args):
    parser = ArgumentParser(description="ODS portal pipeline")
    parser.add_argument("--output-file", type=str)
    return parser.parse_args(args)
