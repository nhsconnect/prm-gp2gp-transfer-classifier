from typing import TextIO

from gp2gp.io.write_as_json import write_as_json
from gp2gp.odsportal.models import PracticeList


def write_practice_list_json(practice_list_data: PracticeList, outfile: TextIO):
    write_as_json(practice_list_data, outfile)
