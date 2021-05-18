from random import randrange

from prmdata.domain.ods_portal.models import PracticeDetails
from tests.builders.common import a_string


def an_asid_list():
    return [a_string() for _ in range(randrange(3))]


def build_practice_details(**kwargs):
    return PracticeDetails(
        ods_code=kwargs.get("ods_code", a_string()),
        name=kwargs.get("name", a_string()),
        asids=kwargs.get("asids", an_asid_list()),
    )
