from random import randrange

from prmdata.domain.ods_portal.models import PracticeDetails, CcgDetails, OrganisationMetadata
from tests.builders.common import a_string, a_datetime


def an_asid_list():
    return [a_string() for _ in range(randrange(1, 3))]


def build_practice_details(**kwargs) -> PracticeDetails:
    return PracticeDetails(
        ods_code=kwargs.get("ods_code", a_string()),
        name=kwargs.get("name", a_string()),
        asids=kwargs.get("asids", an_asid_list()),
    )


def build_ccg_details(**kwargs) -> CcgDetails:
    return CcgDetails(
        ods_code=kwargs.get("ods_code", a_string()),
        name=kwargs.get("name", a_string()),
    )


def build_organisation_metadata():
    generated_on = a_datetime()
    return OrganisationMetadata(
        generated_on=generated_on, practices=[build_practice_details()], ccgs=[build_ccg_details()]
    )
