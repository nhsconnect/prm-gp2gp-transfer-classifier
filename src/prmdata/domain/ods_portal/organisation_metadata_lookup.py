from typing import Dict, Iterator, Tuple

from prmdata.domain.ods_portal.organisation_lookup import OrganisationLookup
from prmdata.domain.ods_portal.organisation_metadata import OrganisationMetadata

YearNumber = int
MonthNumber = int
YearMonth = Tuple[YearNumber, MonthNumber]


class OrganisationMetadataLookup:
    def __init__(self, metadata_dict: Dict[YearMonth, OrganisationLookup]):
        self._metadata_dict = metadata_dict

    @classmethod
    def from_list(cls, datas: Iterator[Dict]):
        metadata_dict = {}
        for data in datas:
            organisation_metadata = OrganisationMetadata.from_dict(data)
            year = organisation_metadata.generated_on.year
            month = organisation_metadata.generated_on.month
            organisation_lookup = OrganisationLookup(
                organisation_metadata.practices, organisation_metadata.ccgs
            )
            metadata_dict[(year, month)] = organisation_lookup
        return cls(metadata_dict)

    def get_month_lookup(self, year_month: YearMonth) -> OrganisationLookup:
        return self._metadata_dict[year_month]
