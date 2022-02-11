from typing import Dict, Iterator, Tuple

from prmdata.domain.ods_portal.organisation_metadata import OrganisationMetadata

YearNumber = int
MonthNumber = int
YearMonth = Tuple[YearNumber, MonthNumber]


class OrganisationMetadataLookup:
    def __init__(self, metadata_dict: Dict[YearMonth, OrganisationMetadata]):
        self._metadata_dict = metadata_dict

    @classmethod
    def from_list(cls, datas: Iterator[Dict]):
        metadata_dict = {}
        for data in datas:
            organisation_metadata = OrganisationMetadata.from_dict(data)
            year = organisation_metadata.generated_on.year
            month = organisation_metadata.generated_on.month
            metadata_dict[(year, month)] = organisation_metadata
        return cls(metadata_dict)

    def get_month_metadata(self, year_month: YearMonth) -> OrganisationMetadata:
        return self._metadata_dict[year_month]
