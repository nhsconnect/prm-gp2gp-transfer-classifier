from datetime import datetime
from typing import Dict, Iterator

from dateutil.relativedelta import relativedelta

from prmdata.domain.ods_portal.organisation_lookup import OrganisationLookup, YearMonth
from prmdata.domain.ods_portal.organisation_metadata import OrganisationMetadata


class OrganisationMetadataMonthly:
    def __init__(self, metadata_dict: Dict[YearMonth, OrganisationLookup]):
        self._metadata_dict = metadata_dict

    @classmethod
    def from_list(cls, datas: Iterator[Dict]):
        metadata_dict = {}
        for data in datas:
            organisation_metadata = OrganisationMetadata.from_dict(data)
            year_month = (organisation_metadata.year, organisation_metadata.month)
            organisation_lookup = OrganisationLookup(
                organisation_metadata.practices, organisation_metadata.sicbls, year_month=year_month
            )
            metadata_dict[year_month] = organisation_lookup
        return cls(metadata_dict)

    def get_lookup(self, year_month: YearMonth) -> OrganisationLookup:
        try:
            return self._metadata_dict[year_month]
        except KeyError:
            previous_month_datetime = datetime(
                year=year_month[0], month=year_month[1], day=1
            ) - relativedelta(months=1)
            previous_year_month = (previous_month_datetime.year, previous_month_datetime.month)
            return self._metadata_dict[previous_year_month]
