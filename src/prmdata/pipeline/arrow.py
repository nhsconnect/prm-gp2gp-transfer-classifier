from typing import Callable, Generic, Iterable, List, Tuple, TypeVar

import pyarrow as pa
from pyarrow import DataType, Table

from prmdata.domain.gp2gp.transfer import Transfer

Value = TypeVar("Value")


class Column(Generic[Value]):
    def __init__(self, name: str, data_type: DataType, read_value: Callable[[Transfer], Value]):
        self._name = name
        self._data_type = data_type
        self._items: List[Value] = []
        self._read_value = read_value

    def add(self, transfer: Transfer):
        value = self._read_value(transfer)
        self._items.append(value)

    def data(self) -> Tuple[str, List]:
        return self._name, self._items

    def schema(self) -> Tuple[str, DataType]:
        return self._name, self._data_type


def _int_list():
    return pa.list_(pa.int64())


def _transfer_columns():
    return [
        Column("conversation_id", pa.string(), lambda t: t.conversation_id),
        Column("sla_duration", pa.uint64(), lambda t: t.sla_duration_seconds),
        Column("requesting_practice_asid", pa.string(), lambda t: t.requesting_practice.asid),
        Column(
            "requesting_practice_ods_code", pa.string(), lambda t: t.requesting_practice.ods_code
        ),
        Column("requesting_practice_name", pa.string(), lambda t: t.requesting_practice.name),
        Column(
            "requesting_practice_sicbl_ods_code",
            pa.string(),
            lambda t: t.requesting_practice.sicbl_ods_code,
        ),
        Column(
            "requesting_practice_sicbl_name",
            pa.string(),
            lambda t: t.requesting_practice.sicbl_name,
        ),
        Column("sending_practice_asid", pa.string(), lambda t: t.sending_practice.asid),
        Column("sending_practice_ods_code", pa.string(), lambda t: t.sending_practice.ods_code),
        Column("sending_practice_name", pa.string(), lambda t: t.sending_practice.name),
        Column(
            "sending_practice_sicbl_ods_code",
            pa.string(),
            lambda t: t.sending_practice.sicbl_ods_code,
        ),
        Column("sending_practice_sicbl_name", pa.string(), lambda t: t.sending_practice.sicbl_name),
        Column("requesting_supplier", pa.string(), lambda t: t.requesting_practice.supplier),
        Column("sending_supplier", pa.string(), lambda t: t.sending_practice.supplier),
        Column("sender_error_codes", _int_list(), lambda t: t.sender_error_codes),
        Column("final_error_codes", _int_list(), lambda t: t.final_error_codes),
        Column("intermediate_error_codes", _int_list(), lambda t: t.intermediate_error_codes),
        Column("status", pa.string(), lambda t: t.status_description),
        Column("failure_reason", pa.string(), lambda t: t.failure_reason),
        Column("date_requested", pa.timestamp("us"), lambda t: t.date_requested),
        Column("date_completed", pa.timestamp("us"), lambda t: t.date_completed),
        Column(
            "last_sender_message_timestamp",
            pa.timestamp("us"),
            lambda t: t.last_sender_message_timestamp,
        ),
    ]


def convert_transfers_to_table(transfers: Iterable[Transfer]) -> Table:
    columns = _transfer_columns()

    for transfer in transfers:
        for column in columns:
            column.add(transfer)

    return pa.table(
        data=dict(column.data() for column in columns),
        schema=pa.schema(column.schema() for column in columns),
    )
