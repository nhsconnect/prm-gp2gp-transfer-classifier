from gp2gp.odsportal.sources import construct_asid_to_ods_mappings


def test_returns_dict_with_one_asid_mapping():
    data = [
        {
            "ASID": "123456789123",
            "NACS": "A12345",
            "OrgName": "A GP",
            "MName": "A Supplier",
            "PName": "A system",
            "OrgType": "GP Practice",
            "PostCode": "X12 2TB",
        }
    ]

    expected = {"A12345": ["123456789123"]}
    actual = construct_asid_to_ods_mappings(data)

    assert actual == expected


def test_returns_dict_with_multiple_asid_mappings():
    data = [
        {
            "ASID": "223456789123",
            "NACS": "B12345",
            "OrgName": "A GP",
            "MName": "A Supplier",
            "PName": "A system",
            "OrgType": "GP Practice",
            "PostCode": "X42 2TB",
        },
        {
            "ASID": "323456789123",
            "NACS": "C12345",
            "OrgName": "Another GP",
            "MName": "A Supplier",
            "PName": "A system",
            "OrgType": "GP Practice",
            "PostCode": "X45 2TB",
        },
        {
            "ASID": "023456789123",
            "NACS": "D12345",
            "OrgName": "GP Three",
            "MName": "A Supplier",
            "PName": "A system",
            "OrgType": "GP Practice",
            "PostCode": "X78 2TB",
        },
    ]

    expected = {"B12345": ["223456789123"], "C12345": ["323456789123"], "D12345": ["023456789123"]}
    actual = construct_asid_to_ods_mappings(data)

    assert actual == expected


def test_returns_dict_with_one_practice_with_multiple_asids():
    data = [
        {
            "ASID": "123456789123",
            "NACS": "A12345",
            "OrgName": "A GP",
            "MName": "A Supplier",
            "PName": "A system",
            "OrgType": "GP Practice",
            "PostCode": "X12 2TB",
        },
        {
            "ASID": "8765456789123",
            "NACS": "A12345",
            "OrgName": "A GP",
            "MName": "A Supplier",
            "PName": "A system",
            "OrgType": "GP Practice",
            "PostCode": "X12 2TB",
        },
    ]

    expected = {"A12345": ["123456789123", "8765456789123"]}
    actual = construct_asid_to_ods_mappings(data)

    assert actual == expected
