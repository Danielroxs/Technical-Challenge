REQUIRED_FIELDS = (
    "period",
    "facility",
    "facilityName",
    "capacity",
    "outage",
    "percentOutage",
)


def validate_required_fields(
    record: dict,
    required_fields: tuple[str, ...] = REQUIRED_FIELDS,
) -> tuple[bool, list[str]]:
    missing_fields = [
        field
        for field in required_fields
        if field not in record or record[field] in (None, "", [])
    ]

    return len(missing_fields) == 0, missing_fields


def validate_records(records: list[dict]) -> tuple[list[dict], list[dict]]:
    valid_records = []
    invalid_records = []

    for record in records:
        is_valid, missing_fields = validate_required_fields(record)

        if is_valid:
            valid_records.append(record)
        else:
            invalid_records.append(
                {
                    "record": record,
                    "missing_fields": missing_fields,
                }
            )

    return valid_records, invalid_records