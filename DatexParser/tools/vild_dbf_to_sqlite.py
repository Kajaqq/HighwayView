from __future__ import annotations

import argparse
import re
import sqlite3
import struct
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

DEFAULT_TABLE = "vild_locations"
TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
LOC_DESCRIPTION_TRANSLATIONS = {
    "Land": "Land",
    "Zee": "Sea",
    "Fuzzy gebied": "Fuzzy area",
    "Provincie": "Province",
    "RWS Regionale Dienst": "RWS Regional Service",
    "RWS Wegendistrict": "RWS Road District",
    "Plaats": "Town",
    "P&R terrein": "P&R area",
    "Parkeergebied": "Parking area",
    "Parkeerterrein": "Parking lot",
    "Gemeente": "Municipality",
    "Veerdienst": "Ferry service",
    "Orde 2 segment": "Order 2 segment",
    "Orde 1 segment": "Order 1 segment",
    "Snelweg": "Highway",
    "Ringweg": "Ring road",
    "Eerste klasse weg": "First class road",
    "Knooppunt (triangle)": "Junction (triangle)",
    "Verbindingsweg": "Connecting road/Slip road",
    "Afrit": "Exit",
    "Aquaduct": "Aqueduct",
    "Parkeerplaats (service)": "Parking lot (service)",
    "Parkeerplaats (rest)": "Parking lot (rest)",
    "Knooppunt": "Junction",
    "Brug": "Bridge",
    "Hectometersprong": "Hectometer jump",
    "Grensovergang": "Border crossing",
    "Tunnel": "Tunnel",
    "Tankstation": "Gas station",
    "Kruising": "Intersection",
    "Aansluiting": "Connection",
    "Sluis": "Lock",
    "Verkeersplein": "Roundabout junction",
    "Spoorwegovergang": "Railway crossing",
    "Veerterminal": "Ferry terminal",
    "Veer": "Ferry",
    "Bebouwde kom": "Built-up area",
    "Industriegebied": "Industrial area",
    "Tol": "Toll",
    "Stadsringweg": "City ring road",
    "Tweede klasse weg": "Second class road",
    "Haven": "Harbor/Port",
}
TEXT_TRANSLATIONS = {
    "vanuit": "from",
    "richting": "towards",
}


@dataclass(frozen=True, slots=True)
class DbfField:
    name: str
    field_type: str
    length: int


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert the VILD attribute DBF into a SQLite LOC_NR lookup table."
    )
    parser.add_argument("dbf_file", type=Path, help="Path to VILD6.x.y.dbf.")
    parser.add_argument(
        "sqlite_file", type=Path, help="SQLite database to create/update."
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help=f"Output table name (default: {DEFAULT_TABLE}).",
    )
    parser.add_argument(
        "--encoding",
        default="cp1252",
        help="DBF text encoding (default: cp1252).",
    )
    return parser.parse_args(argv)


def _read_dbf_fields(data: bytes, encoding: str) -> list[DbfField]:
    fields: list[DbfField] = []
    offset = 32

    while data[offset] != 0x0D:
        descriptor = data[offset : offset + 32]
        name = descriptor[:11].split(b"\x00", maxsplit=1)[0].decode(encoding).strip()
        fields.append(
            DbfField(
                name=name,
                field_type=chr(descriptor[11]),
                length=descriptor[16],
            )
        )
        offset += 32

    return fields


def iter_dbf_rows(path: Path, encoding: str = "cp1252") -> Iterator[dict[str, str]]:
    data = path.read_bytes()
    record_count = struct.unpack("<I", data[4:8])[0]
    header_length = struct.unpack("<H", data[8:10])[0]
    record_length = struct.unpack("<H", data[10:12])[0]
    fields = _read_dbf_fields(data, encoding)

    offset = header_length
    for _ in range(record_count):
        record = data[offset : offset + record_length]
        offset += record_length

        if not record or record[0:1] == b"*":
            continue

        row: dict[str, str] = {}
        field_offset = 1
        for field in fields:
            value = record[field_offset : field_offset + field.length]
            row[field.name] = value.decode(encoding, errors="replace").strip()
            field_offset += field.length
        yield row


def _int_or_none(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _km_or_none(value: str | None) -> float | None:
    hectometer = _int_or_none(value)
    if hectometer is None or hectometer < 0:
        return None
    return hectometer / 10


def _text_or_none(value: str | None) -> str | None:
    if not value:
        return None
    return value.strip() or None


def _translated_text_or_none(
    value: str | None,
    translations: dict[str, str] = TEXT_TRANSLATIONS,
) -> str | None:
    text = _text_or_none(value)
    if text is None:
        return None
    return translations.get(text, text)


def _join_unique(separator: str, *values: str | None) -> str | None:
    parts: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _text_or_none(value)
        if text is None or text in seen:
            continue
        seen.add(text)
        parts.append(text)
    return separator.join(parts) if parts else None


def build_lookup_rows(rows: list[dict[str, str]]) -> list[tuple]:
    rows_by_loc: dict[int, dict[str, str]] = {}
    for row in rows:
        loc_nr = _int_or_none(row.get("LOC_NR"))
        if loc_nr is not None:
            rows_by_loc[loc_nr] = row

    lookup_rows: list[tuple] = []
    for loc_nr, row in sorted(rows_by_loc.items()):
        area = rows_by_loc.get(_int_or_none(row.get("AREA_REF")) or 0, {})
        road_number = _text_or_none(row.get("ROADNUMBER"))
        road_name = _text_or_none(row.get("ROADNAME"))
        from_name = _translated_text_or_none(row.get("FIRST_NAME"))
        to_name = _translated_text_or_none(row.get("SECND_NAME"))

        lookup_rows.append(
            (
                loc_nr,
                road_number,
                road_name,
                _join_unique(" ", road_number, road_name),
                from_name,
                to_name,
                _join_unique(
                    " - ",
                    _translated_text_or_none(area.get("FIRST_NAME")),
                    _translated_text_or_none(area.get("SECND_NAME")),
                ),
                _text_or_none(row.get("LOC_TYPE")),
                _translated_text_or_none(
                    row.get("LOC_DES"),
                    LOC_DESCRIPTION_TRANSLATIONS,
                ),
                _int_or_none(row.get("AREA_REF")),
                _int_or_none(row.get("LIN_REF")),
                _km_or_none(row.get("HSTART_POS")),
                _km_or_none(row.get("HEND_POS")),
                _km_or_none(row.get("HSTART_NEG")),
                _km_or_none(row.get("HEND_NEG")),
            )
        )

    return lookup_rows


def write_sqlite(sqlite_file: Path, table: str, rows: list[tuple]) -> None:
    if TABLE_NAME_RE.fullmatch(table) is None:
        raise ValueError(f"Invalid SQLite table name: {table}")

    sqlite_file.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(sqlite_file) as db:
        db.execute(f"DROP TABLE IF EXISTS {table}")
        db.execute(
            f"""
            CREATE TABLE {table} (
                loc_nr INTEGER PRIMARY KEY,
                road_number TEXT,
                road_name TEXT,
                road_label TEXT,
                "from" TEXT,
                "to" TEXT,
                area_name TEXT,
                loc_type_id TEXT,
                loc_type TEXT,
                area_ref INTEGER,
                line_ref INTEGER,
                km_start_pos REAL,
                km_end_pos REAL,
                km_start_neg REAL,
                km_end_neg REAL
            )
            """
        )
        db.executemany(
            f"""
            INSERT INTO {table} (
                loc_nr,
                road_number,
                road_name,
                road_label,
                "from",
                "to",
                area_name,
                loc_type_id,
                loc_type,
                area_ref,
                line_ref,
                km_start_pos,
                km_end_pos,
                km_start_neg,
                km_end_neg
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        db.execute(f"CREATE INDEX idx_{table}_road_number ON {table}(road_number)")
        db.execute(f"CREATE INDEX idx_{table}_area_name ON {table}(area_name)")


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    rows = list(iter_dbf_rows(args.dbf_file, args.encoding))
    lookup_rows = build_lookup_rows(rows)
    write_sqlite(args.sqlite_file, args.table, lookup_rows)
    print(f"Wrote {len(lookup_rows)} VILD rows to {args.sqlite_file}")


if __name__ == "__main__":
    main()
