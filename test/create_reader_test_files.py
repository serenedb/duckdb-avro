import datetime
import decimal
import json
import os
import struct
import uuid
import warnings
import zlib

import avro.codecs
import avro.errors
import avro.io
import avro.schema
import snappy
import zstandard
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from avro.timezones import utc


warnings.simplefilter("ignore", avro.errors.IgnoredLogicalType)

SYNC = bytes.fromhex("5d4e1ac0ffee7a1b2c3d4e5f60718293")
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=utc)

os.makedirs("reader", exist_ok=True)


def path(name):
    return f"reader/{name}.avro"


def strip_logical_types(schema):
    if isinstance(schema, dict):
        return {key: strip_logical_types(value) for key, value in schema.items() if key != "logicalType"}
    if isinstance(schema, list):
        return [strip_logical_types(value) for value in schema]
    return schema


def write(name, json_schema, records, codec="null", sync_interval=None, raw_values=False, metadata=None):
    data_schema = json.dumps(strip_logical_types(json.loads(json_schema))) if raw_values else json_schema
    writer = DataFileWriter(open(path(name), "wb"), DatumWriter(), avro.schema.parse(data_schema), codec=codec)
    writer.sync_marker = SYNC
    writer.set_meta("avro.schema", json_schema.encode())
    for key, value in (metadata or {}).items():
        writer.set_meta(key, value)
    for record in records:
        writer.append(record)
        if sync_interval is not None and writer.buffer_writer.tell() >= sync_interval:
            writer.sync()
    writer.close()


def read(name, raw_values=False):
    reader = DataFileReader(open(path(name), "rb"), DatumReader())
    if raw_values:
        reader.datum_reader.writers_schema = avro.schema.parse(
            json.dumps(strip_logical_types(json.loads(reader.schema)))
        )
    records = list(reader)
    reader.close()
    return records


def show(name, raw_values=False):
    records = read(name, raw_values)
    print(f"{path(name)}: {len(records)} records")
    for record in records:
        print(f"  {record}")
    return records


def show_failure(name):
    try:
        records = read(name)
    except Exception as error:
        print(f"{path(name)}: avro fails with {type(error).__name__}: {(str(error).splitlines() or [''])[0]}")
        return
    raise AssertionError(f"{path(name)} reads back {len(records)} records")


def branch(union, value):
    branches = [index for index, schema in enumerate(union.schemas) if avro.io.validate(schema, value)]
    assert len(branches) == 1, (union, value, branches)
    return branches[0]


def civil(days):
    days += 719468
    era = days // 146097
    day_of_era = days - era * 146097
    year_of_era = (day_of_era - day_of_era // 1460 + day_of_era // 36524 - day_of_era // 146096) // 365
    day_of_year = day_of_era - (365 * year_of_era + year_of_era // 4 - year_of_era // 100)
    shifted_month = (5 * day_of_year + 2) // 153
    month = shifted_month + 3 if shifted_month < 10 else shifted_month - 9
    return year_of_era + era * 400 + (month <= 2), month, day_of_year - (153 * shifted_month + 2) // 5 + 1


def date_text(days):
    year, month, day = civil(days)
    if year <= 0:
        return f"{1 - year:04d}-{month:02d}-{day:02d} (BC)"
    return f"{year:04d}-{month:02d}-{day:02d}"


def time_text(nanos):
    seconds, fraction = divmod(nanos, 10**9)
    text = f"{seconds // 3600:02d}:{seconds // 60 % 60:02d}:{seconds % 60:02d}"
    if fraction:
        text += f".{fraction:09d}".rstrip("0")
    return text


def python_time_text(value):
    return time_text(((value.hour * 60 + value.minute) * 60 + value.second) * 10**9 + value.microsecond * 1000)


def python_datetime_text(value):
    return f"{value.year:04d}-{value.month:02d}-{value.day:02d} {python_time_text(value)}"


def timestamp_text(value, units_per_second):
    days, nanos = divmod(value * (10**9 // units_per_second), 86400 * 10**9)
    return f"{date_text(days)} {time_text(nanos)}"


def interval_text(months, days, millis):
    parts = []
    for value, unit in ((months // 12, "year"), (months % 12, "mon"), (days, "day")):
        if value:
            parts.append(f"{value} {unit}" + ("" if value == 1 else "s"))
    if millis or not parts:
        parts.append(time_text(millis * 10**6))
    return " ".join(parts)


def decimal_text(unscaled, scale):
    digits = str(abs(unscaled)).rjust(scale + 1, "0")
    text = f"{digits[:-scale]}.{digits[-scale:]}" if scale else digits
    return f"-{text}" if unscaled < 0 else text


def make_decimal(unscaled, scale):
    return decimal.Decimal(f"{unscaled}E-{scale}")


def unscaled(value, scale):
    sign, digits, exponent = value.as_tuple()
    return (-1 if sign else 1) * int("".join(map(str, digits))) * 10 ** (exponent + scale)


def micros(value):
    delta = value - EPOCH
    return (delta.days * 86400 + delta.seconds) * 10**6 + delta.microseconds


for days in (-719162, -1, 0, 1, 19782, 2932896):
    assert date_text(days) == (datetime.date(1970, 1, 1) + datetime.timedelta(days)).isoformat()
assert date_text(-719163) == "0001-12-31 (BC)"


def long_bytes(value):
    zigzag = (value << 1) ^ (value >> 63)
    out = bytearray()
    while zigzag >= 0x80:
        out.append(zigzag & 0x7F | 0x80)
        zigzag >>= 7
    out.append(zigzag)
    return bytes(out)


def bytes_bytes(value):
    return long_bytes(len(value)) + value


def string_bytes(value):
    return bytes_bytes(value.encode())


def entry_bytes(key, value):
    return string_bytes(key) + value


def negative(*items):
    return True, items


def positive(*items):
    return False, items


def collection_bytes(*blocks):
    out = b""
    for is_negative, items in blocks:
        data = b"".join(items)
        if is_negative:
            out += long_bytes(-len(items)) + long_bytes(len(data)) + data
        else:
            out += long_bytes(len(items)) + data
    return out + long_bytes(0)


def header_bytes(json_schema, codec="null"):
    return (
        b"Obj\x01"
        + collection_bytes(
            positive(
                entry_bytes("avro.schema", bytes_bytes(json_schema.encode())),
                entry_bytes("avro.codec", bytes_bytes(codec.encode())),
            )
        )
        + SYNC
    )


def raw_block_bytes(count, data):
    return long_bytes(count) + long_bytes(len(data)) + data + SYNC


def block_bytes(*records):
    return raw_block_bytes(len(records), b"".join(records))


def write_bytes(name, content):
    with open(path(name), "wb") as out:
        out.write(content)


json_schema = """
{
  "type": "record",
  "name": "codec_row",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "word", "type": "string"},
    {"name": "payload", "type": "bytes"},
    {"name": "score", "type": ["null", "double"]},
    {"name": "tags", "type": {"type": "array", "items": "string"}}
  ]
}
"""

codec_records = [
    {
        "id": i,
        "word": f"row-{i:05d}" + "x" * (i % 23),
        "payload": bytes((i * 31 + k) % 256 for k in range(i % 17)),
        "score": None if i % 7 == 0 else i / 4,
        "tags": [f"t{(i + k) % 13}" for k in range(i % 4)],
    }
    for i in range(3000)
]

for codec in ("null", "deflate", "snappy", "zstandard"):
    write(f"codec_{codec}", json_schema, codec_records, codec=codec, sync_interval=512)


class ZstandardWithoutContentSizeCodec(avro.codecs.Codec):
    @staticmethod
    def compress(data):
        compressed = zstandard.ZstdCompressor(write_content_size=False).compress(data)
        return compressed, len(compressed)

    decompress = avro.codecs.KNOWN_CODECS["zstandard"].decompress


zstandard_codec = avro.codecs.KNOWN_CODECS["zstandard"]
avro.codecs.KNOWN_CODECS["zstandard"] = ZstandardWithoutContentSizeCodec
write("codec_zstandard_no_content_size", json_schema, codec_records, codec="zstandard", sync_interval=512)
avro.codecs.KNOWN_CODECS["zstandard"] = zstandard_codec

write("codec_bzip2", json_schema, codec_records[:100], codec="bzip2")

for name in ("null", "deflate", "snappy", "zstandard", "zstandard_no_content_size", "bzip2"):
    with open(path(f"codec_{name}"), "rb") as file:
        blocks = file.read().count(SYNC) - 1
    records = read(f"codec_{name}")
    assert records == codec_records[: len(records)]
    print(f"{path(f'codec_{name}')}: {len(records)} records in {blocks} blocks")

print(
    "codec aggregates:",
    len(codec_records),
    sum(r["id"] for r in codec_records),
    min(r["id"] for r in codec_records),
    max(r["id"] for r in codec_records),
    sum(len(r["word"]) for r in codec_records),
    sum(len(r["payload"]) for r in codec_records),
    sum(r["score"] is not None for r in codec_records),
    sum(r["score"] for r in codec_records if r["score"] is not None),
    sum(len(r["tags"]) for r in codec_records),
)
for record in read("codec_null"):
    if record["id"] < 4 or record["id"] == 2999:
        print(f"  {record}")


json_schema = """
{
  "type": "record",
  "name": "parallel_row",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "word", "type": "string"},
    {"name": "nums", "type": {"type": "array", "items": "int"}},
    {"name": "opt", "type": ["null", "long"]}
  ]
}
"""

write(
    "parallel",
    json_schema,
    (
        {
            "id": i,
            "word": f"w{i % 7919}",
            "nums": [i % 1000 + k for k in range(i % 5)],
            "opt": None if i % 3 == 0 else i * 3,
        }
        for i in range(300000)
    ),
    sync_interval=4096,
)

records = read("parallel")
for r in records:
    i = r["id"]
    assert r["word"] == f"w{i % 7919}" and r["nums"] == [i % 1000 + k for k in range(i % 5)]
    assert r["opt"] == (None if i % 3 == 0 else i * 3)
with open(path("parallel"), "rb") as file:
    print(f"{path('parallel')}: {len(records)} records in {file.read().count(SYNC) - 1} blocks")
print(
    "parallel id:",
    len(records),
    len({r["id"] for r in records}),
    sum(r["id"] for r in records),
    min(r["id"] for r in records),
    max(r["id"] for r in records),
)
print(
    "parallel word:",
    len({r["word"] for r in records}),
    min(r["word"] for r in records),
    max(r["word"] for r in records),
    sum(len(r["word"]) for r in records),
)
print(
    "parallel nums and opt:",
    sum(len(r["nums"]) for r in records),
    sum(sum(r["nums"]) for r in records),
    sum(r["opt"] is not None for r in records),
    sum(r["opt"] for r in records if r["opt"] is not None),
)
for r in records:
    if r["id"] in (0, 1, 7919, 149999, 299999):
        print(f"  {r}")


json_schema = """
{
  "type": "record",
  "name": "primitives",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "f_null", "type": "null"},
    {"name": "f_boolean", "type": "boolean"},
    {"name": "f_int", "type": "int"},
    {"name": "f_long", "type": "long"},
    {"name": "f_float", "type": "float"},
    {"name": "f_double", "type": "double"},
    {"name": "f_bytes", "type": "bytes"},
    {"name": "f_string", "type": "string"}
  ]
}
"""

float_max = struct.unpack("<f", bytes.fromhex("ffff7f7f"))[0]
float_min = struct.unpack("<f", bytes.fromhex("01000000"))[0]
values = [
    (False, 0, 0, 0.0, 0.0, b"", ""),
    (True, 1, 1, 1.5, 1.5, b"\x00", "a"),
    (False, -1, -1, -2.25, -2.25, b"\xff\xfe\xfd", "Größe"),
    (True, 2147483647, 9223372036854775807, float_max, 1.7976931348623157e308, b"\x00\x01\x7f\x80\xfe\xff", "€uro"),
    (
        False,
        -2147483648,
        -9223372036854775808,
        -float_max,
        -1.7976931348623157e308,
        b"quote'double\"back\\slash",
        "🦆 duck",
    ),
    (True, 63, 64, float_min, 5e-324, bytes(range(33, 127)), "a string longer than twelve bytes"),
    (False, -64, -65, float("inf"), float("-inf"), b"x" * 200, "s" * 150),
    (True, 8191, -8193, float("nan"), float("nan"), b"\x80" * 3, "ascii"),
    (False, -8192, 1 << 40, -0.0, -0.0, b"tail", "末尾"),
]
write(
    "primitives",
    json_schema,
    (
        dict(
            zip(
                ("id", "f_null", "f_boolean", "f_int", "f_long", "f_float", "f_double", "f_bytes", "f_string"),
                (i, None) + v,
            )
        )
        for i, v in enumerate(values)
    ),
    metadata={"user.note": b"metadata a reader ignores"},
)
show("primitives")


json_schema = json.dumps(
    {
        "type": "record",
        "name": "enums_and_fixed",
        "fields": [
            {"name": "id", "type": "int"},
            {
                "name": "suit",
                "type": {"type": "enum", "name": "suit_enum", "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]},
            },
            {"name": "maybe_suit", "type": ["null", "suit_enum"]},
            {"name": "big", "type": {"type": "enum", "name": "big_enum", "symbols": [f"S{i:03d}" for i in range(300)]}},
            {"name": "f1", "type": {"type": "fixed", "name": "fixed1", "size": 1}},
            {"name": "f3", "type": {"type": "fixed", "name": "fixed3", "size": 3}},
            {"name": "f16", "type": ["null", {"type": "fixed", "name": "fixed16", "size": 16}]},
        ],
    },
    indent=2,
)

write(
    "enums_and_fixed",
    json_schema,
    [
        {"id": 0, "suit": "SPADES", "maybe_suit": None, "big": "S000", "f1": b"\x00", "f3": b"abc", "f16": None},
        {
            "id": 1,
            "suit": "HEARTS",
            "maybe_suit": "SPADES",
            "big": "S063",
            "f1": b"\x7f",
            "f3": b"\x00\x00\x00",
            "f16": bytes(range(16)),
        },
        {
            "id": 2,
            "suit": "DIAMONDS",
            "maybe_suit": "CLUBS",
            "big": "S064",
            "f1": b"\x80",
            "f3": b"\xff\xfe\xfd",
            "f16": b"\xff" * 16,
        },
        {
            "id": 3,
            "suit": "CLUBS",
            "maybe_suit": "HEARTS",
            "big": "S255",
            "f1": b"\xff",
            "f3": b"x,y",
            "f16": b"0123456789abcdef",
        },
        {
            "id": 4,
            "suit": "SPADES",
            "maybe_suit": "DIAMONDS",
            "big": "S256",
            "f1": b"a",
            "f3": b"'\"\\",
            "f16": b"\x00" * 16,
        },
        {"id": 5, "suit": "CLUBS", "maybe_suit": None, "big": "S299", "f1": b"~", "f3": b"end", "f16": None},
    ],
)
show("enums_and_fixed")


json_schema = """
{
  "type": "record",
  "name": "nested",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "level1", "type": {"type": "record", "name": "level1_record", "fields": [
      {"name": "a", "type": "int"},
      {"name": "level2", "type": {"type": "record", "name": "level2_record", "fields": [
        {"name": "b", "type": "string"},
        {"name": "level3", "type": {"type": "record", "name": "level3_record", "fields": [
          {"name": "c", "type": "long"},
          {"name": "d", "type": ["null", "string"]}
        ]}}
      ]}}
    ]}},
    {"name": "matrix", "type": {"type": "array", "items": {"type": "array", "items": "int"}}},
    {"name": "words", "type": {"type": "array", "items": {"type": "array", "items": "string"}}},
    {"name": "recs", "type": {"type": "map", "values": {"type": "record", "name": "map_value", "fields": [
      {"name": "x", "type": "int"},
      {"name": "s", "type": "string"}
    ]}}},
    {"name": "lists", "type": {"type": "map", "values": {"type": "array", "items": "long"}}},
    {"name": "maps", "type": {"type": "array", "items": {"type": "map", "values": "int"}}},
    {"name": "structs", "type": {"type": "array", "items": "level3_record"}}
  ]
}
"""

write(
    "nested",
    json_schema,
    [
        {
            "id": 0,
            "level1": {"a": 1, "level2": {"b": "x", "level3": {"c": 10, "d": None}}},
            "matrix": [[1, 2], [], [3]],
            "words": [["a", "b"], []],
            "recs": {"k1": {"x": 1, "s": "one"}, "k2": {"x": 2, "s": "two"}},
            "lists": {"a": [1, 2], "b": []},
            "maps": [{"x": 1}, {}],
            "structs": [{"c": 1, "d": "p"}],
        },
        {
            "id": 1,
            "level1": {"a": -1, "level2": {"b": "", "level3": {"c": -10, "d": "deep"}}},
            "matrix": [],
            "words": [[]],
            "recs": {},
            "lists": {},
            "maps": [],
            "structs": [],
        },
        {
            "id": 2,
            "level1": {
                "a": 2147483647,
                "level2": {"b": "abcdefghijklmnopqrstuvwxyz", "level3": {"c": -9223372036854775808, "d": "z"}},
            },
            "matrix": [[]],
            "words": [["abcdefghijklmnopqrstuvwxyz"], ["c"]],
            "recs": {"k": {"x": -3, "s": "three"}},
            "lists": {"c": [9223372036854775807, -9223372036854775808]},
            "maps": [{"y": 2, "z": 3}],
            "structs": [{"c": 2, "d": None}, {"c": 3, "d": "q"}],
        },
    ],
)
show("nested")


json_schema = """
{
  "type": "record",
  "name": "unions",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "null_first", "type": ["null", "string"]},
    {"name": "null_last", "type": ["long", "null"]},
    {"name": "null_two", "type": ["null", "int", "string"]},
    {"name": "two", "type": ["string", "long"]},
    {"name": "null_middle", "type": ["int", "null", "string"]},
    {"name": "single", "type": ["int"]},
    {"name": "only_null", "type": ["null"]},
    {"name": "records", "type": ["null",
      {"type": "record", "name": "cat", "fields": [{"name": "meow", "type": "string"}]},
      {"type": "record", "name": "dog", "fields": [{"name": "bark", "type": "int"}]}
    ]},
    {"name": "nullable_record", "type": ["null", "cat"]},
    {"name": "in_array", "type": {"type": "array", "items": ["null", "int", "string"]}},
    {"name": "in_map", "type": {"type": "map", "values": ["null", "long", "string"]}},
    {"name": "nullable_in_map", "type": {"type": "map", "values": ["null", "int"]}},
    {"name": "collections", "type": ["null", {"type": "array", "items": "int"}, {"type": "map", "values": "int"}]}
  ]
}
"""

union_records = [
    {
        "id": 0,
        "null_first": "a",
        "null_last": 1,
        "null_two": 1,
        "two": "x",
        "null_middle": 7,
        "single": 1,
        "only_null": None,
        "records": {"meow": "purr"},
        "nullable_record": {"meow": "hiss"},
        "in_array": [1, "b", None],
        "in_map": {"k": 5, "s": "v", "n": None},
        "nullable_in_map": {"a": 1, "b": None},
        "collections": [1, 2],
    },
    {
        "id": 1,
        "null_first": None,
        "null_last": None,
        "null_two": "s",
        "two": 42,
        "null_middle": None,
        "single": -1,
        "only_null": None,
        "records": {"bark": 3},
        "nullable_record": None,
        "in_array": [],
        "in_map": {},
        "nullable_in_map": {},
        "collections": {"x": 1},
    },
    {
        "id": 2,
        "null_first": "",
        "null_last": 9223372036854775807,
        "null_two": None,
        "two": "",
        "null_middle": "z",
        "single": 2147483647,
        "only_null": None,
        "records": None,
        "nullable_record": {"meow": ""},
        "in_array": [None, None],
        "in_map": {"w": "word"},
        "nullable_in_map": {"c": None},
        "collections": None,
    },
    {
        "id": 3,
        "null_first": "last",
        "null_last": -9223372036854775808,
        "null_two": -2147483648,
        "two": -9223372036854775808,
        "null_middle": -2147483648,
        "single": -2147483648,
        "only_null": None,
        "records": {"meow": "a much longer string than twelve"},
        "nullable_record": {"meow": "z"},
        "in_array": ["x", 2147483647, "y"],
        "in_map": {"big": 9223372036854775807},
        "nullable_in_map": {"d": -5},
        "collections": [],
    },
]
write("unions", json_schema, union_records)
schema = avro.schema.parse(json_schema)
for record in show("unions"):
    branches = {
        name: branch(schema.fields_dict[name].type, record[name])
        for name in ("null_two", "two", "null_middle", "records", "collections")
    }
    branches["in_array"] = [branch(schema.fields_dict["in_array"].type.items, v) for v in record["in_array"]]
    branches["in_map"] = {k: branch(schema.fields_dict["in_map"].type.values, v) for k, v in record["in_map"].items()}
    print(f"  branches of {record['id']}: {branches}")


json_schema = """
{
  "type": "record",
  "name": "Outer",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "p1", "type": {"type": "record", "name": "Point", "fields": [
      {"name": "x", "type": "int"},
      {"name": "y", "type": "int"}
    ]}},
    {"name": "p2", "type": "Point"},
    {"name": "p3", "type": "com.example.Point"},
    {"name": "other", "type": {"type": "record", "name": "Other", "namespace": "org.other", "fields": [
      {"name": "p", "type": "com.example.Point"},
      {"name": "q", "type": {"type": "record", "name": "Point", "fields": [{"name": "word", "type": "string"}]}},
      {"name": "r", "type": "Point"},
      {"name": "suit", "type": {"type": "enum", "name": "Suit", "symbols": ["SPADES", "HEARTS"]}},
      {"name": "suit2", "type": "Suit"}
    ]}},
    {"name": "p4", "type": "Point"},
    {"name": "q2", "type": "org.other.Point"},
    {"name": "suit3", "type": "org.other.Suit"},
    {"name": "h1", "type": {"type": "fixed", "name": "Hash", "size": 4}},
    {"name": "h2", "type": "Hash"},
    {"name": "h3", "type": "com.example.Hash"},
    {"name": "dotted", "type": {"type": "record", "name": "x.y.Dotted", "namespace": "ignored.namespace", "fields": [
      {"name": "v", "type": "long"}
    ]}},
    {"name": "dotted2", "type": "x.y.Dotted"},
    {"name": "points", "type": {"type": "array", "items": "Point"}},
    {"name": "maybe", "type": ["null", "Point", "org.other.Point"]},
    {"name": "by_key", "type": {"type": "map", "values": "org.other.Suit"}}
  ]
}
"""

schema = avro.schema.parse(json_schema)
assert schema.fields_dict["p4"].type.fullname == "com.example.Point"
assert schema.fields_dict["q2"].type.fullname == "org.other.Point"
assert schema.fields_dict["other"].type.fields_dict["r"].type.fullname == "org.other.Point"
assert schema.fields_dict["dotted2"].type.fullname == "x.y.Dotted"
write(
    "named_types",
    json_schema,
    [
        {
            "id": 0,
            "p1": {"x": 1, "y": 2},
            "p2": {"x": 3, "y": 4},
            "p3": {"x": 5, "y": 6},
            "other": {
                "p": {"x": 7, "y": 8},
                "q": {"word": "q"},
                "r": {"word": "r"},
                "suit": "SPADES",
                "suit2": "HEARTS",
            },
            "p4": {"x": 9, "y": 10},
            "q2": {"word": "q2"},
            "suit3": "HEARTS",
            "h1": b"\xde\xad\xbe\xef",
            "h2": b"\x00\x01\x02\x03",
            "h3": b"abcd",
            "dotted": {"v": 11},
            "dotted2": {"v": 12},
            "points": [{"x": 13, "y": 14}, {"x": 15, "y": 16}],
            "maybe": {"x": 17, "y": 18},
            "by_key": {"a": "SPADES", "b": "HEARTS"},
        },
        {
            "id": 1,
            "p1": {"x": -1, "y": -2},
            "p2": {"x": -3, "y": -4},
            "p3": {"x": -5, "y": -6},
            "other": {
                "p": {"x": -7, "y": -8},
                "q": {"word": "qq"},
                "r": {"word": "rr"},
                "suit": "HEARTS",
                "suit2": "SPADES",
            },
            "p4": {"x": -9, "y": -10},
            "q2": {"word": "qq2"},
            "suit3": "SPADES",
            "h1": b"\xff\xff\xff\xff",
            "h2": b"wxyz",
            "h3": b"\x00\x00\x00\x00",
            "dotted": {"v": -11},
            "dotted2": {"v": -12},
            "points": [],
            "maybe": {"word": "other point"},
            "by_key": {},
        },
        {
            "id": 2,
            "p1": {"x": 0, "y": 0},
            "p2": {"x": 0, "y": 0},
            "p3": {"x": 0, "y": 0},
            "other": {"p": {"x": 0, "y": 0}, "q": {"word": ""}, "r": {"word": ""}, "suit": "SPADES", "suit2": "SPADES"},
            "p4": {"x": 0, "y": 0},
            "q2": {"word": ""},
            "suit3": "HEARTS",
            "h1": b"1234",
            "h2": b"5678",
            "h3": b"9abc",
            "dotted": {"v": 0},
            "dotted2": {"v": 0},
            "points": [{"x": 0, "y": 0}],
            "maybe": None,
            "by_key": {"c": "HEARTS"},
        },
    ],
)
for record in show("named_types"):
    print(
        f"  branch of maybe: {None if record['maybe'] is None else branch(schema.fields_dict['maybe'].type, record['maybe'])}"
    )


json_schema = """
{
  "type": "record",
  "name": "temporal",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "d", "type": ["null", {"type": "int", "logicalType": "date"}]},
    {"name": "t_ms", "type": ["null", {"type": "int", "logicalType": "time-millis"}]},
    {"name": "t_us", "type": [{"type": "long", "logicalType": "time-micros"}, "null"]},
    {"name": "ts_ms", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
    {"name": "ts_us", "type": [{"type": "long", "logicalType": "timestamp-micros"}, "null"]},
    {"name": "ts_ns", "type": ["null", {"type": "long", "logicalType": "timestamp-nanos"}]},
    {"name": "lts_ms", "type": ["null", {"type": "long", "logicalType": "local-timestamp-millis"}]},
    {"name": "lts_us", "type": [{"type": "long", "logicalType": "local-timestamp-micros"}, "null"]},
    {"name": "lts_ns", "type": ["null", {"type": "long", "logicalType": "local-timestamp-nanos"}]}
  ]
}
"""


def temporal(i, d, t_ms, t_us, ts_ms, ts_us, ts_ns):
    return {
        "id": i,
        "d": d,
        "t_ms": t_ms,
        "t_us": t_us,
        "ts_ms": ts_ms,
        "ts_us": ts_us,
        "ts_ns": ts_ns,
        "lts_ms": None if ts_ms is None else micros(ts_ms) // 1000,
        "lts_us": None if ts_us is None else micros(ts_us),
        "lts_ns": ts_ns,
    }


def utc_datetime(*args):
    return datetime.datetime(*args, tzinfo=utc)


write(
    "logical_temporal",
    json_schema,
    [
        temporal(0, datetime.date(1970, 1, 1), datetime.time(0), datetime.time(0), EPOCH, EPOCH, 0),
        temporal(
            1,
            datetime.date(1969, 12, 31),
            datetime.time(0, 0, 0, 1000),
            datetime.time(0, 0, 0, 1),
            utc_datetime(1969, 12, 31, 23, 59, 59, 999000),
            utc_datetime(1969, 12, 31, 23, 59, 59, 999999),
            -1,
        ),
        temporal(
            2,
            datetime.date(2024, 2, 29),
            datetime.time(12, 34, 56, 789000),
            datetime.time(12, 34, 56, 789012),
            utc_datetime(2024, 6, 15, 12, 30, 45, 123000),
            utc_datetime(2024, 6, 15, 12, 30, 45, 123456),
            1718454645123456789,
        ),
        temporal(
            3,
            datetime.date(1900, 3, 1),
            datetime.time(23, 59, 59, 999000),
            datetime.time(23, 59, 59, 999999),
            utc_datetime(1900, 1, 1, 0, 0, 0, 500000),
            utc_datetime(1900, 1, 1, 0, 0, 0, 1),
            -2208988799999999999,
        ),
        temporal(
            4,
            datetime.date(1, 1, 1),
            datetime.time(0, 0, 1),
            datetime.time(0, 1),
            utc_datetime(1, 1, 1),
            utc_datetime(1, 1, 1),
            -9223372036854775806,
        ),
        temporal(
            5,
            datetime.date(9999, 12, 31),
            datetime.time(1),
            datetime.time(23),
            utc_datetime(9999, 12, 31, 23, 59, 59, 999000),
            utc_datetime(9999, 12, 31, 23, 59, 59, 999999),
            9223372036854775806,
        ),
        temporal(6, None, None, None, None, None, None),
    ],
)
show("logical_temporal", raw_values=True)
for logical, raw in zip(read("logical_temporal"), read("logical_temporal", raw_values=True)):
    if raw["d"] is None:
        print(f"  {raw['id']}: all NULL")
        continue
    assert date_text(raw["d"]) == logical["d"].isoformat()
    assert time_text(raw["t_ms"] * 10**6) == python_time_text(logical["t_ms"])
    assert time_text(raw["t_us"] * 1000) == python_time_text(logical["t_us"])
    assert timestamp_text(raw["ts_ms"], 1000) == python_datetime_text(logical["ts_ms"])
    assert timestamp_text(raw["ts_us"], 10**6) == python_datetime_text(logical["ts_us"])
    assert timestamp_text(raw["lts_us"], 10**6) == python_datetime_text(logical["ts_us"])
    print(
        f"  {raw['id']}:",
        date_text(raw["d"]),
        time_text(raw["t_ms"] * 10**6),
        time_text(raw["t_us"] * 1000),
        timestamp_text(raw["ts_ms"], 1000),
        timestamp_text(raw["ts_us"], 10**6),
        timestamp_text(raw["ts_ns"], 10**9),
        timestamp_text(raw["lts_ms"], 1000),
        timestamp_text(raw["lts_us"], 10**6),
        timestamp_text(raw["lts_ns"], 10**9),
        sep=" | ",
    )


json_schema = """
{
  "type": "record",
  "name": "temporal_extreme",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "d", "type": {"type": "int", "logicalType": "date"}},
    {"name": "t_ms", "type": {"type": "int", "logicalType": "time-millis"}},
    {"name": "t_us", "type": {"type": "long", "logicalType": "time-micros"}},
    {"name": "ts_ms", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "ts_us", "type": {"type": "long", "logicalType": "timestamp-micros"}},
    {"name": "ts_ns", "type": {"type": "long", "logicalType": "timestamp-nanos"}},
    {"name": "lts_ms", "type": {"type": "long", "logicalType": "local-timestamp-millis"}},
    {"name": "lts_us", "type": {"type": "long", "logicalType": "local-timestamp-micros"}},
    {"name": "lts_ns", "type": {"type": "long", "logicalType": "local-timestamp-nanos"}}
  ]
}
"""


def temporal_extreme(i, d, t_ms, t_us, ts_ms, ts_us, ts_ns):
    return {
        "id": i,
        "d": d,
        "t_ms": t_ms,
        "t_us": t_us,
        "ts_ms": ts_ms,
        "ts_us": ts_us,
        "ts_ns": ts_ns,
        "lts_ms": ts_ms,
        "lts_us": ts_us,
        "lts_ns": ts_ns,
    }


write(
    "logical_temporal_extreme",
    json_schema,
    [
        temporal_extreme(
            0, 2147483646, 86400000, 86400000000, 9223372036854775, 9223372036854775806, 9223372036854775806
        ),
        temporal_extreme(1, -2147483646, 0, 0, -9223372022400000, -9223372022400000000, -9223372036854775806),
        temporal_extreme(2, 2932897, 1, 1, -62135596800001, -62135596800000001, -1),
    ],
    raw_values=True,
)
for raw in show("logical_temporal_extreme", raw_values=True):
    print(
        f"  {raw['id']}:",
        date_text(raw["d"]),
        time_text(raw["t_ms"] * 10**6),
        time_text(raw["t_us"] * 1000),
        timestamp_text(raw["ts_ms"], 1000),
        timestamp_text(raw["ts_us"], 10**6),
        timestamp_text(raw["ts_ns"], 10**9),
        timestamp_text(raw["lts_ms"], 1000),
        timestamp_text(raw["lts_us"], 10**6),
        timestamp_text(raw["lts_ns"], 10**9),
        sep=" | ",
    )


decimal_values = {
    4: [0, 1, -1, 127, 128, -128, -129, 255, 256, 9999, -9999],
    9: [0, 1, -1, 32767, 32768, -32768, -32769, 8388607, -8388608, 999999999, -999999999],
    18: [0, 1, -1, 2**31 - 1, 2**31, -(2**31), -(2**31) - 1, 2**55, -(2**55) - 1, 10**18 - 1, -(10**18) + 1],
    38: [
        0,
        1,
        -1,
        2**63 - 1,
        2**63,
        -(2**63),
        -(2**63) - 1,
        2**64,
        -(2**64),
        2**100,
        -(2**100),
        10**38 - 1,
        -(10**38) + 1,
    ],
}
decimal_rows = max(len(v) for v in decimal_values.values()) + 1

bytes_columns = [(4, 0), (4, 2), (9, 4), (9, 9), (18, 0), (18, 6), (38, 0), (38, 10), (38, 38)]
json_schema = json.dumps(
    {
        "type": "record",
        "name": "decimal_bytes",
        "fields": [{"name": "id", "type": "int"}]
        + [
            {
                "name": f"b{p}_{s}",
                "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": p, "scale": s}],
            }
            for p, s in bytes_columns
        ],
    },
    indent=2,
)


def decimal_record(i, columns):
    record = {"id": i}
    for name, precision, scale in columns:
        values = decimal_values[precision]
        record[name] = make_decimal(values[i], scale) if i < len(values) else None
    return record


write(
    "logical_decimal_bytes",
    json_schema,
    [decimal_record(i, [(f"b{p}_{s}", p, s) for p, s in bytes_columns]) for i in range(decimal_rows)],
)

fixed_columns = [
    (4, 1, 2),
    (4, 4, 2),
    (9, 0, 4),
    (9, 5, 4),
    (18, 3, 8),
    (18, 18, 8),
    (38, 0, 16),
    (38, 38, 16),
    (9, 2, 8),
    (38, 10, 20),
]
json_schema = json.dumps(
    {
        "type": "record",
        "name": "decimal_fixed",
        "fields": [{"name": "id", "type": "int"}]
        + [
            {
                "name": f"f{p}_{s}_{n}",
                "type": [
                    "null",
                    {
                        "type": "fixed",
                        "name": f"decimal_{p}_{s}_{n}",
                        "size": n,
                        "logicalType": "decimal",
                        "precision": p,
                        "scale": s,
                    },
                ],
            }
            for p, s, n in fixed_columns
        ],
    },
    indent=2,
)
write(
    "logical_decimal_fixed",
    json_schema,
    [decimal_record(i, [(f"f{p}_{s}_{n}", p, s) for p, s, n in fixed_columns]) for i in range(decimal_rows)],
)

for name, columns in (
    ("logical_decimal_bytes", [(f"b{p}_{s}", s) for p, s in bytes_columns]),
    ("logical_decimal_fixed", [(f"f{p}_{s}_{n}", s) for p, s, n in fixed_columns]),
):
    records = read(name)
    raw_records = read(name, raw_values=True)
    print(f"{path(name)}: {len(records)} records")
    for record, raw in zip(records, raw_records):
        texts = []
        for column, scale in columns:
            if record[column] is None:
                texts.append("NULL")
                continue
            value = unscaled(record[column], scale)
            assert value == int.from_bytes(raw[column], "big", signed=True)
            texts.append(decimal_text(value, scale))
        print(
            f"  {record['id']}: {' | '.join(texts)}   raw {[raw[c].hex() if raw[c] is not None else None for c, _ in columns]}"
        )


json_schema = """
{
  "type": "record",
  "name": "uuids",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "u_str", "type": ["null", {"type": "string", "logicalType": "uuid"}]},
    {"name": "u_fix", "type": ["null", {"type": "fixed", "name": "uuid16", "size": 16, "logicalType": "uuid"}]}
  ]
}
"""

uuid_strings = [
    "00000000-0000-0000-0000-000000000000",
    "00000000-0000-0000-0000-000000000001",
    "7fffffff-ffff-ffff-ffff-ffffffffffff",
    "80000000-0000-0000-0000-000000000000",
    "ffffffff-ffff-ffff-ffff-ffffffffffff",
    "550e8400-e29b-41d4-a716-446655440000",
    "123E4567-E89B-12D3-A456-426614174000",
    None,
]
write(
    "logical_uuid",
    json_schema,
    [{"id": i, "u_str": s, "u_fix": None if s is None else uuid.UUID(s).bytes} for i, s in enumerate(uuid_strings)],
)
for record in show("logical_uuid"):
    if record["u_fix"] is not None:
        assert str(uuid.UUID(bytes=record["u_fix"])) == record["u_str"].lower()
        print(f"  {record['id']}: {uuid.UUID(record['u_str'])}")


json_schema = """
{
  "type": "record",
  "name": "durations",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "dur", "type": ["null", {"type": "fixed", "name": "duration12", "size": 12, "logicalType": "duration"}]}
  ]
}
"""

durations = [
    (0, 0, 0),
    (1, 1, 1),
    (12, 0, 0),
    (14, 3, 3723004),
    (0, 30, 0),
    (0, 0, 86400000),
    (0, 0, 4294967295),
    (0x01020304, 0x05060708, 0x090A0B0C),
    (2147483647, 2147483647, 4294967295),
    None,
]
write(
    "logical_duration",
    json_schema,
    [{"id": i, "dur": None if d is None else struct.pack("<III", *d)} for i, d in enumerate(durations)],
)
for record in show("logical_duration"):
    if record["dur"] is not None:
        print(f"  {record['id']}: {interval_text(*struct.unpack('<III', record['dur']))}")


json_schema = """
{
  "type": "record",
  "name": "unknown_logical_types",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "my_long", "type": {"type": "long", "logicalType": "my-type"}},
    {"name": "my_string", "type": ["null", {"type": "string", "logicalType": "my-type"}]},
    {"name": "my_fixed", "type": {"type": "fixed", "name": "my_fixed4", "size": 4, "logicalType": "my-type"}}
  ]
}
"""

write(
    "logical_unknown",
    json_schema,
    [
        {"id": 0, "my_long": 0, "my_string": "a", "my_fixed": b"abcd"},
        {"id": 1, "my_long": -9223372036854775808, "my_string": None, "my_fixed": b"\x00\x01\x02\x03"},
        {"id": 2, "my_long": 9223372036854775807, "my_string": "z", "my_fixed": b"\xff\xff\xff\xff"},
    ],
)
show("logical_unknown")

json_schema = """
{
  "type": "record",
  "name": "invalid_logical_types",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "date_on_string", "type": {"type": "string", "logicalType": "date"}},
    {"name": "timestamp_on_int", "type": {"type": "int", "logicalType": "timestamp-millis"}},
    {"name": "decimal_scale_above_precision", "type": {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 6}},
    {"name": "uuid_on_fixed8", "type": {"type": "fixed", "name": "fixed8", "size": 8, "logicalType": "uuid"}},
    {"name": "duration_on_fixed8", "type": {"type": "fixed", "name": "other_fixed8", "size": 8, "logicalType": "duration"}}
  ]
}
"""

write(
    "logical_invalid",
    json_schema,
    [
        {
            "id": 0,
            "date_on_string": "2024-01-01",
            "timestamp_on_int": 86400,
            "decimal_scale_above_precision": b"\x01\x02",
            "uuid_on_fixed8": b"\x00\x11\x22\x33\x44\x55\x66\x77",
            "duration_on_fixed8": b"\x01\x00\x00\x00\x02\x00\x00\x00",
        },
        {
            "id": 1,
            "date_on_string": "not a date",
            "timestamp_on_int": -1,
            "decimal_scale_above_precision": b"",
            "uuid_on_fixed8": b"abcdefgh",
            "duration_on_fixed8": b"\xff" * 8,
        },
    ],
)
show("logical_invalid")


json_schema = """
{
  "type": "record",
  "name": "table",
  "fields": [
    {"name": "id", "type": "long", "field-id": 1},
    {"name": "word", "type": ["null", "string"], "field-id": 2},
    {"name": "point", "type": {"type": "record", "name": "r3", "fields": [
      {"name": "x", "type": "int", "field-id": 4},
      {"name": "y", "type": "int", "field-id": 5}
    ]}, "field-id": 3},
    {"name": "nums", "type": {"type": "array", "items": "long", "element-id": 7}, "field-id": 6},
    {"name": "props", "type": {"type": "map", "values": ["null", "string"], "key-id": 9, "value-id": 10}, "field-id": 8},
    {"name": "id_map", "type": {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "k12_v13", "fields": [
      {"name": "key", "type": "int", "field-id": 12},
      {"name": "value", "type": ["null", "long"], "field-id": 13}
    ]}}, "field-id": 11},
    {"name": "str_map", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "k15_v16", "fields": [
      {"name": "key", "type": "string", "field-id": 15},
      {"name": "value", "type": {"type": "record", "name": "r16", "fields": [{"name": "z", "type": "int", "field-id": 17}]}, "field-id": 16}
    ]}}], "field-id": 14}
  ]
}
"""

write(
    "field_ids",
    json_schema,
    [
        {
            "id": 1,
            "word": "a",
            "point": {"x": 1, "y": 2},
            "nums": [1, 2, 3],
            "props": {"p": "q", "n": None},
            "id_map": [{"key": 1, "value": 10}, {"key": 2, "value": None}],
            "str_map": [{"key": "k", "value": {"z": 5}}, {"key": "l", "value": {"z": -5}}],
        },
        {"id": 2, "word": None, "point": {"x": -1, "y": -2}, "nums": [], "props": {}, "id_map": [], "str_map": None},
        {
            "id": 3,
            "word": "c",
            "point": {"x": 0, "y": 0},
            "nums": [-9223372036854775808],
            "props": {"only": "one"},
            "id_map": [{"key": -2147483648, "value": 9223372036854775807}],
            "str_map": [],
        },
    ],
)
show("field_ids")


json_schema = """
{
  "type": "record",
  "name": "header_only",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "word", "type": "string"}
  ]
}
"""

write("header_only", json_schema, [])
with open(path("header_only"), "rb") as file:
    assert file.read().count(SYNC) == 1
show("header_only")

json_schema = """
{
  "type": "record",
  "name": "with_empty_record",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "empty", "type": {"type": "record", "name": "empty_record", "fields": []}},
    {"name": "empties", "type": {"type": "array", "items": "empty_record"}},
    {"name": "trailer", "type": "string"}
  ]
}
"""

write(
    "empty_record",
    json_schema,
    [
        {"id": 0, "empty": {}, "empties": [], "trailer": "a"},
        {"id": 1, "empty": {}, "empties": [{}, {}, {}], "trailer": "b"},
    ],
)
show("empty_record")


json_schema = """
{
  "type": "record",
  "name": "blocks",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "ints", "type": {"type": "array", "items": "int"}},
    {"name": "words", "type": {"type": "map", "values": "string"}},
    {"name": "grid", "type": {"type": "array", "items": {"type": "array", "items": "long"}}},
    {"name": "tail", "type": "string"}
  ]
}
"""

L = long_bytes
S = string_bytes
records = [
    L(0)
    + collection_bytes(negative(L(1), L(2)), positive(L(3)), negative(L(4), L(5), L(6)))
    + collection_bytes(
        negative(entry_bytes("a", S("x")), entry_bytes("b", S("yy"))), negative(entry_bytes("c", S("zzz")))
    )
    + collection_bytes(
        negative(
            collection_bytes(negative(L(10)), positive(L(20))), collection_bytes(), collection_bytes(negative(L(30)))
        )
    )
    + S("end0"),
    L(1) + collection_bytes() + collection_bytes() + collection_bytes() + S("end1"),
    L(2)
    + collection_bytes(negative(*[L(i) for i in range(1000)]))
    + collection_bytes(
        positive(entry_bytes("k", S("v"))), negative(entry_bytes("l", S("w"))), positive(entry_bytes("m", S("x")))
    )
    + collection_bytes(
        positive(collection_bytes(positive(L(-1)), negative(L(-2), L(-3)))),
        negative(
            collection_bytes(positive(L(9223372036854775807))), collection_bytes(negative(L(-9223372036854775808)))
        ),
    )
    + S("end2"),
    L(3)
    + collection_bytes(positive(L(7)), positive(L(8)), positive(L(9)))
    + collection_bytes(positive(entry_bytes("only", S("one"))))
    + collection_bytes(positive(collection_bytes(), collection_bytes(positive(L(5)))))
    + S("end3"),
]
metadata = [
    entry_bytes("avro.schema", bytes_bytes(json_schema.encode())),
    entry_bytes("user.note", bytes_bytes(b"no avro.codec entry")),
]
write_bytes(
    "negative_block_counts",
    b"Obj\x01"
    + collection_bytes(negative(metadata[0]), negative(metadata[1]))
    + SYNC
    + block_bytes(*records[:2])
    + block_bytes(*records[2:]),
)
for record in show("negative_block_counts"):
    print(f"  {record['id']}: {len(record['ints'])} ints summing to {sum(record['ints'])}")


json_schema = """
{
  "type": "record",
  "name": "corrupt",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "word", "type": "string"}
  ]
}
"""

blocks = [block_bytes(*[L(i) + S(f"word {i}") for i in range(b * 5, b * 5 + 5)]) for b in range(3)]
content = header_bytes(json_schema) + b"".join(blocks)
write_bytes("corrupt_truncated", content[: len(content) - len(SYNC) - 20])
show_failure("corrupt_truncated")

wrong_sync = bytes([SYNC[0] ^ 0xFF]) + SYNC[1:]
write_bytes(
    "corrupt_sync_marker", header_bytes(json_schema) + blocks[0][: -len(SYNC)] + wrong_sync + blocks[1] + blocks[2]
)
show_failure("corrupt_sync_marker")

write_bytes(
    "corrupt_utf8",
    header_bytes(json_schema)
    + block_bytes(L(0) + S("valid"), L(1) + bytes_bytes(b"in\xc3\x28valid"), L(2) + S("valid")),
)
show_failure("corrupt_utf8")

json_schema = """
{
  "type": "record",
  "name": "corrupt_enum",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "e", "type": {"type": "enum", "name": "abc", "symbols": ["A", "B", "C"]}}
  ]
}
"""

write_bytes("corrupt_enum", header_bytes(json_schema) + block_bytes(L(0) + L(2), L(1) + L(3), L(2) + L(0)))
show_failure("corrupt_enum")
write_bytes("corrupt_enum_negative", header_bytes(json_schema) + block_bytes(L(0) + L(2), L(1) + L(-1), L(2) + L(0)))
show_failure("corrupt_enum_negative")

json_schema = """
{
  "type": "record",
  "name": "corrupt_union",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "u", "type": ["null", "int", "string"]}
  ]
}
"""

write_bytes(
    "corrupt_union", header_bytes(json_schema) + block_bytes(L(0) + L(1) + L(5), L(1) + L(3) + L(5), L(2) + L(0))
)
show_failure("corrupt_union")

json_schema = """
{
  "type": "record",
  "name": "corrupt_nullable_union",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "n", "type": ["null", "long"]}
  ]
}
"""

write_bytes(
    "corrupt_union_nullable",
    header_bytes(json_schema) + block_bytes(L(0) + L(1) + L(5), L(1) + L(2) + L(5), L(2) + L(0)),
)
show_failure("corrupt_union_nullable")
write_bytes(
    "corrupt_union_negative",
    header_bytes(json_schema) + block_bytes(L(0) + L(1) + L(5), L(1) + L(-1) + L(5), L(2) + L(0)),
)
show_failure("corrupt_union_negative")

json_schema = """
{
  "type": "record",
  "name": "out_of_range",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "t_ms", "type": {"type": "int", "logicalType": "time-millis"}},
    {"name": "t_us", "type": {"type": "long", "logicalType": "time-micros"}},
    {"name": "ts_ms", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "dur", "type": {"type": "fixed", "name": "dur12", "size": 12, "logicalType": "duration"}}
  ]
}
"""


def out_of_range_row(i, t_ms, t_us, ts_ms, months):
    return L(i) + L(t_ms) + L(t_us) + L(ts_ms) + months.to_bytes(4, "little") + bytes(8)


day_ms = 86400000
good = out_of_range_row(0, day_ms, day_ms * 1000, 0, 0)
for name, row in (
    ("out_of_range_time_negative", out_of_range_row(1, -1, 0, 0, 0)),
    ("out_of_range_time_millis", out_of_range_row(1, day_ms + 1, 0, 0, 0)),
    ("out_of_range_time_micros", out_of_range_row(1, 0, day_ms * 1000 + 1, 0, 0)),
    ("out_of_range_timestamp_millis", out_of_range_row(1, 0, 0, 2**62, 0)),
    ("out_of_range_duration", out_of_range_row(1, 0, 0, 0, 2**31)),
):
    write_bytes(name, header_bytes(json_schema) + block_bytes(good, row))
    print(f"{path(name)}: {name}")

json_schema = """
{
  "type": "record",
  "name": "split_row",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "payload", "type": "bytes"}
  ]
}
"""


def split_payload(i):
    return bytes((i * 7 + k) % 256 for k in range(i % 40))


write_bytes(
    "big_block",
    header_bytes(json_schema) + block_bytes(*(L(i) + bytes_bytes(split_payload(i)) for i in range(200000))),
)
print(f"{path('big_block')}: {len(read('big_block'))} records in one block")


def false_sync_payload(i):
    return b"\x05" + SYNC + bytes(range(i % 50))


false_sync_records = [L(i) + bytes_bytes(false_sync_payload(i)) for i in range(60000)]
write_bytes(
    "false_sync",
    header_bytes(json_schema)
    + b"".join(block_bytes(*false_sync_records[i : i + 97]) for i in range(0, len(false_sync_records), 97)),
)
records = read("false_sync")
print(f"{path('false_sync')}: {len(records)} records, sum(len(payload)) = {sum(len(r['payload']) for r in records)}")


json_schema = """
{
  "type": "record",
  "name": "codec_block",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "word", "type": "string"}
  ]
}
"""

codec_data = b"".join(L(i) + S(f"word {i}") for i in range(5))


def raw_deflate(data, flush=zlib.Z_FINISH):
    compressor = zlib.compressobj(9, zlib.DEFLATED, -15)
    return compressor.compress(data) + compressor.flush(flush)


def write_codec_blocks(name, codec, *blocks):
    header = header_bytes(json_schema, codec)
    write_bytes(name, header + b"".join(blocks))
    print(f"{path(name)}: last block at offset {len(header) + sum(len(block) for block in blocks[:-1])}")


write_codec_blocks(
    "corrupt_deflate", "deflate", raw_block_bytes(5, raw_deflate(codec_data)), raw_block_bytes(5, b"\xff" * 8)
)
show_failure("corrupt_deflate")

ends_early_data = L(0) + S("x" * 65532)
ends_early = raw_deflate(ends_early_data, zlib.Z_SYNC_FLUSH)
assert len(ends_early_data) == 65536 and ends_early.endswith(b"\x00\x00\xff\xff") and len(ends_early) * 4 < 65536
ends_early = ends_early[:-4]
inflater = zlib.decompressobj(-15)
assert inflater.decompress(ends_early, 65536) == ends_early_data
assert not inflater.unconsumed_tail and not inflater.eof and inflater.decompress(b"", 65536) == b""
write_codec_blocks("corrupt_deflate_end", "deflate", raw_block_bytes(1, ends_early))
show_failure("corrupt_deflate_end")

for name, data in (
    ("corrupt_snappy_short", b"\x00\x00\x00"),
    ("corrupt_snappy_length", b"\xff" * 6 + bytes(4)),
    ("corrupt_snappy_data", b"\x0a\x24abc" + bytes(4)),
    ("corrupt_snappy_checksum", snappy.compress(codec_data) + (zlib.crc32(codec_data) ^ 1).to_bytes(4, "big")),
    ("corrupt_snappy_declared_length", b"\x80\x80\x80\x80\x08" + b"\x00a" + bytes(4)),
):
    write_codec_blocks(name, "snappy", raw_block_bytes(5, data))
    show_failure(name)

runs_data = L(0) + S("a" * 60000)
runs = snappy.compress(runs_data)
assert len(runs_data) > 20 * len(runs)
write_codec_blocks("codec_snappy_runs", "snappy", raw_block_bytes(1, runs + zlib.crc32(runs_data).to_bytes(4, "big")))
assert read("codec_snappy_runs") == [{"id": 0, "word": "a" * 60000}]
print(f"{path('codec_snappy_runs')}: {len(runs_data)} bytes from {len(runs)} snappy bytes")

write_codec_blocks("corrupt_zstandard", "zstandard", raw_block_bytes(5, bytes(8)))
show_failure("corrupt_zstandard")
write_codec_blocks(
    "corrupt_zstandard_end", "zstandard", raw_block_bytes(5, zstandard.ZstdCompressor().compress(codec_data)[:-3])
)
show_failure("corrupt_zstandard_end")

frames = [zstandard.ZstdCompressor().compress(part) for part in (codec_data[:20], codec_data[20:])]
assert b"".join(zstandard.ZstdDecompressor().decompress(frame) for frame in frames) == codec_data
write_codec_blocks("codec_zstandard_frames", "zstandard", raw_block_bytes(5, b"".join(frames)))

json_schema = """
{
  "type": "record",
  "name": "ones",
  "fields": [{"name": "n", "type": "long"}]
}
"""

frame = zstandard.ZstdCompressor(write_content_size=False).compress(L(1) * 100000)
assert zstandard.get_frame_parameters(frame).content_size == zstandard.CONTENTSIZE_UNKNOWN
assert len(frame) * 4 < 65536
write_bytes("codec_zstandard_large", header_bytes(json_schema, "zstandard") + raw_block_bytes(100000, frame))
records = read("codec_zstandard_large")
assert records == [{"n": 1}] * 100000
print(f"{path('codec_zstandard_large')}: {len(records)} records in a block of {len(frame)} bytes")

frame = (
    b"\x28\xb5\x2f\xfd\xc0\x38"
    + (2**64 - 3).to_bytes(8, "little")
    + ((100000 << 3) | (1 << 1) | 1).to_bytes(3, "little")
    + L(1)
)
assert zstandard.get_frame_parameters(frame).content_size == 2**64 - 3
write_bytes("corrupt_zstandard_content_size", header_bytes(json_schema, "zstandard") + raw_block_bytes(100000, frame))
show_failure("corrupt_zstandard_content_size")


json_schema = """
{
  "type": "record",
  "name": "block_frame",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "word", "type": "string"}
  ]
}
"""

frame_header = header_bytes(json_schema)
frame_first = block_bytes(*[L(i) + S(f"word {i}") for i in range(3)])
frame_last = L(3) + S("word 3")
for name, tail in (
    ("corrupt_block_count_malformed", b"\x80" * 10 + bytes(32)),
    ("corrupt_block_count_cut", b"\x80"),
    ("corrupt_block_size_malformed", L(1) + b"\x80" * 10 + bytes(32)),
    ("corrupt_block_size_cut", L(1) + b"\x80"),
    ("corrupt_block_count_negative", L(-1) + L(len(frame_last)) + frame_last + SYNC),
    ("corrupt_block_sync_cut", L(1) + L(len(frame_last)) + frame_last + SYNC[:10]),
):
    write_bytes(name, frame_header + frame_first + tail)
    print(f"{path(name)}: second block at offset {len(frame_header) + len(frame_first)}")

write_bytes(
    "empty_block",
    frame_header + block_bytes(L(0) + S("a"), L(1) + S("b")) + raw_block_bytes(0, b"") + block_bytes(L(2) + S("c")),
)
show("empty_block")

write_bytes("block_trailing_bytes", frame_header + raw_block_bytes(2, L(0) + S("a") + L(1) + S("b") + bytes(3)))
print(f"{path('block_trailing_bytes')}: block at offset {len(frame_header)}")

json_schema = """
{
  "type": "record",
  "name": "nothing",
  "fields": [{"name": "n", "type": "null"}]
}
"""

many_records_header = header_bytes(json_schema)
write_bytes("block_many_records", many_records_header + raw_block_bytes(2**24 + 1, b""))
print(f"{path('block_many_records')}: {2**24 + 1} records in a block at offset {len(many_records_header)}")


def record_schema(name, *fields):
    return json.dumps(
        {"type": "record", "name": name, "fields": [{"name": n, "type": t} for n, t in (("id", "long"),) + fields]}
    )


for name, json_schema, records in (
    ("malformed_long", record_schema("malformed_long", ("n", "long")), [L(0) + L(5), L(1) + b"\x80" * 10]),
    ("long_past_end", record_schema("long_past_end", ("n", "long")), [L(0) + L(5), L(1) + b"\x80"]),
    ("negative_length", record_schema("negative_length", ("s", "string")), [L(0) + S("a"), L(1) + L(-1)]),
    ("length_past_end", record_schema("length_past_end", ("s", "string")), [L(0) + S("a"), L(1) + L(100) + b"ab"]),
    ("boolean_past_end", record_schema("boolean_past_end", ("flag", "boolean")), [L(0) + b"\x01", L(1)]),
    (
        "array_count_min",
        record_schema("array_count_min", ("a", {"type": "array", "items": "long"})),
        [L(0) + collection_bytes(positive(L(7))), L(1) + L(-(2**63))],
    ),
    (
        "array_count_past_end",
        record_schema("array_count_past_end", ("a", {"type": "array", "items": "long"})),
        [L(0) + collection_bytes(positive(L(7))), L(1) + L(100) + L(7)],
    ),
    (
        "array_count_empty_items",
        record_schema("array_count_empty_items", ("a", {"type": "array", "items": "null"})),
        [L(0) + collection_bytes(positive(b"", b"")), L(1) + L(2**32 + 1) + L(0)],
    ),
):
    header = header_bytes(json_schema)
    write_bytes(name, header + block_bytes(*records))
    print(f"{path(name)}: block at offset {len(header)}")


json_schema = json.dumps(
    {
        "type": "record",
        "name": "invalid_values",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "i", "type": "int"},
            {"name": "u", "type": {"type": "string", "logicalType": "uuid"}},
            {"name": "dur", "type": {"type": "fixed", "name": "dur12", "size": 12, "logicalType": "duration"}},
            {
                "name": "nested",
                "type": {
                    "type": "record",
                    "name": "nested_times",
                    "fields": [
                        {"name": "t", "type": {"type": "int", "logicalType": "time-millis"}},
                        {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                    ],
                },
            },
            {
                "name": "floats",
                "type": {
                    "type": "record",
                    "name": "float_pair",
                    "fields": [{"name": "f", "type": "float"}, {"name": "d", "type": "double"}],
                },
            },
        ],
    },
    indent=2,
)


def invalid_values_row(i, int_value=7, uuid_text="123e4567-e89b-12d3-a456-426614174000", days=2, ts=1000):
    return (
        L(i)
        + L(int_value)
        + S(uuid_text)
        + struct.pack("<III", 1, days, 3)
        + L(1000)
        + L(ts)
        + struct.pack("<fd", 1.5, 2.5)
    )


invalid_values_header = header_bytes(json_schema)
write_bytes(
    "invalid_values",
    invalid_values_header
    + block_bytes(
        invalid_values_row(0),
        invalid_values_row(1, int_value=2**31),
        invalid_values_row(2, int_value=-(2**31) - 1),
        invalid_values_row(3, uuid_text="not-a-uuid"),
        invalid_values_row(4, days=2**31),
        invalid_values_row(5, ts=2**62),
    ),
)
print(f"{path('invalid_values')}: block at offset {len(invalid_values_header)}")


json_schema = json.dumps(
    {
        "type": "record",
        "name": "decimal_width",
        "fields": [{"name": "id", "type": "long"}]
        + [
            {"name": f"b{p}", "type": {"type": "bytes", "logicalType": "decimal", "precision": p, "scale": 0}}
            for p in (4, 18, 38)
        ]
        + [
            {
                "name": f"f{p}",
                "type": {
                    "type": "fixed",
                    "name": f"fixed{p}_12",
                    "size": 12,
                    "logicalType": "decimal",
                    "precision": p,
                    "scale": 0,
                },
            }
            for p in (4, 9, 18)
        ],
    },
    indent=2,
)


def twos(value, size):
    return value.to_bytes(size, "big", signed=True)


def decimal_width_row(
    i,
    b4=b"",
    b18=twos(-123456789012, 12),
    b38=twos(10**37, 17),
    f4=-1234,
    f9=123456789,
    f18=-123456789012345678,
):
    return L(i) + bytes_bytes(b4) + bytes_bytes(b18) + bytes_bytes(b38) + twos(f4, 12) + twos(f9, 12) + twos(f18, 12)


decimal_width_header = header_bytes(json_schema)
write_bytes(
    "decimal_width",
    decimal_width_header
    + block_bytes(
        decimal_width_row(0),
        decimal_width_row(1, b4=twos(40000, 3)),
        decimal_width_row(2, b4=twos(-40000, 3)),
        decimal_width_row(3, b38=b"\x01" + bytes(16)),
        decimal_width_row(4, b38=b"\x00\x80" + bytes(15)),
        decimal_width_row(5, f18=2**70),
        decimal_width_row(6, f4=40000),
    ),
)
print(f"{path('decimal_width')}: block at offset {len(decimal_width_header)}")


json_schema = json.dumps(
    {
        "type": "record",
        "name": "empty_items",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "blanks", "type": {"type": "array", "items": {"type": "fixed", "name": "fixed0", "size": 0}}},
            {
                "name": "units",
                "type": {
                    "type": "array",
                    "items": {"type": "record", "name": "unit", "fields": [{"name": "n", "type": "null"}]},
                },
            },
            {"name": "maybe", "type": [{"type": "record", "name": "nothing", "fields": []}, "long"]},
        ],
    },
    indent=2,
)

write_bytes(
    "empty_items",
    header_bytes(json_schema)
    + block_bytes(
        L(0) + collection_bytes(positive(b"", b"", b"")) + collection_bytes(positive(b"", b"")) + L(0),
        L(1) + collection_bytes() + collection_bytes(positive(b"")) + L(1) + L(42),
    ),
)
show("empty_items")


json_schema = json.dumps(
    [
        "null",
        {
            "type": "record",
            "name": "row",
            "fields": [
                {"name": "a", "type": ["null", "long"]},
                {"name": "b", "type": "string"},
                {"name": "c", "type": "double"},
            ],
        },
    ],
    indent=2,
)

write(
    "root_union",
    json_schema,
    [None, {"a": 1, "b": "x", "c": 1.5}, {"a": None, "b": "y", "c": 2.5}, None, {"a": 3, "b": "z", "c": 3.5}],
)
show("root_union")


json_schema = """
{
  "type": "record",
  "name": "tail",
  "fields": [{"name": "id", "type": "long"}]
}
"""

tail_block = block_bytes(L(5))
assert len(tail_block) == 19
write_bytes("lookup_tail", header_bytes(json_schema) + block_bytes(*[L(i) for i in range(5)]) + tail_block)
show("lookup_tail")


json_schema = """
{
  "type": "record",
  "name": "binary_metadata",
  "fields": [{"name": "n", "type": "long"}]
}
"""

write("binary_metadata", json_schema, [{"n": 1}], metadata={"user.binary": b"\xff\xfe\x00\x01"})
show("binary_metadata")

write_bytes(
    "binary_metadata_key",
    b"Obj\x01"
    + collection_bytes(
        positive(
            entry_bytes("avro.schema", bytes_bytes(json_schema.encode())),
            bytes_bytes(b"user.\xff") + bytes_bytes(b"x"),
        )
    )
    + SYNC
    + block_bytes(long_bytes(1)),
)


for name, key, nullable_entry, records in (
    ("map_nullable_key", ["null", "int"], False, [{"m": [{"key": 1, "value": 10}, {"key": 2, "value": 20}]}]),
    ("map_null_key", ["null", "int"], False, [{"m": [{"key": 1, "value": 10}]}, {"m": [{"key": None, "value": 20}]}]),
    ("map_null_later_key", ["null", "int"], False, [{"m": [{"key": 1, "value": 10}, {"key": None, "value": 20}]}]),
    ("map_null_entry", "int", True, [{"m": [{"key": 1, "value": 10}]}, {"m": [None]}]),
):
    entry = {
        "type": "record",
        "name": f"{name}_entry",
        "fields": [{"name": "key", "type": key}, {"name": "value", "type": "long"}],
    }
    items = ["null", entry] if nullable_entry else entry
    write(
        name,
        json.dumps(
            {
                "type": "record",
                "name": name,
                "fields": [{"name": "m", "type": {"type": "array", "logicalType": "map", "items": items}}],
            }
        ),
        records,
    )
    show(name)
