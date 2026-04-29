import pyarrow as pa
import pyarrow.parquet
import os

PYARROW_PATH = "fixtures/pyarrow3"


def case_basic_nullable(size=1):
    int64 = [0, 1, None, 3, None, 5, 6, 7, None, 9]
    float64 = [0.0, 1.0, None, 3.0, None, 5.0, 6.0, 7.0, None, 9.0]
    string = ["Hello", None, "aa", "", None, "abc", None, None, "def", "aaa"]
    boolean = [True, None, False, False, None, True, None, None, True, True]
    fixed_binary = [b"aa", None, b"cc", b"dd", None, b"ff", None, None, b"ii", b"jj"]

    fields = [
        pa.field("int64", pa.int64()),
        pa.field("float64", pa.float64()),
        pa.field("string", pa.utf8()),
        pa.field("bool", pa.bool_()),
        pa.field("date", pa.timestamp("ms")),
        pa.field("uint32", pa.uint32()),
        pa.field("fixed_binary", pa.binary(length=2)),
    ]
    schema = pa.schema(fields)

    return (
        {
            "int64": int64 * size,
            "float64": float64 * size,
            "string": string * size,
            "bool": boolean * size,
            "date": int64 * size,
            "uint32": int64 * size,
            "fixed_binary": fixed_binary * size,
        },
        schema,
        f"basic_nullable_{size*10}.parquet",
    )


def case_basic_required(size=1):
    int64 = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    float64 = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    string = ["Hello", "bbb", "aa", "", "bbb", "abc", "bbb", "bbb", "def", "aaa"]
    boolean = [True, True, False, False, False, True, True, True, True, True]
    fixed_binary = [b"aa", b"bb", b"cc", b"dd", b"ee", b"ff", b"gg", b"hh", b"ii", b"jj"]

    fields = [
        pa.field("int64", pa.int64(), nullable=False),
        pa.field("float64", pa.float64(), nullable=False),
        pa.field("string", pa.utf8(), nullable=False),
        pa.field("bool", pa.bool_(), nullable=False),
        pa.field("date", pa.timestamp("ms"), nullable=False),
        pa.field("uint32", pa.uint32(), nullable=False),
        pa.field("fixed_binary", pa.binary(length=2), nullable=False),
    ]
    schema = pa.schema(fields)

    return (
        {
            "int64": int64 * size,
            "float64": float64 * size,
            "string": string * size,
            "bool": boolean * size,
            "date": int64 * size,
            "uint32": int64 * size,
            "fixed_binary": fixed_binary * size,
        },
        schema,
        f"basic_required_{size*10}.parquet",
    )


def case_nested(size):
    items = [[0, 1], None, [2, None, 3], [4, 5, 6], [], [7, 8, 9], None, [10]]
    fields = [
        pa.field("list_int64", pa.list_(pa.int64())),
    ]
    schema = pa.schema(fields)
    return (
        {
            "list_int64": items * size,
        },
        schema,
        f"nested_nullable_{size*10}.parquet",
    )


def case_struct(size):
    string = ["Hello", None, "aa", "", None, "abc", None, None, "def", "aaa"]
    boolean = [True, None, False, False, None, True, None, None, True, True]
    validity = [True, False, False, False, False, False, False, False, False, False]
    struct_fields = [
        ("f1", pa.utf8()),
        ("f2", pa.bool_()),
    ]
    fields = [
        pa.field(
            "struct_nullable",
            pa.struct(struct_fields),
        ),
        pa.field(
            "struct_required",
            pa.struct(struct_fields),
        ),
    ]
    schema = pa.schema(fields)
    return (
        {
            "struct_nullable": pa.StructArray.from_arrays(
                [pa.array(string * size), pa.array(boolean * size)],
                fields=struct_fields,
                mask=pa.array(validity * size),
            ),
            "struct_required": pa.StructArray.from_arrays(
                [pa.array(string * size), pa.array(boolean * size)],
                fields=struct_fields,
            ),
        },
        schema,
        f"struct_nullable_{size*10}.parquet",
    )


def write_pyarrow(
    case, size=1, page_version=1, use_dictionary=False, compression=None
):
    data, schema, path = case(size)

    compression_path = f"/{compression}" if compression else ""

    if use_dictionary:
        base_path = f"{PYARROW_PATH}/v{page_version}/dict{compression_path}"
    else:
        base_path = f"{PYARROW_PATH}/v{page_version}/non_dict{compression_path}"

    t = pa.table(data, schema=schema)
    os.makedirs(base_path, exist_ok=True)
    pa.parquet.write_table(
        t,
        f"{base_path}/{path}",
        version=f"{page_version}.0",
        data_page_version=f"{page_version}.0",
        write_statistics=True,
        compression=compression,
        use_dictionary=use_dictionary,
    )


for case in [case_basic_nullable, case_basic_required, case_nested, case_struct]:
    for version in [1, 2]:
        for use_dict in [False, True]:
            for compression in [None, "brotli", "lz4", "gzip", "snappy", "zstd"]:
                write_pyarrow(case, 1, version, use_dict, compression)
