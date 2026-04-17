import pyarrow.json as pj
import pyarrow as pa
import pyarrow.compute as pc
import io


def get_raw_schema(schema: pa.Schema) -> pa.Schema:
    return pa.schema(
        [
            f.with_type(pa.string()) if pa.types.is_floating(f.type) else f
            for f in schema
        ]
    )


def fix_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    for f in schema:
        if pa.types.is_floating(f.type):
            column = table.column(f.name)
            table = table.set_column(
                table.schema.get_field_index(f.name),
                f.name,
                pc.replace_with_mask(
                    column,
                    pc.equal(pc.utf8_length(column), 0).combine_chunks(),
                    pa.scalar(None, pa.string()),
                ).cast(pa.float64()),
            )
    return table


def json_to_table(array: pa.Array, schema: pa.Schema) -> pa.Table:
    if len(array) == 0:
        return schema.empty_table()
    else:
        raw_schema = get_raw_schema(schema)
        with io.BytesIO() as buffer:
            for payload in array:
                buffer.write(payload)
                buffer.write(b"\n")
            buffer.seek(0)
            table = pj.read_json(
                buffer,
                parse_options=pj.ParseOptions(explicit_schema=raw_schema),
            )
            return fix_schema(table, schema)
