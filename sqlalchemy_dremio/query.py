from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from sqlalchemy import types

import pyarrow as pa
from pyarrow import flight

_type_map = {
    'boolean': types.BOOLEAN,
    'BOOLEAN': types.BOOLEAN,
    'varbinary': types.LargeBinary,
    'VARBINARY': types.LargeBinary,
    'date': types.DATE,
    'DATE': types.DATE,
    'float64': types.FLOAT,
    'decimal': types.DECIMAL,
    'DECIMAL': types.DECIMAL,
    'double': types.FLOAT,
    'DOUBLE': types.FLOAT,
    'interval': types.Interval,
    'INTERVAL': types.Interval,
    'int32': types.INTEGER,
    'int64': types.BIGINT,
    'time': types.TIME,
    'TIME': types.TIME,
    'timestamp': types.TIMESTAMP,
    'TIMESTAMP': types.TIMESTAMP,
    'varchar': types.VARCHAR,
    'VARCHAR': types.VARCHAR,
    'smallint': types.SMALLINT,
    'CHARACTER VARYING': types.VARCHAR,
    'object': types.VARCHAR
}


def run_query(query, flightclient=None):
    info = flightclient.get_flight_info(flight.FlightDescriptor.for_command(query))
    reader = flightclient.do_get(info.endpoints[0].ticket)

    batches = []
    while True:
        try:
            batch, metadata = reader.read_chunk()
            batches.append(batch)
        except StopIteration:
            break
    # TODO Naren: Coverting to dataframe seems uncessary

    data = pa.Table.from_batches(batches)
    df = data.to_pandas()

    return df


def execute(query, flightclient=None):
    df = run_query(query, flightclient)

    result = []

    for x, y in df.dtypes.to_dict().items():
        o = (x, _type_map[str(y.name)], None, None, True)
        result.append(o)

    return df.values.tolist(), result
