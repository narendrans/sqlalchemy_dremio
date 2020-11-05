from .flight import _type_map

import pyarrow as pa
from pyarrow import flight

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

    return pa.Table.from_batches(batches)


def execute(query, flightclient=None):
    data = run_query(query, flightclient)

    result = []

    for x in data.schema:
        o = (x.name, _type_map[str(x.type)], None, None, True)
        result.append(o)

    return list(data.to_pydict().values()), result
