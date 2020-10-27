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
