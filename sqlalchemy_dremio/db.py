import logging
from typing import Optional, Any, Never, Generator, Union

from pyarrow import flight
from pyarrow._flight import FlightClient
from sqlalchemy import Executable, types

from sqlalchemy_dremio.exceptions import Error, NotSupportedError
from sqlalchemy_dremio.flight_middleware import CookieMiddlewareFactory
from sqlalchemy_dremio.query import execute

logger = logging.getLogger(__name__)

paramstyle = 'qmark'


class Binary(types.LargeBinary):
    __visit_name__ = "VARBINARY"


def connect(c):
    return Connection(c)


def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    def g(self, *args, **kwargs):
        if self.closed:
            raise Error(
                f'{self.__class__.__name__} already closed')
        return f(self, *args, **kwargs)

    return g


def check_result(f):
    """Decorator that checks if the cursor has results from `execute`."""

    def d(self, *args, **kwargs):
        if self._results is None:
            raise Error('Called before `execute`')
        return f(self, *args, **kwargs)

    return d


class Connection:

    def __init__(self, connection_string: str):

        # Build a map from the connection string supplied using the SQLAlchemy URI
        # and supplied properties. The format is generated from DremioDialect_flight.create_connect_args()
        # and is a semi-colon delimited string of key=value pairs. Note that the value itself can
        # contain equal signs.
        properties = {}
        splits = connection_string.split(";")

        for kvpair in splits:
            kv = kvpair.split("=", 1)
            properties[kv[0]] = kv[1]

        connection_args = {}

        # Connect to the server endpoint with an encrypted TLS connection by default.
        protocol = 'tls'
        if 'UseEncryption' in properties and properties['UseEncryption'].lower() == 'false':
            protocol = 'tcp'
        else:
            # Specify the trusted certificates
            connection_args['disable_server_verification'] = False
            if 'TrustedCerts' in properties:
                with open(properties['TrustedCerts'], "rb") as root_certs:
                    connection_args["tls_root_certs"] = root_certs.read()
            # Or disable server verification entirely
            elif 'DisableCertificateVerification' in properties and properties[
                'DisableCertificateVerification'].lower() == 'true':
                connection_args['disable_server_verification'] = True

        # Enabling cookie middleware for stateful connectivity.
        client_cookie_middleware = CookieMiddlewareFactory()

        client = flight.FlightClient(f'grpc+{protocol}://{properties["HOST"]}:{properties["PORT"]}',
                                     middleware=[client_cookie_middleware], **connection_args)

        # Authenticate either using basic username/password or using the Token parameter.
        headers = []
        if 'UID' in properties:
            bearer_token = client.authenticate_basic_token(properties['UID'], properties['PWD'])
            headers.append(bearer_token)
        else:
            headers.append((b'authorization', f"Bearer {properties["Token"]}".encode('utf-8')))

        # Propagate Dremio-specific headers.
        def add_header(properties: dict, headers: list[tuple[bytes, bytes]], header_name: str) -> None:
            if header_name in properties:
                headers.append((header_name.lower().encode('utf-8'), properties[header_name].encode('utf-8')))

        add_header(properties, headers, 'schema')
        add_header(properties, headers, 'routing_queue')
        add_header(properties, headers, 'routing_tag')
        add_header(properties, headers, 'quoting')
        add_header(properties, headers, 'routing_engine')

        self.flightclient = client
        self.options = flight.FlightCallOptions(headers=headers)

        self.closed = False
        self.cursors = []

    @check_closed
    def rollback(self) -> None:
        pass

    @check_closed
    def close(self) -> None:
        """Close the connection now."""
        self.closed = True
        for cursor in self.cursors:
            try:
                cursor.close()
            except Error:
                pass  # already closed

    @check_closed
    def commit(self) -> None:
        pass

    @check_closed
    def cursor(self) -> "Cursor":
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(self.flightclient, self.options)
        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(self, query: Executable) -> "Cursor":
        cursor = self.cursor()
        return cursor.execute(query)

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *exc) -> None:
        self.commit()  # no-op
        self.close()


class Cursor:
    """Connection cursor."""

    def __init__(self, flightclient: Optional[FlightClient] = None, options: Optional[Any] = None):
        self.flightclient = flightclient
        self.options = options

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated only after a query
        self.description = None

        # this is set to a list of rows after a successful query
        self._results = None

    @property
    @check_result
    @check_closed
    def rowcount(self) -> int:
        return len(self._results)

    @check_closed
    def close(self) -> None:
        """Close the cursor."""
        self.closed = True

    @check_closed
    def execute(self, query: Union[Executable, str], params: Optional[tuple[Any, ...]] = None) -> "Cursor":
        self.description = None
        if params is not None:
            for param in params:
                if isinstance(param, str):
                    param = f"'{param}'"
                query = query.replace('?', str(param), 1)
        self._results, self.description = execute(query, self.flightclient, self.options)
        return self

    @check_closed
    def executemany(self, query: str) -> Never:
        raise NotSupportedError(
            '`executemany` is not supported, use `execute` instead')

    @check_result
    @check_closed
    def fetchone(self) -> Optional[Any]:
        """
        Fetch the next row of a query result set, returning a single sequence,
        or `None` when no more data is available.
        """
        try:
            return self._results.pop(0)
        except IndexError:
            return None

    @check_result
    @check_closed
    def fetchmany(self, size: Optional[int] = None) -> list[tuple[Any, ...]]:
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        size = size or self.arraysize
        out = self._results[:size]
        self._results = self._results[size:]
        return out

    @check_result
    @check_closed
    def fetchall(self) -> list[tuple[Any, ...]]:
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        out = self._results[:]
        self._results = []
        return out

    @check_closed
    def setinputsizes(self, sizes) -> None:
        # not supported
        raise NotSupportedError()

    @check_closed
    def setoutputsizes(self, sizes) -> None:
        # not supported
        raise NotSupportedError()

    @check_closed
    def __iter__(self) -> Generator[tuple[Any, ...], None, None]:
        return iter(self._results)
