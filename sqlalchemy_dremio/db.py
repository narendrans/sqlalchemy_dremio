from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging

from pyarrow import flight

from sqlalchemy_dremio.exceptions import Error, NotSupportedError
from sqlalchemy_dremio.flight_middleware import CookieMiddlewareFactory
from sqlalchemy_dremio.query import execute

logger = logging.getLogger(__name__)

paramstyle = 'qmark'


def connect(c):
    return Connection(c)


def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    def g(self, *args, **kwargs):
        if self.closed:
            raise Error(
                '{klass} already closed'.format(klass=self.__class__.__name__))
        return f(self, *args, **kwargs)

    return g


def check_result(f):
    """Decorator that checks if the cursor has results from `execute`."""

    def d(self, *args, **kwargs):
        if self._results is None:
            raise Error('Called before `execute`')
        return f(self, *args, **kwargs)

    return d


class Connection(object):

    def __init__(self, connection_string):

        # Build a map from the connection string supplied using the SQLAlchemy URI
        # and supplied properties. The format is generated from DremioDialect_flight.create_connect_args()
        # and is a semi-colon delimited string of key=value pairs. Note that the value itself can
        # contain equal signs.
        properties = {}
        splits = connection_string.split(";")

        for kvpair in splits:
            kv = kvpair.split("=",1)
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
                with open(properties['TrustedCerts'] , "rb") as root_certs:
                    connection_args["tls_root_certs"] = root_certs.read()
            # Or disable server verification entirely
            elif 'DisableCertificateVerification' in properties and properties['DisableCertificateVerification'].lower() == 'true':
                connection_args['disable_server_verification'] = True

        # Enabling cookie middleware for stateful connectivity.
        client_cookie_middleware = CookieMiddlewareFactory()

        client = flight.FlightClient('grpc+{0}://{1}:{2}'.format(protocol, properties['HOST'], properties['PORT']),
            middleware=[client_cookie_middleware], **connection_args)
        
        # Authenticate either using basic username/password or using the Token parameter.
        headers = []
        if 'UID' in properties:
            bearer_token = client.authenticate_basic_token(properties['UID'], properties['PWD'])
            headers.append(bearer_token)
        else:
            headers.append((b'authorization', "Bearer {}".format(properties['Token']).encode('utf-8')))

        # Propagate Dremio-specific headers.
        def add_header(properties, headers, header_name):
            if header_name in properties:
                headers.append((header_name.lower().encode('utf-8'), properties[header_name].encode('utf-8')))

        add_header(properties, headers, 'Schema')
        add_header(properties, headers, 'routing_queue')
        add_header(properties, headers, 'routing_tag')
        add_header(properties, headers, 'quoting')
        add_header(properties, headers, 'routing_engine')

        self.flightclient = client
        self.options = flight.FlightCallOptions(headers=headers)

        self.closed = False
        self.cursors = []

    @check_closed
    def rollback(self):
        pass

    @check_closed
    def close(self):
        """Close the connection now."""
        self.closed = True
        for cursor in self.cursors:
            try:
                cursor.close()
            except Error:
                pass  # already closed

    @check_closed
    def commit(self):
        pass

    @check_closed
    def cursor(self):
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(self.flightclient, self.options)
        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(self, query):
        cursor = self.cursor()
        return cursor.execute(query)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.commit()  # no-op
        self.close()


class Cursor(object):
    """Connection cursor."""

    def __init__(self, flightclient=None, options=None):
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
    def rowcount(self):
        return len(self._results)

    @check_closed
    def close(self):
        """Close the cursor."""
        self.closed = True

    @check_closed
    def execute(self, query, params=None):
        self.description = None
        self._results, self.description = execute(
            query, self.flightclient, self.options)
        return self

    @check_closed
    def executemany(self, query, seq_of_parameters=None):
        """Compatibility wrapper for DBAPI executemany.

        ``df.to_sql`` and other helpers expect the ``executemany`` method to
        accept the SQL statement and a sequence of parameters.  Dremio does not
        support parameterized execution, so this method simply raises a
        ``NotSupportedError`` regardless of the parameters passed.
        """

        raise NotSupportedError(
            '`executemany` is not supported, use `execute` instead')

    @check_result
    @check_closed
    def fetchone(self):
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
    def fetchmany(self, size=None):
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
    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        out = self._results[:]
        self._results = []
        return out

    @check_closed
    def setinputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def setoutputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def __iter__(self):
        return iter(self._results)
