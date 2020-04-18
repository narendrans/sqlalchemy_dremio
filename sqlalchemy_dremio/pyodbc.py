import platform

from sqlalchemy.connectors.pyodbc import PyODBCConnector

from .base import DremioExecutionContext, DremioDialect


class DremioExecutionContext_pyodbc(DremioExecutionContext):
    pass


class DremioDialect_pyodbc(PyODBCConnector, DremioDialect):
    execution_ctx_cls = DremioExecutionContext_pyodbc
    driver_for_platform = {
        'Linux 64bit': '/opt/dremio-odbc/lib64/libdrillodbc_sb64.so',
        'Linux 32bit': '/opt/dremio-odbc/lib32/libdrillodbc_sb32.so',
        'Windows': 'Dremio Connector',
        'Darwin': '/Library/Dremio/ODBC/lib/libdrillodbc_sbu.dylib'
    }
    pyodbc_driver_name = driver_for_platform[platform.system() + (' ' + platform.architecture()[0] if platform.system() == 'Linux' else '')]

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')

        connect_args = {}
        connectors = ["DRIVER={%s}" % self.pyodbc_driver_name,
                      "UID=%s" % opts['user'],
                      "PWD=%s" % opts['password'],
                      'HOST=%s' % opts['host'],
                      'PORT=%s' % opts['port'],
                      'Schema=%s' % opts['database']
                      ]

        return [[";".join(connectors)], connect_args]
