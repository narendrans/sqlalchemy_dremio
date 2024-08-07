from http.cookies import SimpleCookie

from pyarrow._flight import CallInfo
from pyarrow.flight import ClientMiddleware
from pyarrow.flight import ClientMiddlewareFactory


class CookieMiddlewareFactory(ClientMiddlewareFactory):
    """A factory that creates CookieMiddleware(s)."""

    def __init__(self, *args, **kwargs) -> None:
        self.cookies = {}
        super().__init__(*args, **kwargs)

    def start_call(self, info: CallInfo) -> "CookieMiddleware":
        return CookieMiddleware(self)


class CookieMiddleware(ClientMiddleware):
    """
    A ClientMiddleware that receives and retransmits cookies.
    For simplicity, this does not auto-expire cookies.
    Parameters
    ----------
    factory : CookieMiddlewareFactory
        The factory containing the currently cached cookies.
    """

    def __init__(self, factory: CookieMiddlewareFactory, *args, **kwargs) -> None:
        self.factory = factory
        super().__init__(*args, **kwargs)

    def received_headers(self, headers: dict[str, str]) -> None:
        for key in headers:
            if key.lower() == 'set-cookie':
                cookie = SimpleCookie()
                for item in headers.get(key):
                    cookie.load(item)

                self.factory.cookies.update(cookie.items())

    def sending_headers(self) -> dict[bytes, bytes]:
        if self.factory.cookies:
            cookie_string = '; '.join(f"{key}={val.value}" for key, val in self.factory.cookies.items())
            return {b'cookie': cookie_string.encode('utf-8')}
        return {}
