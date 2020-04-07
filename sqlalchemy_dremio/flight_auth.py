from pyarrow.flight import BasicAuth, ClientAuthHandler


class HttpDremioClientAuthHandler(ClientAuthHandler):
    def __init__(self, username, password):
        super(ClientAuthHandler, self).__init__()
        self.basic_auth = BasicAuth(username, password)
        self.token = None

    def authenticate(self, outgoing, incoming):
        auth = self.basic_auth.serialize()
        outgoing.write(auth)
        self.token = incoming.read()

    def get_token(self):
        return self.token