from suds.client import Client
import xmltodict


class Transmer(object):

    def __init__(self, username, password, url=""):
        self._user = username
        self._pass = password
        self._url = url
        self._client = None

    def _gen_client(self):
        if self._client is None:
            self._client = Client(self._url)

    def send_events(self, events):
        self._gen_client()
        events["LoginYInsertarEventos"]["SystemUser"] = self._user
        events["LoginYInsertarEventos"]["Password"] = self._pass
        xml = xmltodict.unparse(events)
        resp = self._client.service.LoginYInsertarEventos(self._user, self._pass,
                                                          events["LoginYInsertarEventos"]["Eventos"])
        return resp
