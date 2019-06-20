from suds.client import Client


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
        pEvents = []
        for event in events:
            pEvento = self._client.factory.create("pEvento")
            pEvento["Dominio"] = event["Dominio"]
            pEvento["NroSerie"] = event["NroSerie"]
            pEvento["Codigo"] = event["Codigo"]
            pEvento["Latitud"] = event["Latitud"]
            pEvento["Longitud"] = event["Longitud"]
            pEvento["Altitud"] = event["Altitud"]
            pEvento["Velocidad"] = event["Velocidad"]
            pEvento["FechaHoraEvento"] = event["FechaHoraEvento"].replace(" ", "T")
            pEvento["FechaHoraRecepcion"] = event["FechaHoraRecepcion"].replace(" ", "T")
            pEvents.append(pEvento)
        eventos = self._client.factory.create("ArrayOfPEvento")
        eventos.pEvento = pEvents
        resp = self._client.service.LoginYInsertarEventos(self._user, self._pass, eventos)
        return resp
