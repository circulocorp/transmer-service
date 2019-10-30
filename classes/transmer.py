from suds.client import Client
from datetime import datetime, timedelta


class Transmer(object):

    def __init__(self, username, password, url=""):
        self._user = username
        self._pass = password
        self._url = url
        self._client = None
        self._token = None

    def _gen_client(self):
        if self._client is None:
            self._client = Client(self._url)

    def _get_token(self):
        token = Client.dict(self._client.service.GetUserToken(self._user, self._pass))
        token['expires'] = datetime.now() + timedelta(hours=24)
        self._token = token

    def _is_token_valid(self):
        now = datetime.now() + timedelta(minutes=30)
        if not self._token:
            return False
        if now > self._token['expires']:
            return False
        else:
            return True

    def send_events(self, events):
        self._gen_client()
        if not self._is_token_valid():
            self._get_token()
        pEvents = []
        for event in events:
            pEvento = self._client.factory.create("ns0:Event")
            pEvento["asset"] = event["Dominio"]
            pEvento["serialNumber"] = event["NroSerie"]
            pEvento["code"] = event["Codigo"]
            pEvento["latitude"] = event["Latitud"]
            pEvento["shipment"] = ""
            pEvento["longitude"] = event["Longitud"]
            pEvento["altitude"] = event["Altitud"]
            pEvento["speed"] = event["Velocidad"]
            pEvento["Customer.name"] = event["customer_name"]
            if event["customer_name"] != "":
                pEvento["Customer.id"] = "41013"
            else:
                pEvento["Customer.id"] = ""
            pEvento["date"] = event["FechaHoraEvento"].replace(" ", "T")
            pEvents.append(pEvento)
        eventos = self._client.factory.create("ns0:ArrayOfEvent")
        eventos.Event = pEvents
        resp = self._client.service.GPSAssetTracking(self._token['token'], eventos)
        return resp
