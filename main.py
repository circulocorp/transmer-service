from classes.transmer import Transmer
from PydoNovosoft.utils import Utils
import datetime
import os
import pika
import json_logging
import logging
import json
import requests
import sys


json_logging.ENABLE_JSON_LOGGING = True
json_logging.init()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

config = Utils.read_config("package.json")
env_cfg = config[os.environ["environment"]]
url = env_cfg["API_URL"]
rabbitmq = env_cfg["RABBITMQ_URL"]
endpoint = env_cfg["ENDPOINT"]
transmer_user = ""
transmer_pass = ""


if env_cfg["secrets"]:
    rabbit_user = Utils.get_secret("rabbitmq_user")
    rabbit_pass = Utils.get_secret("rabbitmq_passw")
    transmer_user = Utils.get_secret("transmer_user")
    transmer_pass = Utils.get_secret("transmer_pass")
else:
    rabbit_user = env_cfg["rabbitmq_user"]
    rabbit_pass = env_cfg["rabbitmq_passw"]


transmer = Transmer(transmer_user, transmer_pass, endpoint)


def get_vehicle(unit_id):
    response = requests.get(url+"/api/vehicles?Unit_Id="+unit_id)
    vehicles = response.json()
    if len(vehicles) > 0:
        return vehicles[0]
    else:
        logger.error("Vehicle not found", extra={'props': {"vehicle": unit_id, "app": config["name"],
                                                           "label": config["name"]}})
        return None


def send(data):
    logger.info("Sending data to Transmer",
                extra={'props': {"raw": data, "app": config["name"], "label": config["name"]}})
    resp = transmer.send_events(data)
    if resp:
        logger.info("Data was accepted by Transmer",
                    extra={'props': {"raw": data, "app": config["name"], "label": config["name"]}})
    else:
        print(resp)
        logger.error(resp, extra={'props': {"raw": data, "app": config["name"], "label": config["name"]}})


def get_name(description):
    if "LETICIA MARTINEZ MADRID" in description.upper():
        return "LETICIA MARTINEZ MADRID"
    elif "GIOVANNI PAREDES CHAVEZ" in description.upper():
        return "GIOVANNI PAREDES CHAVEZ"
    else:
        return ""


def fix_data(msg):
    print("Reading events")
    data = json.loads(msg)
    events = []
    for event in data["events"]:
        pEvent = dict()
        vehicle = get_vehicle(event["header"]["UnitId"])
        if vehicle:
            pEvent["Dominio"] = vehicle["Registration"]
            pEvent["NroSerie"] = vehicle["Description"]
            pEvent["Codigo"] = "1"
            pEvent["customer_name"] = get_name(vehicle["Description"])
            pEvent["Latitud"] = event["header"]["Latitude"]
            pEvent["Longitud"] = event["header"]["Longitude"]
            pEvent["Altitud"] = event["header"]["Odometer"]
            pEvent["Velocidad"] = event["header"]["Speed"]
            pEvent["FechaHoraEvento"] = Utils.format_date(Utils.utc_to_datetime(
                    event["header"]["UtcTimestampSeconds"]), '%Y-%m-%dT%H:%M:%S')
            pEvent["FechaHoraRecepcion"] = Utils.format_date(Utils.string_to_date(data["date"], "%Y-%m-%d %H:%M:%S"),
                                                             "%Y-%m-%dT%H:%M:%S")
            events.append(pEvent)
        else:
            logger.error("Vehicle not found: "+event["header"]["UnitId"], extra={'props': {"app": config["name"],
                                                                                           "label": config["name"]}})
    logger.info("Sending document", extra={'props': {"raw": events, "app": config["name"], "label": config["name"]}})
    if len(events) > 0:
        send(events)


def callback(ch, method, properties, body):
    logger.info("Reading message", extra={'props': {"raw": json.loads(body), "app": config["name"],
                                                    "label": config["name"]}})
    fix_data(body)


def start():
    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    parameters = pika.ConnectionParameters(rabbitmq, 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(config["queue"], durable=True)
    channel.basic_consume(callback, config["queue"], no_ack=True)
    logger.info("Connection successful to RabbitMQ", extra={'props': {"app": config["name"], "label": config["name"]}})
    print("Connection successful to RabbitMQ")
    channel.start_consuming()


def main():
    print(Utils.print_title("package.json"))
    start()


if __name__ == '__main__':
    main()

