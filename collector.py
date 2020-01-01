import socketio
import os
import csv
import logging

ENDPOINT_URL = "https://io.lightstream.bitflyer.com"
PUBLIC_CHANNEL = "lightning_executions_FX_BTC_JPY"

FILE_NAME = "executions.csv"

logger = logging.getLogger(__name__)
fmt = "%(asctime)s %(levelname)s %(name)s :%(message)s"
logging.basicConfig(filename="test.log", level=logging.INFO, format=fmt)

sio = socketio.Client()

@sio.event
def connect():
    logger.info("connection establised")
    def cb(err):
        if err:
            logger.error(f"{PUBLIC_CHANNEL} Subscribe Error: {err}")
            return
        else:
            logger.info(f"{PUBLIC_CHANNEL} Subscribed.")

    sio.emit("subscribe", PUBLIC_CHANNEL, callback=cb)

@sio.event
def disconnect():
    logger.info("disconnected from server")

@sio.on(PUBLIC_CHANNEL)
def receive_event(msgs):
    with open(FILE_NAME, "a") as f:
        fieldnames = ["id", "side", "price", "size", "exec_date", "buy_child_order_acceptance_id", "sell_child_order_acceptance_id"] 
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        for msg in msgs:
            writer.writerow(msg)


if __name__ == "__main__":
    if not os.path.isfile(FILE_NAME):
        with open(FILE_NAME, "w") as f:
            fieldnames = ["id", "side", "price", "size", "exec_date", "buy_child_order_acceptance_id", "sell_child_order_acceptance_id"] 
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

    try:
        sio.connect(ENDPOINT_URL, transports="websocket")
        sio.wait()
    except Exception as e:
        logger.exception(e)
        exit()


