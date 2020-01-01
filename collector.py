import socketio
import os
import csv
import logging

ENDPOINT_URL = "https://io.lightstream.bitflyer.com"
PUBLIC_CHANNEL = "lightning_executions_FX_BTC_JPY"

FILE_NAME = "executions.csv"

logging.basicConfig(filename="test.log", level=logging.INFO)

sio = socketio.Client()

@sio.event
def connect():
    logging.info("connection establised")
    def cb(err):
        if err:
            logging.error(PUBLIC_CHANNEL, "Subscribe Error:", err)
            return
        else:
            logging.info(PUBLIC_CHANNEL, "Subscribed.")

    sio.emit("subscribe", PUBLIC_CHANNEL, callback=cb)

@sio.event
def disconnect():
    logging.info("disconnected from server")

@sio.on(PUBLIC_CHANNEL)
def receive_event(msgs):
    with open(FILE_NAME, "a") as f:
        fieldnames = ["id", "side", "price", "size", "exec_date", "buy_child_order_acceptance_id", "sell_child_order_acceptance_id"] 
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        for msg in msgs:
            writer.writerow(msg)


if __name__ == "__main__":
    if os.path.isfile(FILE_NAME):
        with open(FILE_NAME, "w") as f:
            fieldnames = ["id", "side", "price", "size", "exec_date", "buy_child_order_acceptance_id", "sell_child_order_acceptance_id"] 
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

    sio.connect(ENDPOINT_URL, transports="websocket")
    sio.wait()

