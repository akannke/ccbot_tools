import socketio
import os
import csv

ENDPOINT_URL = "https://io.lightstream.bitflyer.com"
PUBLIC_CHANNEL = "lightning_executions_FX_BTC_JPY"

FILE_NAME = "executions.csv"

sio = socketio.Client()

@sio.event
def connect():
    print("connection establised")
    def cb(err):
        if err:
            print(PUBLIC_CHANNEL, "Subscribe Error:", err)
            return
        else:
            print(PUBLIC_CHANNEL, "Subscribed.")

    sio.emit("subscribe", PUBLIC_CHANNEL, callback=cb)

@sio.event
def disconnect():
    print("disconnected from server")

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

