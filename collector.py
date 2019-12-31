import socketio

ENDPOINT_URL = "https://io.lightstream.bitflyer.com"
PUBLIC_CHANNEL = "lightning_executions_FX_BTC_JPY"

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
    print('disconnected from server')

@sio.on(PUBLIC_CHANNEL)
def receive_event(msg):
    print(msg)

sio.connect(ENDPOINT_URL, transports="websocket")
sio.wait()

