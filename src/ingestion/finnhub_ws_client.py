# ingestion/finnhub_ws_client.py

import json

import websocket
from confluent_kafka import Producer

from ingestion.config import (
    FINNHUB_API_KEY,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_TRADES,
    STOCK_SYMBOLS,
)

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def on_delivery(err, msg):
    if err is not None:
        print(f"Échec de livraison pour {msg.key()}: {err}")


def on_open(ws):
    print("Connexion WebSocket ouverte, abonnement aux symboles...")
    for symbol in STOCK_SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))


def on_message(ws, message):
    payload = json.loads(message)

    if payload.get("type") != "trade":
        return

    for trade in payload["data"]:
        print("Trade reçu :", trade)
        key = trade["s"].encode("utf-8")
        value = json.dumps(trade).encode("utf-8")

        producer.produce(
            topic=KAFKA_TOPIC_TRADES,
            key=key,
            value=value,
            callback=on_delivery,
        )

    producer.poll(0)


def on_error(ws, error):
    print(f"Erreur WebSocket : {error}")


def on_close(ws, close_status_code, close_msg):
    print("Connexion WebSocket fermée.")
    producer.flush()


if __name__ == "__main__":
    ws_app = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws_app.run_forever()