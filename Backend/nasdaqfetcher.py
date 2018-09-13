import websocket
import threading
import time
import json

def on_open(ws):
    def run():
        ws.send("")
        time.sleep(1)
        ws.close()
    threading.Thread(target=run).start()

def fetchStocks(symbol, startDate, endDate):
    companyStocks = {}
    def parseMessage(ws, message):
        stock = json.loads(message)
        date = stock['DateStamp'][:10]
        companyStocks[date] = stock['Close']

    url = 'ws://34.214.11.52/stream?symbol={}&start={}&end={}'.format(symbol,startDate,endDate)
    ws = websocket.WebSocketApp(url, on_message = parseMessage)
    ws.on_open = on_open
    ws.run_forever()
    return companyStocks
