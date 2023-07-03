import requests
import json
from datetime import datetime, timedelta, timezone
import pandas as pd
import ta

def obterDadosHitBTC(symbol, period='M30', limit=100, sort='DESC', from_time=None, till_time=None):
    # base_url = 'http://localhost:8081/api/3/public/candles'
    base_url = 'https://api.hitbtc.com/api/3/public/candles'
    url = f'{base_url}/{symbol}?period={period}&limit={limit}&sort={sort}'
	
    if from_time:
        url += f'&from={from_time}'
    if till_time:
        url += f'&till={till_time}'

    print(url)

    response = requests.get(url)

    if response.status_code == 200:
        jsonData = json.loads(response.text)
        return jsonData
    else:
        raise Exception('Erro ao obter os dados da HitBTC. Código de status: ' + str(response.status_code))

def converterTiposJson(json_data, timezone=None):
    # converte o campo timestamp
    for item in json_data:
        timestamp = datetime.fromisoformat(item["timestamp"])
        item["timestamp"] = timestamp

        # campo adicional para o timezone especificado (se houver)
        if timezone:
            timestamp_br = timestamp.astimezone(timezone) # altera o timezone
            item["timestamp_br"] = timestamp_br

    return json_data

def ordenarJsonTimestamp(json_data, sortToAsc=True):
    # ordena os dados por timestamp
    json_data_sorted = sorted(json_data, key=lambda x: x["timestamp"], reverse=not sortToAsc)

    return json_data_sorted

def adicionarEMA(json_data, periodo=12):
    close_prices = [float(item["close"]) for item in json_data]
    df = pd.DataFrame({"close": close_prices})
    ema_calculada = ta.trend.ema_indicator(df["close"], window=periodo)
    for i, item in enumerate(json_data):
        item["ema"] = ema_calculada[i] if i >= periodo - 1 else None
    return json_data

def adicionarSMA(json_data, periodo=12):
    close_prices = [float(item["close"]) for item in json_data]
    df = pd.DataFrame({"close": close_prices})
    sma_calculada = ta.trend.sma_indicator(df["close"], window=periodo)
    for i, item in enumerate(json_data):
        item["sma"] = sma_calculada[i] if i >= periodo - 1 else None
    return json_data

def adicionarRSI(json_data, periodo=14):
    close_prices = [float(item["close"]) for item in json_data]
    df = pd.DataFrame({"close": close_prices})
    rsi_calculado = ta.momentum.rsi(df["close"], window=periodo)
    for i, item in enumerate(json_data):
        item["rsi"] = rsi_calculado[i] if i >= periodo - 1 else None
    return json_data

def adicionarStockRSI(json_data, periodo=14, signal_period=3):
    close_prices = [float(item["close"]) for item in json_data]
    df = pd.DataFrame({"close": close_prices})
    stock_rsi_calculado = ta.momentum.stochrsi(df["close"], window=periodo, smooth1=signal_period, smooth2=signal_period)
    for i, item in enumerate(json_data):
        item["stock_rsi"] = stock_rsi_calculado[i] if i >= periodo - 1 else None
    return json_data

def adicionarMACD(json_data, window_fast=12, window_slow=26, window_sign=9):
    close_prices = [float(item["close"]) for item in json_data]
    df = pd.DataFrame({"close": close_prices})
    
    macd = ta.trend.macd( df["close"], window_slow=window_slow, window_fast=window_fast )
    macd_signal = ta.trend.macd_signal( df["close"], window_slow=window_slow, window_fast=window_fast, window_sign=window_sign )
    macd_diff = ta.trend.macd_diff( df["close"], window_slow=window_slow, window_fast=window_fast, window_sign=window_sign )
    
    for i, item in enumerate(json_data):
        item["macd_line"] = macd[i] if i >= window_slow - 1 else None
        item["macd_signal"] = macd_signal[i] if i >= window_slow + window_sign - 2 else None
        item["macd_diff"] = macd_diff[i] if i >= window_slow + window_sign - 2 else None
    return json_data

def adicionarIndicadores(jsonData):
  jsonData = adicionarSMA(jsonData, 12)
  jsonData = adicionarEMA(jsonData, 12)
  jsonData = adicionarRSI(jsonData, 14)
  jsonData = adicionarStockRSI(jsonData, 14, 3)
  jsonData = adicionarMACD(jsonData, 12, 26, 9)  
  return jsonData


def backtest_strategy(json_data, max_open_positions=1):
    # trades = []  # Lista para armazenar as negociações (compra e venda)
    buys = []
    sells = []
    open_positions = 0  # Contador de posições abertas

    # Variáveis para rastrear as condições anteriores
    previous_histogram = None
    previous_macd = None
    previous_signal = None
    highest_histogram = None

    for data in json_data:
        # Obter os valores dos indicadores para o ponto de dados atual
        histogram = data.get("macd_diff")
        macd = data.get("macd_line")
        signal = data.get("macd_signal")

        # Condições para comprar
        if histogram is not None and macd is not None and signal is not None and previous_macd is not None and previous_histogram is not None:
            if macd > signal and previous_macd < signal and macd > previous_macd:
                if open_positions < max_open_positions:
                    # Realizar compra
                    #trade = {"action": "buy", "price": data["close"], "timestamp": data["timestamp"], "quantity": 1}
                    #trades.append(trade)
                    buys.append(data)
                    open_positions += 1

        # Condições para vender
        if open_positions > 0 and histogram is not None and previous_histogram is not None:
            if highest_histogram is None or histogram > highest_histogram:
                highest_histogram = histogram
            elif histogram < highest_histogram :
                # Realizar venda
                # trade = {"action": "sell", "price": data["close"], "timestamp": data["timestamp"], "quantity": 1}
                # trades.append(trade)
                sells.append(data)
                open_positions -= 1
                highest_histogram = None

        # Atualizar as condições anteriores
        previous_histogram = histogram
        previous_macd = macd
        previous_signal = signal

    # return trades
    return buys, sells

# apenas para fazer print do campo datetime no json como string
def datetime_encoder(obj):
    if isinstance(obj, datetime):
        #return obj.isoformat()
        return obj.strftime('%Y-%m-%d %H:%M:%S %Z')

    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def main():
  myTimeZone = timezone(timedelta(hours=-3))

  # Chamar a função obterDadosHitBTC com os parâmetros desejados
  jsonData = obterDadosHitBTC('BTCUSDT', period='H1', limit=1000, sort='DESC')
#   jsonData = obterDadosHitBTC('BTCUSDT', period='M1', limit=30, sort='DESC')
  
  # converte campo timestamp para tipo timestamp e adiciona timestamp_br
  jsonData = converterTiposJson(jsonData, timezone=myTimeZone)
  
  # Ordena os dados por timestamp
  jsonData = ordenarJsonTimestamp(jsonData, sortToAsc=True)
  
#   additional_jsonData = obterDadosHitBTC('BTCUSDT', period='M1', limit=2, sort='DESC')
#   additional_jsonData = converterTiposJson(additional_jsonData, timezone=myTimeZone)
  
#   jsonData.extend(additional_jsonData)
#   jsonData = ordenarJsonTimestamp(jsonData, sortToAsc=True)
   
#   json_str = json.dumps(jsonData, indent=4, default=datetime_encoder)
#   print(json_str)

#  for i, item in enumerate(jsonData):
    #print(jsonData[i]['timestamp'])

  jsonData = adicionarIndicadores(jsonData)
#   trades = backtest_strategy(jsonData)
  buys, sells = backtest_strategy(jsonData)
#   print(trades)

#   total_profit = 0
#   for i in range(0, len(trades), 2):
#     buy_trade = trades[i]
#     sell_trade = trades[i + 1] if i + 1 < len(trades) else None

#     buy_price = float(buy_trade["price"])
#     sell_price = float(sell_trade["price"]) if sell_trade else None

#     if sell_price is None:
#         profit_percentage = "N/A"
#     else:
#         profit = sell_price - buy_price
#         profit_percentage = (profit / buy_price) * 100
        
#     if sell_price:
#         total_profit += profit

#     print(f"Compra: Preço {buy_price:.2f} - Venda: Preço {sell_price if sell_price else 'N/A'} - Lucro/Prejuízo %: {profit_percentage if profit_percentage == 'N/A' else profit_percentage:.2f}% - Lucro/Prejuízo: {profit if profit == 'N/A' else profit:.2f}")
    
#   print("\n")
#   print(total_profit)

  perc = 0
  for i in range(0, len(sells), 2):
    perc = (float(sells[i]['close']) / float(buys[i]['close'])) - 1
          
    print(f"Transação {i+1}")
    print(f"Compra: {buys[i]['timestamp_br']}, {buys[i]['close']}")
    print(f"Venda: {sells[i]['timestamp_br']}, {sells[i]['close']}, Percentual: {perc*100}")
    print("\n")
#   json_str = json.dumps(jsonData[:3], indent=4, default=datetime_encoder)
#   print(json_str)
#   print('...')
#   json_str = json.dumps(jsonData[-3:], indent=4, default=datetime_encoder)
#   print(json_str)

   
if __name__ == "__main__":
  main()
