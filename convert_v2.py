import requests
import json
from datetime import datetime, timedelta, timezone
import pandas as pd
import ta
import time

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
    df = adicionarSMA_df(df, periodo)
    for i, item in enumerate(json_data):
        item["sma"] = df["sma"][i] if i >= periodo - 1 else None
    return json_data    

def adicionarSMA_df(df, periodo=12):
    sma_calculada = ta.trend.sma_indicator(df["close"], window=periodo)
    df["sma"] = sma_calculada

    return df

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
    stock_rsi_d = ta.momentum.stochrsi_d(df["close"], window=periodo, smooth1=signal_period, smooth2=signal_period)
    stock_rsi_k = ta.momentum.stochrsi_k(df["close"], window=periodo, smooth1=signal_period, smooth2=signal_period)
    for i, item in enumerate(json_data):
        item["stock_rsi"] = stock_rsi_calculado[i] if i >= periodo - 1 else None
        item["stock_rsi_d"] = stock_rsi_d[i] if i >= periodo - 1 else None
        item["stock_rsi_k"] = stock_rsi_k[i] if i >= periodo - 1 else None        
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

# apenas para fazer print do campo datetime no json como string
def datetime_encoder(obj):
    if isinstance(obj, datetime):
        #return obj.isoformat()
        return obj.strftime('%Y-%m-%d %H:%M:%S %Z')

    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def check_buy_triggers(data, prev_data):
    # Verificar se o MACD acabou de cruzar a linha do sinal (buy signal)
    if data["macd_line"] is not None and data["macd_signal"] is not None and prev_data["macd_line"] is not None and prev_data["macd_signal"]:
        if data["macd_line"] > 0 and data["macd_line"] > data["macd_signal"] and prev_data["macd_line"] < prev_data["macd_signal"] and data["macd_line"] > prev_data["macd_line"]:
            return True
    
    return False

def check_sell_triggers(data, transactions, prev_data):
    # loop pelas transações
    for i in range(0, len(transactions)):
        # confere se está sem venda
        if transactions[i]["sell"] == None:
            # se atingiu a meta percentual então preenche a venda
            if float(data["close"]) > float(transactions[i]["buy"]["close"]) * 1.012:
                transactions[i]["sell"] = data
                transactions[i]["%"] = (float(data["close"]) / float(transactions[i]["buy"]["close"])) - 1
                return True
    return False

# def check_sell_triggers(data, transactions, prev_data):
#     # loop pelas transações
#     for i in range(0, len(transactions)):
#         # confere se está sem venda
#         if transactions[i]["sell"] == None:
#             if data["macd_line"] is not None and data["macd_signal"] is not None and prev_data["macd_line"] is not None and prev_data["macd_signal"]:
#                 # Verificar se o MACD acabou de cruzar PARA BAIXO da linha do sinal (sell signal)
#                 # if data["macd_line"] < data["macd_signal"] and prev_data["macd_line"] > prev_data["macd_signal"]:
#                 # Verificar se o macd_diff DIMINUIU (HISTOGRAMA)
#                 if data["macd_diff"] < prev_data["macd_diff"]:
#                     transactions[i]["sell"] = data
#                     transactions[i]["%"] = (float(data["close"]) / float(transactions[i]["buy"]["close"])) - 1
#                     return True
#     return False

def convertJsonToDataFrame(jsonData):
    df = pd.DataFrame(jsonData)
    # df['timestamp_br'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')       
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['timestamp_br'] = df['timestamp'].dt.tz_convert('America/Sao_Paulo')   
    df = df.set_index('timestamp')
    df = df.sort_index(ascending=True)
    
    # df = df.astype(float)
    
    dt_columns = ['timestamp_br']
    for col in df.columns:
        if col not in dt_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    return df
            
def backtest_strategy(json_data, max_open_positions=3 ):
    transactions = []  # Lista para armazenar as negociações (compra e venda)
    open_positions = 0  # Contador de posições abertas

    prev_data = None
    
    for data in json_data:
        # Condições para comprar
        if (prev_data != None) and (check_buy_triggers(data, prev_data)):
            if open_positions < max_open_positions:
                # Realizar compra
                transaction = {"buy": data, "sell": None}
                transactions.append(transaction)
                open_positions += 1

        # Condições para venda
        if open_positions>0:
            if (prev_data != None) and (check_sell_triggers(data, transactions, prev_data)):
                open_positions -= 1               

        # preenche o data anterior para a próxima checagem
        prev_data = data

    # return trades
    return transactions

def main2():
    
    # Chamar a função obterDadosHitBTC com os parâmetros desejados
    jsonData = obterDadosHitBTC('BTCUSDT', period='H1', limit=1000, sort='DESC')
    
    df = convertJsonToDataFrame(jsonData)
   
    # df = adicionarSMA_df(df, 12)
    df = adicionarSMA_df(df, 3)
    
    # remove registros que possuem campos NaN
    df.dropna(inplace=True)
    
    # print(df.head())
    print(df.tail())
    # print(df.dtypes)
    
    
    time.sleep(30)
    
    
    # traz um candle adicional para atualizar o dataframe
    jsonData2 = obterDadosHitBTC('BTCUSDT', period='H1', limit=1, sort='DESC')
    
    df2 = convertJsonToDataFrame(jsonData2)
    
    df.update(df2)
    
    # calcula novamente os indicadores considerando atualizado
    df = adicionarSMA_df(df, 3)
    
    # print(df.head(12))
    print(df.tail())

def main():
    myTimeZone = timezone(timedelta(hours=-3))

    # Chamar a função obterDadosHitBTC com os parâmetros desejados
    jsonData = obterDadosHitBTC('BTCUSDT', period='H1', limit=1000, sort='DESC')

    # converte campo timestamp para tipo timestamp e adiciona timestamp_br
    jsonData = converterTiposJson(jsonData, timezone=myTimeZone)

    # Ordena os dados por timestamp
    jsonData = ordenarJsonTimestamp(jsonData, sortToAsc=True)

    jsonData = adicionarIndicadores(jsonData)

    json_str = json.dumps(jsonData[:3], indent=4, default=datetime_encoder)
    print(json_str)
    print('...')
    json_str = json.dumps(jsonData[-3:], indent=4, default=datetime_encoder)
    print(json_str)
'''
    trades = backtest_strategy(jsonData, 1)
    
    # print(trades)
    
    perc = 0
    for i in range(0, len(trades)):
        print(f"Compra: {i+1}")
        print(f"    {trades[i]['buy']['timestamp_br']}, {trades[i]['buy']['close']}")
        print(f"Venda: {i+1}")
        if trades[i]["sell"] != None: 
            print(f"    {trades[i]['sell']['timestamp_br']}, {trades[i]['sell']['close']}, {trades[i]['%']}")
            perc += trades[i]['%']
        else:
            print("    None")
        print("\n")
    print(f"Percentual Total: {perc}")
'''

if __name__ == "__main__":
    main()
