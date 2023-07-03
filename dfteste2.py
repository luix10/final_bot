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

def convertJsonToDataFrame(jsonData):
    df = pd.DataFrame(jsonData)
    # df['timestamp_br'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    # converte timestamp para fuso do Brasil
    df['timestamp'] = df['timestamp'].dt.tz_convert('America/Sao_Paulo')       
    df = df.set_index('timestamp')
    df = df.sort_index(ascending=True)
    
    # converte todas colunas para float (menos o index que é tratado separadamente)
    df = df.astype(float)
    
    # converte as colunas para float ou datetime dependendo do nome da coluna
    # dt_columns = ['timestamp_br']
    # for col in df.columns:
    #     if col not in dt_columns:
    #         df[col] = pd.to_numeric(df[col], errors='coerce')
    #     else:
    #         df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # adicionar timestamp_br após timestamp virar o índice
    # df['timestamp_br'] = df.index.tz_convert('America/Sao_Paulo')
    
    return df

def adicionarEMA_df(df, periodo=12):
    ema_calculada = ta.trend.ema_indicator(df["close"], window=periodo)
    df["ema"] = ema_calculada
    
    return df

def adicionarSMA_df(df, periodo=12):
    sma_calculada = ta.trend.sma_indicator(df["close"], window=periodo)
    df["sma"] = sma_calculada

    return df

def adicionarRSI_df(df, periodo=14):
    rsi_calculado = ta.momentum.rsi(df["close"], window=periodo)
    df["rsi"] = rsi_calculado

    return df

def adicionarStockRSI_df(df, periodo=14, signal_period=3):
    stock_rsi_calculado = ta.momentum.stochrsi(df["close"], window=periodo, smooth1=signal_period, smooth2=signal_period)
    stock_rsi_d = ta.momentum.stochrsi_d(df["close"], window=periodo, smooth1=signal_period, smooth2=signal_period)
    stock_rsi_k = ta.momentum.stochrsi_k(df["close"], window=periodo, smooth1=signal_period, smooth2=signal_period)
    
    df["stock_rsi"] = stock_rsi_calculado
    df["stock_rsi_d"] = stock_rsi_d
    df["stock_rsi_k"] = stock_rsi_k
    
    return df

def adicionarMACD_df(df, window_fast=12, window_slow=26, window_sign=9):
    macd = ta.trend.macd( df["close"], window_slow=window_slow, window_fast=window_fast )
    macd_signal = ta.trend.macd_signal( df["close"], window_slow=window_slow, window_fast=window_fast, window_sign=window_sign )
    macd_diff = ta.trend.macd_diff( df["close"], window_slow=window_slow, window_fast=window_fast, window_sign=window_sign )
       
    df["macd_line"] = macd
    df["macd_signal"] = macd_signal
    df["macd_diff"] = macd_diff
    
    return df

def adicionarIndicadores_df(df):
    df = adicionarSMA_df(df, 12)
    df = adicionarEMA_df(df, 12)
    df = adicionarRSI_df(df, 14)
    df = adicionarStockRSI_df(df, 14, 3)
    df = adicionarMACD_df(df, 12, 26, 9)
    
    return df

def check_buy_triggers(df):
    # confere se macd_line acabou de ultrapassar macd_signal
    # df['buy_trigger'] = (df['macd_line'] > df['macd_signal']) & (df['macd_line'].shift() < df['macd_signal'].shift())
    # df['buy_trigger'] = (df['macd_diff'] > 0) & (df['macd_diff'].shift(1) < 0)
    buy_trigger = (df['macd_diff'] > 0) & (df['macd_diff'].shift(1) < 0)
    
    # utiliza o método rolling() do Pandas para calcular uma janela deslizante de tamanho 24 e soma os valores "True" de "trigger"
    # df['trigger'] = df['trigger'] & (df['trigger'].rolling(24).sum() == 0)
    # buy_trigger = buy_trigger & (buy_trigger.shift(1).rolling(5).sum() == 0)
    
    # dúvida se devo usar shift(1) ou shift(-1), não entendi a diferença
    
    df['buy_trigger'] = buy_trigger
    
    return df

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

def main():
    
    # Chamar a função obterDadosHitBTC com os parâmetros desejados
    jsonData = obterDadosHitBTC('BTCUSDT', period='H1', limit=1000, sort='DESC')
    
    df = convertJsonToDataFrame(jsonData)
    
    # df = adicionarSMA_df(df, 12)
    # df = adicionarSMA_df(df, 3)
    # df = adicionarIndicadores_df(df)
    
    # remove registros que possuem campos NaN
    df.dropna(inplace=True)
    
    # df = check_buy_triggers(df)
    df['up'] = df['close'].shift(-1)
    df['down'] = df['close'].shift(1)
    
    # print(df.head())
    # print(df.tail(70))
    for i in range(len(df)-5, len(df)):
        # print(df.iloc[i])
        print(f'{df.index[i]}, close: {df.iloc[i]["close"]}, up: {df.iloc[i]["up"]}, down: {df.iloc[i]["down"]}')
        # print(df.index[i])
    # print(df.dtypes)
    # print(df.columns)
    
    # for data in df.tail(6).shift(1):
    #     print(data)

'''
    time.sleep(30)
    # traz dados adicionais de candles para atualizar o dataframe
    jsonData2 = obterDadosHitBTC('BTCUSDT', period='H1', limit=1, sort='DESC')
    df2 = convertJsonToDataFrame(jsonData2)
    
    # atualiza o df com df2
    df.update(df2)
    
    # calcula novamente os indicadores considerando df atualizado
    df = adicionarIndicadores_df(df, 3)
    
    print(df.head())
    print(df.tail())
'''

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
