import pandas as pd
import numpy as np
import ta
import time
import os
from dotenv import load_dotenv
from binance import Client
# import matplotlib.pyplot as plt

load_dotenv()

bin_api_key = os.getenv('bin_api_key')
bin_api_sec = os.getenv('bin_api_sec')

client = Client(bin_api_key, bin_api_sec)

def getminutedata(symbol, interval, lookback):
    lookback = str(lookback)
    frame = pd.DataFrame(
        client.get_historical_klines(
            symbol,
            interval,
            lookback + ' min ago UTC'
        )
    )
    frame = frame.iloc[:, :6]
    frame.columns = ['Time','Open','High','Low','Close','Volume']
    frame = frame.set_index('Time')
    frame.index = pd.to_datetime(frame.index, unit='ms')
    frame = frame.astype(float)
    return frame

# df = getminutedata(symbol='BTCUSDT', interval='15m', lookback=720)
#df = getminutedata(symbol='BTCUSDT', interval='15m', lookback=(7*24*60)) #7 dias
df = getminutedata(symbol='BTCUSDT', interval='1h', lookback=(2*24*60)) #2 dias

print(df)
exit()

def applytechnicals(df):
    df['%K'] = ta.momentum.stoch(df.High, df.Low, df.Close, window=14, smooth_window=3)
    df['%D'] = df['%K'].rolling(3).mean()
    df['rsi'] = ta.momentum.rsi(df.Close, window=14)
    # df['macd'] = ta.trend.macd(df.Close)
    # df['macd_signal'] = ta.trend.macd_signal(df.Close)
    df['macd_diff'] = ta.trend.macd_diff(df.Close)    
    df.dropna(inplace=True)

applytechnicals(df)

print(df)
df.to_json("storage/MY_DF.json")

exit()

def gettriggers(df, lags, buy):
    """ Confere se nos últimos x Candles (x=lags) houve cruzamento das linhas %K e %D abaixo de 20 ou acima de 80

    Args:
        df (pandas dataframe): Timestamp + OCHL
        lags (int): [description]
        buy (boolean): [description]

    Returns:
        [dataframe]: retorna uma coluna com valor booleano caso a checagem dê positivo
    """
    dfx = pd.DataFrame()
    for i in range(1, lags+1):
        if buy:
            mask = (df['%K'].shift(i) < 20) & (df['%D'].shift(i) < 20)
        else:
            mask = (df['%K'].shift(i) > 80) & (df['%D'].shift(i) > 80)
        dfx = dfx.append(mask, ignore_index=True)
    return dfx.sum(axis=0)

def decide(df):
    df['BuyTrigger'] = np.where(gettriggers(df, lags=4, buy=True), 1, 0)
    df['SellTrigger'] = np.where(gettriggers(df, lags=4, buy=False), 1, 0)

    df['Buy'] = np.where(
        (
            (df.BuyTrigger) &
            (df['%K'].between(20,80)) &
            (df['%D'].between(20,80)) &
            (df.rsi > 50) &
            (df.macd_diff > 0)
        ), 1, 0)

    df['Sell'] = np.where(
        (
            (df.SellTrigger) &
            (df['%K'].between(20,80)) &
            (df['%D'].between(20,80)) &
            (df.rsi < 50) &
            (df.macd_diff < 0)
        ), 1, 0)

    # print(df[df.Buy==True])
    # print(df[df.Sell==True])
    
decide(df)
print(df)
# print(df[df.Buy==True])
# print(df[df.Sell==True])



Buying_dates, Selling_dates = [], []

for i in range(len(df) - 1):
    if df.Buy.iloc[i]:
        Buying_dates.append(df.iloc[i + 1].name)

        for num,j in enumerate(df.Sell[i:]):
            if j:
                Selling_dates.append(df.iloc[i + num + 1].name)
                break
            
cutit = len(Buying_dates) - len(Selling_dates)
if cutit:
    Buying_dates = Buying_dates[:-cutit]
    
# aqui estão todos os pontos de entrada e saída, porém não bate certinho as datas e posição comprado x vendido
# ou seja, são feitas múltiplas compras, vendas que acontecem antes das compras, etc
df_exec = pd.DataFrame({'Buying_dates': Buying_dates, 'Selling_dates': Selling_dates})

# aqui é uma tentativa de manter apenas os pares de compra e venda que fazem sentido de data 
# esse é o exemplo do vídeo, mas ainda é falho pois pula o primeiro registro (por não ter selling date anterior)
# df_actuals = df_exec[df_exec.Buying_dates > df_exec.Selling_dates.shift(1)]
df_actuals = df_exec[df_exec.Buying_dates > df_exec.Selling_dates.shift(1)]

def profit_calc():
    Buyprices = df.loc[df_actuals.Buying_dates].Open
    Sellprices = df.loc[df_actuals.Selling_dates].Open
    return (Sellprices.values - Buyprices.values) / Buyprices.values

profits = profit_calc()
print(profits)
print(profits.mean())
print((profits +1).prod())

# print(df)
df.to_json("storage/MY_DF.json")
