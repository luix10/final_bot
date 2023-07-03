import requests
import json
from datetime import datetime, timedelta, timezone
import pandas as pd
import ta
import time

def obterDadosHitBTC_v1(symbol, period='M30', limit=100, sort='DESC', from_time=None, till_time=None):
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
    
def obterDadosHitBTC(symbol, period='M30', limit=100, sort='ASC', from_time=None, till_time=None):
    # base_url = 'http://localhost:8081/api/3/public/candles'
    base_url = 'https://api.hitbtc.com/api/3/public/candles'
    
    # Quando tiver from_time, significa que a ordem é crescente e vai chamando a api até chegar na data esperada
    # Quando não tiver, significa que a ordem dos dados é descrente e vai chamar dados até o limit (últimos x registros de agora para trás)    
    busca_por_limite = False
    if (from_time == None):
        busca_por_limite = True
        
    if busca_por_limite:
        sort = 'DESC'
    
    url = f'{base_url}/{symbol}?period={period}&limit={limit}&sort={sort}'
    
    results = []
    
    while True:
        url_date = url
        
        if from_time:
            url_date = url_date + f'&from={from_time}'
        if till_time:
            url_date = url_date + f'&till={till_time}'

        print(url_date)

        response = requests.get(url_date)

        if response.status_code == 200:
            jsonData = json.loads(response.text)
            results.extend(jsonData)
            
            if len(jsonData) < limit or busca_por_limite:
                break  # Se o número de registros retornados for menor que o limite, todos os dados foram obtidos
            
            # Atualize from_time para o próximo intervalo
            if sort=='ASC':
                from_time = jsonData[-1]['timestamp']           
        else:
            raise Exception('Erro ao obter os dados da HitBTC. Código de status: ' + str(response.status_code))

        # Aguarde um breve intervalo para evitar sobrecarregar a API
        time.sleep(1)

    return results

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

def adicionarStochRSI_df(df, periodo=14, signal_period=3):
    stoch_rsi_calculado = ta.momentum.stochrsi(df["close"], window=periodo, smooth1=signal_period, smooth2=signal_period)
    # stoch_rsi_d = ta.momentum.stochrsi_d(df["close"], window=periodo, smooth1=signal_period, smooth2=signal_period)
    # stoch_rsi_k = ta.momentum.stochrsi_k(df["close"], window=periodo, smooth1=signal_period, smooth2=signal_period)

    df["stoch_rsi"] = stoch_rsi_calculado
    # df["stoch_rsi_d"] = stoch_rsi_d
    # df["stoch_rsi_k"] = stoch_rsi_k
    
    return df

def adicionarMACD_df(df, window_fast=12, window_slow=26, window_sign=9):
    macd = ta.trend.macd( df["close"], window_slow=window_slow, window_fast=window_fast )
    macd_signal = ta.trend.macd_signal( df["close"], window_slow=window_slow, window_fast=window_fast, window_sign=window_sign )
    macd_diff = ta.trend.macd_diff( df["close"], window_slow=window_slow, window_fast=window_fast, window_sign=window_sign )
       
    df["macd_line"] = macd
    df["macd_signal"] = macd_signal
    df["macd_diff"] = macd_diff
    
    return df
	
def adicionarSTOCH_df(df, periodo=14, smooth_window=3):
	stoch_k = ta.momentum.stoch(df['max'], df['min'], df['close'], window=periodo, smooth_window=smooth_window)
	stoch_d = stoch_k.rolling(3).mean()	
	
	df['stoch_k'] = stoch_k
	df['stoch_d'] = stoch_d
 
	return df

def adicionarIndicadores_df(df):
    df = adicionarSMA_df(df, 12)
    df = adicionarEMA_df(df, 12)
    df = adicionarRSI_df(df, 14)
    df = adicionarStochRSI_df(df, 14, 3)
    df = adicionarMACD_df(df, 12, 26, 9)
    df = adicionarSTOCH_df(df, 14, 3)
	
    return df

def check_buy_triggers_v1(df):
    # confere se macd_line acabou de cruzar acima de macd_signal
    # buy_trigger = (df['macd_line'] > df['macd_signal']) & (df['macd_line'].shift(1) < df['macd_signal'].shift(1))
    buy_trigger = (df['macd_diff'] > 0) & (df['macd_diff'].shift(1) < 0)
    
    # Confere se não houve buy_trigger nos últimos 24 candles (para evitar compras muito próximas)
    # utiliza o método rolling() do Pandas para calcular uma janela deslizante de tamanho 24 
    # utiliza shift(1) para avançar os valores para frente, cortando assim o valor atual para não ser considerado
    # e por fim, soma os valores "True" de "trigger"
    buy_trigger = buy_trigger & (buy_trigger.shift(1).rolling(24).sum() == 0)
    
    df['buy_trigger'] = buy_trigger
    
    return df

def check_sell_triggers_v1(df):
    # confere se macd_line acabou de cruzar abaixo de macd_signal
    # sell_trigger = (df['macd_line'] < df['macd_signal']) & (df['macd_line'].shift(1) > df['macd_signal'].shift(1))
    sell_trigger = (df['macd_diff'] < 0) & (df['macd_diff'].shift(1) > 0)
    
    # Confere se não houve sell_trigger nos últimos 24 candles (para evitar vendas muito próximas)
    # utiliza o método rolling() do Pandas para calcular uma janela deslizante de tamanho 24 
    # utiliza shift(1) para avançar os valores para frente, cortando assim o valor atual para não ser considerado
    # e por fim, soma os valores "True" de "trigger"
    # sell_trigger = sell_trigger & (sell_trigger.shift(1).rolling(24).sum() == 0)
    
    df['sell_trigger'] = sell_trigger
    
    return df

def check_buy_triggers(df):
    # confere se as linhas K% e D% estão abaixo de 20
    k_d_abaixo = (df['stoch_k'] < 20) & (df['stoch_d'] < 20)
    df['k_d_abaixo'] = k_d_abaixo
    
    # Confere se k e d estiveram abaixo de 20 nos últimos 5 candles
    # utiliza o método rolling() do Pandas para calcular uma janela deslizante de tamanho 5 
    # utiliza shift(1) para avançar os valores para frente, cortando o valor atual para não ser considerado
    # e por fim, soma os valores "True" de "k_d_abaixo"
    buy_trigger = (
        (df['stoch_k'] > 20) &
        (df['stoch_d'] > 20) &
        (df['stoch_k'] < 80) &
        (df['stoch_d'] < 80) &
        (k_d_abaixo.shift(1).rolling(5).sum() > 0) &
        (df["rsi"] > 50) &
        (df["macd_diff"] > 0)
    )
    
    df['buy_trigger'] = buy_trigger
    
    return df

def check_sell_triggers(df):
    # confere se as linhas K% e D% estão acima de 80   
    k_d_acima = (df['stoch_k'] > 80) & (df['stoch_d'] > 80)
    df['k_d_acima'] = k_d_acima
    
    # Confere se k e d estiveram acima de 80 nos últimos 5 candles
    # utiliza o método rolling() do Pandas para calcular uma janela deslizante de tamanho 5 
    # utiliza shift(1) para avançar os valores para frente, cortando o valor atual para não ser considerado
    # e por fim, soma os valores "True" de "k_d_acima"
    sell_trigger = (
        (df['stoch_k'] > 20) &
        (df['stoch_d'] > 20) &
        (df['stoch_k'] < 80) &
        (df['stoch_d'] < 80) &
        (k_d_acima.shift(1).rolling(5).sum() > 0) &
        (df["rsi"] < 50) &
        (df["macd_diff"] < 0)
    )
    
    # Confere se não houve sell_trigger nos últimos 24 candles (para evitar vendas muito próximas)
    # utiliza o método rolling() do Pandas para calcular uma janela deslizante de tamanho 24 
    # utiliza shift(1) para avançar os valores para frente, cortando assim o valor atual para não ser considerado
    # e por fim, soma os valores "True" de "trigger"
    # sell_trigger = sell_trigger & (sell_trigger.shift(1).rolling(24).sum() == 0)
    
    df['sell_trigger'] = sell_trigger
    
    return df

def backtest_strategy(df, initial_cash=1000, max_open_positions=1, venda_forcada=False):
    positions = []  # Lista de posições de compra
    open_positions = 0  # Contagem de posições de compra em aberto atualmente
    cash = initial_cash
    
    for index, row in df.iterrows():
        if row['buy_trigger'] and open_positions < max_open_positions:
            buy_cash = cash / (max_open_positions - open_positions)
            buy_ammount = buy_cash / row['close']
            cash -= buy_cash
            positions.append({
                'buy_timestamp': index,
                'buy_price': row['close'],
                'buy_cash': buy_cash,
                'buy_ammount': buy_ammount,
                'sell': False,
            })  # Adiciona uma nova posição de compra na lista
            open_positions += 1  # Incrementa a quantidade de posições de compra em aberto
        
        if row['sell_trigger']:
            for position in positions:
                # if 'sell' not in position:  # Verifica se a posição não foi fechada
                if position['sell'] == False:
                    if (row['close'] > (position['buy_price'] * 1.01)): # vende apenas se atingiu o lucro mínimo esperado
                        position['sell'] = True
                        position['sell_timestamp'] = index  # Define o timestamp da venda para a posição de compra
                        position['sell_price'] = row['close']  # Define o preço de venda para a posição de compra
                        position['profit'] = (position['buy_ammount'] * position['sell_price']) - (position['buy_ammount'] * position['buy_price'])
                        position['profit_perc'] = (position['sell_price'] / position['buy_price']) - 1 
                        cash += position['buy_ammount'] * row['close']
                        open_positions -= 1  # Decrementa a quantidade de posições de compra em aberto
                        
                        break  # Sai do loop após encontrar uma posição de venda para a compra em aberto
                    
    # adiciona venda forçada ao último registro, fazendo uma saída completa de mercado
    if venda_forcada:
        last_row = df.iloc[-1]
        for position in positions:
            if not position['sell']:
                position['sell'] = True
                position['sell_timestamp'] = last_row.name  # Define o timestamp da venda como o último registro
                position['sell_price'] = last_row['close']  # Define o preço de venda como o preço de fechamento do último registro
                position['profit'] = (position['buy_ammount'] * position['sell_price']) - (position['buy_ammount'] * position['buy_price'])
                position['profit_perc'] = (position['sell_price'] / position['buy_price']) - 1
                cash += position['buy_ammount'] * last_row['close']
                open_positions -= 1  # Decrementa a quantidade de posições de compra em aberto
    
    return positions, cash, open_positions

def df_to_csv(df, nome_arquivo, separador_csv=';', separador_decimal=','):
     # Cria uma cópia do DataFrame
    df_formatado = df.copy()
    
    # Seleciona apenas as colunas numéricas
    colunas_numericas = df_formatado.select_dtypes(include=['int', 'float']).columns
    
    # Percorre as colunas numéricas
    for coluna in colunas_numericas:
        # Converte os valores numéricos para strings formatadas
        # remove os separadores de milhares no padrão EUA
        df_formatado[coluna] = df_formatado[coluna].apply(lambda x: f'{x:,}'.replace(',', ''))
        # troca o separador de decimal do padrão EUA para o especificado
        df_formatado[coluna] = df_formatado[coluna].apply(lambda x: f'{x:s}'.replace('.', separador_decimal))
    
    # Salva o DataFrame em um arquivo CSV
    df_formatado.to_csv(nome_arquivo, sep=separador_csv, index=True)
    
def main():
    # till_time = time.strftime("%Y-%m-%dT%H:%M:%S")  # till_time será o momento atual
    # from_time = '2023-01-01T00:00:00'
    
    from_time = '2022-01-01T00:00:00'
    till_time = '2023-03-01T00:00:00'
    
    # Chamar a função obterDadosHitBTC com os parâmetros desejados   
    jsonData = obterDadosHitBTC('BTCUSDT', period='H1', from_time=from_time, till_time=till_time, limit=1000)
        
    df = convertJsonToDataFrame(jsonData)
    
    # df = adicionarSMA_df(df, 12)
    # df = adicionarSMA_df(df, 3)
    df = adicionarIndicadores_df(df)
    
    # remove registros que possuem campos NaN
    df.dropna(inplace=True)
    
    # preenche "buy_trigger"
    df = check_buy_triggers(df)

    # preenche "sell_trigger"
    df = check_sell_triggers(df)
    
    print(df)
    # df.to_csv("storage/MY_DF.csv", sep=';')
    df_to_csv(df, "storage/MY_DF.csv")
    # print(df.head())    

    # Obter a lista de transações baseado em "buy_trigger" e "sell_trigger"
    transacoes, banco, em_aberto = backtest_strategy(df, initial_cash=1000, max_open_positions=10, venda_forcada=False)

    for transacao in transacoes:
        print(transacao)
        
    print(f'em_aberto: {em_aberto}')
    print(f'banco: {banco}')
    
    #Converte as transacoes para dataframe
    dft = pd.DataFrame(transacoes)
    # dft = dft.set_index('buy_timestamp')
    # dft = dft.sort_index(ascending=True)
    
    # print(dft)
    # print(dft.dtypes)
    
    df_to_csv(dft, "storage/MY_DF_TRANS.csv")
    
    # buy_signals = df.loc[df['buy_trigger'] == True]
    # print(buy_signals)
    
    # print(df.tail(70))
    # for i in range(0, 10):
    # for i in range(len(df)-101, len(df)):
        # print(df.iloc[i])
        # print(df.index[i])
        # print(f'{df.index[i]}, close: {df.iloc[i]["close"]}, buy_trigger1: {df.iloc[i]["buy_trigger1"]}, buy_trigger2: {df.iloc[i]["buy_trigger2"]}')
        
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

if __name__ == "__main__":
    main()
