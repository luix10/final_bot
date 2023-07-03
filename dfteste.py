import pandas as pd

# Criar um dicionário com os dados do dataframe
dados = {'A': [1, 2, 3, 4, 5],
         'B': [10, 20, 30, 40, 50]}

# Criar o dataframe
df = pd.DataFrame(dados)

print(df)

# Deslocar os valores da coluna 'A' para baixo
df['Deslocamento para Baixo'] = df['A'].shift(1)

# Deslocar os valores da coluna 'A' para cima
df['Deslocamento para Cima'] = df['A'].shift(-1)

print(df)
