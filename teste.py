import datetime
import pytz

# Obtenha o objeto datetime com o timezone
dt = datetime.datetime(2023, 5, 31, 10, 30, tzinfo=pytz.utc)

# Converta para o fuso horário local
local_timezone = pytz.timezone('America/Sao_Paulo')  # Substitua pelo seu fuso horário
local_dt = dt.astimezone(local_timezone)

# Formate a string
formato = '%Y-%m-%d %H:%M:%S %Z%z'  # Exemplo de formato: "2023-05-31 07:30:00 BRT-0300"
string_formatada = local_dt.strftime(formato)

print(string_formatada)
