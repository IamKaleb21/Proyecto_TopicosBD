import pandas as pd
import numpy as np
from datetime import datetime

# Columnas esperadas y sus tipos
COLUMNS_TYPES = {
    'hotel': str,
    'fue_cancelada': int,
    'tiempo_anticipacion_reserva_dias': int,
    'anio_llegada': int,
    'mes_llegada': str,
    'semana_llegada': int,
    'dia_llegada': int,
    'noches_fin_semana': int,
    'noches_semana': int,
    'adultos': int,
    'ninos': float,
    'bebes': float,
    'regimen_alimenticio': str,
    'pais_origen_cliente': str,
    'segmento_mercado': str,
    'canal_reserva': str,
    'es_huesped_recurrente_historico': int,
    'total_cancelaciones_previas_cliente': int,
    'total_reservas_previas_no_canceladas_cliente': int,
    'tipo_habitacion_reservada': str,
    'tipo_habitacion_asignada': str,
    'cambios_en_reserva': int,
    'agente': str,
    'compania': str,
    'dias_en_lista_espera': int,
    'tipo_cliente_en_reserva': str,
    'adr': float,
    'espacios_estacionamiento_requeridos': int,
    'total_solicitudes_especiales': int,
    'estado_reserva': str,
    'fecha_estado_reserva': str,
    'nombre_completo': str,
    'email': str,
    'telefono': str,
    'tipo_documento_identidad': str,
    'numero_documento_identidad': str,
    'fecha_nacimiento': str
}

OBLIGATORIAS = [
    'hotel', 'anio_llegada', 'mes_llegada', 'dia_llegada', 'nombre_completo',
    'tipo_documento_identidad', 'numero_documento_identidad'
]

UNIQUE_KEY = ['tipo_documento_identidad', 'numero_documento_identidad']

input_file = 'hotel_bookings_es.csv'
output_file = 'hotel_bookings_es_validado.csv'

print('Leyendo archivo...')
df = pd.read_csv(input_file, dtype=str)

# 1. Mapeo/verificación de columnas
print('Verificando columnas...')
missing_cols = [col for col in COLUMNS_TYPES if col not in df.columns]
if missing_cols:
    print(f'ERROR: Faltan columnas: {missing_cols}')
    exit(1)

# 2. Parseo de tipos
print('Parseando tipos de datos...')
for col, col_type in COLUMNS_TYPES.items():
    if col_type == int:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    elif col_type == float:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    else:
        df[col] = df[col].astype(str)

# 3. Validación básica
print('Validando datos...')
invalid_rows = set()

# Validar nulos en obligatorias
def check_nulls(row, idx):
    for col in OBLIGATORIAS:
        val = row[col]
        if pd.isnull(val) or str(val) == '' or str(val).lower() == 'nan':
            print(f"Fila {idx+2}: Valor nulo en columna obligatoria '{col}'")
            invalid_rows.add(idx)

# Validar formato de fecha_nacimiento
def check_fecha_nacimiento(row, idx):
    fecha = row['fecha_nacimiento']
    if fecha:
        try:
            datetime.strptime(fecha, '%Y-%m-%d')
        except Exception:
            print(f"Fila {idx+2}: Formato inválido en fecha_nacimiento: '{fecha}'")
            invalid_rows.add(idx)

# Validar unicidad de documento
doc_set = set()
def check_unicidad(row, idx):
    key = (row['tipo_documento_identidad'], row['numero_documento_identidad'])
    if key in doc_set:
        print(f"Fila {idx+2}: Duplicado de documento: {key}")
        invalid_rows.add(idx)
    else:
        doc_set.add(key)

for idx, row in df.iterrows():
    check_nulls(row, idx)
    check_fecha_nacimiento(row, idx)
    check_unicidad(row, idx)

# 4. Guardar solo filas válidas
print(f'Filas inválidas detectadas: {len(invalid_rows)}')
df_valid = df.drop(list(invalid_rows))
df_valid.to_csv(output_file, index=False)
print(f'Archivo validado guardado como {output_file} ({len(df_valid)} filas válidas)')