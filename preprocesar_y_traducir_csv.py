import pandas as pd
import requests
import random
from datetime import datetime
from faker import Faker

# Mapeo completo de columnas inglés -> español
COLUMN_MAP = {
    'hotel': 'hotel',
    'is_canceled': 'fue_cancelada',
    'lead_time': 'tiempo_anticipacion_reserva_dias',
    'arrival_date_year': 'anio_llegada',
    'arrival_date_month': 'mes_llegada',
    'arrival_date_week_number': 'semana_llegada',
    'arrival_date_day_of_month': 'dia_llegada',
    'stays_in_weekend_nights': 'noches_fin_semana',
    'stays_in_week_nights': 'noches_semana',
    'adults': 'adultos',
    'children': 'ninos',
    'babies': 'bebes',
    'meal': 'regimen_alimenticio',
    'country': 'pais_origen_cliente',
    'market_segment': 'segmento_mercado',
    'distribution_channel': 'canal_reserva',
    'is_repeated_guest': 'es_huesped_recurrente_historico',
    'previous_cancellations': 'total_cancelaciones_previas_cliente',
    'previous_bookings_not_canceled': 'total_reservas_previas_no_canceladas_cliente',
    'reserved_room_type': 'tipo_habitacion_reservada',
    'assigned_room_type': 'tipo_habitacion_asignada',
    'booking_changes': 'cambios_en_reserva',
    'agent': 'agente',
    'company': 'compania',
    'days_in_waiting_list': 'dias_en_lista_espera',
    'customer_type': 'tipo_cliente_en_reserva',
    'adr': 'adr',
    'required_car_parking_spaces': 'espacios_estacionamiento_requeridos',
    'total_of_special_requests': 'total_solicitudes_especiales',
    'reservation_status': 'estado_reserva',
    'reservation_status_date': 'fecha_estado_reserva',
    # Las siguientes ya se agregan en el script:
    # 'nombre_completo': 'nombre_completo',
    # 'email': 'email',
    # 'telefono': 'telefono',
    # 'tipo_documento_identidad': 'tipo_documento_identidad',
    # 'numero_documento_identidad': 'numero_documento_identidad',
    # 'fecha_nacimiento': 'fecha_nacimiento',
}

# Mapeos manuales SOLO para mes_llegada y estado_reserva
MESES_MAP = {
    'January': 'Enero', 'February': 'Febrero', 'March': 'Marzo', 'April': 'Abril',
    'May': 'Mayo', 'June': 'Junio', 'July': 'Julio', 'August': 'Agosto',
    'September': 'Septiembre', 'October': 'Octubre', 'November': 'Noviembre', 'December': 'Diciembre'
}
ESTADO_RESERVA_MAP = {
    'Check-Out': 'Salida',
    'Check-In': 'Entrada',
    'Canceled': 'Cancelada'
}

# Columnas de texto/categóricas a traducir
CATEGORICAL_COLS = [
    'segmento_mercado', 'canal_reserva', 'tipo_habitacion_reservada', 'tipo_habitacion_asignada',
    'tipo_cliente_en_reserva'
]

TIPOS_DOC = ['DNI', 'CE', 'Pasaporte']

fake = Faker('es_ES')

# Función para generar número de documento
def generar_numero_documento(tipo):
    if tipo == 'DNI':
        return str(random.randint(10000000, 99999999))
    elif tipo == 'CE':
        return str(random.randint(100000000, 999999999))
    elif tipo == 'Pasaporte':
        return f"P{random.randint(1000000, 9999999)}"
    return '00000000'

# Función para obtener datos ficticios usando Faker
def obtener_datos_ficticios():
    nombre = fake.name()
    email = fake.email()
    telefono = fake.phone_number()
    fecha_nacimiento = fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d')
    return nombre, email, telefono, fecha_nacimiento

# Traducir valores únicos de una columna usando LibreTranslate
def traducir_columna_libretranslate(df, col):
    valores = df[col].dropna().unique()
    traducciones = {}
    print(f"Traduciendo columna '{col}' ({len(valores)} valores únicos)...")
    for i, val in enumerate(valores, 1):
        if isinstance(val, str) and val.strip():
            try:
                response = requests.post(
                    'https://libretranslate.com/translate',
                    data={
                        'q': val,
                        'source': 'en',
                        'target': 'es',
                        'format': 'text'
                    },
                    timeout=10
                )
                if response.status_code == 200:
                    traduccion = response.json()['translatedText']
                else:
                    traduccion = val
            except Exception:
                traduccion = val
            traducciones[val] = traduccion
        else:
            traducciones[val] = val
        if i % 10 == 0 or i == len(valores):
            print(f"  - {i}/{len(valores)} traducidos...")
    df[col] = df[col].map(traducciones)
    return df

def main():
    print("Leyendo archivo CSV original...")
    df = pd.read_csv('hotel_bookings_reduced.csv')

    print("Renombrando columnas...")
    columns_renamed = []
    for k, v in COLUMN_MAP.items():
        if k in df.columns:
            df.rename(columns={k: v}, inplace=True)
            columns_renamed.append(f"{k} → {v}")
    if columns_renamed:
        print("Columnas renombradas:")
        for c in columns_renamed:
            print(f"  - {c}")
    else:
        print("No se renombró ninguna columna.")

    # Aplicar mapeos manuales SOLO para mes_llegada y estado_reserva
    if 'mes_llegada' in df.columns:
        df['mes_llegada'] = df['mes_llegada'].map(MESES_MAP).fillna(df['mes_llegada'])
    if 'estado_reserva' in df.columns:
        df['estado_reserva'] = df['estado_reserva'].map(ESTADO_RESERVA_MAP).fillna(df['estado_reserva'])

    print("\n--- Traducción de columnas categóricas ---")
    for col in CATEGORICAL_COLS:
        if col in df.columns:
            df = traducir_columna_libretranslate(df, col)
    print("Traducción completada.\n")

    print("Generando datos ficticios para clientes...")
    nombres, emails, telefonos, fechas_nac = [], [], [], []
    tipos_doc, numeros_doc = [], []
    total = len(df)
    for idx in range(total):
        nombre, email, telefono, fecha_nac = obtener_datos_ficticios()
        # 20% de los clientes sin email
        if random.random() < 0.2:
            email = ''
        tipo_doc = random.choice(TIPOS_DOC)
        numero_doc = generar_numero_documento(tipo_doc)
        nombres.append(nombre)
        emails.append(email)
        telefonos.append(telefono)
        fechas_nac.append(fecha_nac)
        tipos_doc.append(tipo_doc)
        numeros_doc.append(numero_doc)
        if (idx+1) % 500 == 0 or (idx+1) == total:
            print(f"  - {idx+1}/{total} filas procesadas...")

    df['nombre_completo'] = nombres
    df['email'] = emails
    df['telefono'] = telefonos
    df['tipo_documento_identidad'] = tipos_doc
    df['numero_documento_identidad'] = numeros_doc
    df['fecha_nacimiento'] = fechas_nac

    print("\nGuardando nuevo archivo CSV: hotel_bookings_es.csv ...")
    df.to_csv('hotel_bookings_es.csv', index=False)
    print('¡Archivo hotel_bookings_es.csv generado correctamente!')

if __name__ == '__main__':
    main() 