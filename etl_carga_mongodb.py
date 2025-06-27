import pandas as pd
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson.objectid import ObjectId

# --- Conexión a MongoDB ---
try:
    client = MongoClient('mongodb://localhost:27017/')
    db = client['CostaDelInkaDB']
    print("Conexión a MongoDB exitosa.")
except Exception as e:
    print(f"Error al conectar a MongoDB: {e}")
    exit()

clientes_col = db['Clientes']
reservas_col = db['Reservas']
detalles_reserva_col = db['DetallesReserva']
tipos_habitacion_col = db['TiposHabitacion']
tipos_cliente_col = db['TiposCliente']

# --- Función para crear o obtener tipo de habitación ---
def get_or_create_tipo_habitacion(tipo_habitacion):
    tipo_habitacion_doc = tipos_habitacion_col.find_one({"nombre_tipo_habitacion": tipo_habitacion})
    if not tipo_habitacion_doc:
        tipo_habitacion_id = ObjectId()
        tipo_habitacion_data = {
            "_id": tipo_habitacion_id,
            "nombre_tipo_habitacion": tipo_habitacion,
            "capacidad_maxima_adultos": 2,
            "precio_base_noche": 100.0,
            "activo": True
        }
        tipos_habitacion_col.insert_one(tipo_habitacion_data)
        return tipo_habitacion_id
    return tipo_habitacion_doc['_id']

# --- ETL sobre hotel_bookings_es_validado.csv ---
input_file = 'hotel_bookings_es_validado.csv'
df = pd.read_csv(input_file, dtype=str)
print(f"Leídas {len(df)} filas de {input_file}")

clientes_insertados = 0
reservas_insertadas = 0
detalles_insertados = 0

for idx, row in df.iterrows():
    try:
        # --- Cliente ---
        cliente_email = row.get('email')
        # Asegurar que email sea string y no NaN/None/float
        if not isinstance(cliente_email, str) or pd.isnull(cliente_email) or str(cliente_email).lower() == 'nan':
            cliente_email = ''
        cliente_doc = clientes_col.find_one({"email": cliente_email})
        if not cliente_doc:
            tipo_doc = row.get('tipo_documento_identidad', '')
            num_doc = row.get('numero_documento_identidad', '')
            if tipo_doc and num_doc:
                cliente_doc = clientes_col.find_one({
                    "tipo_documento_identidad": tipo_doc,
                    "numero_documento_identidad": num_doc
                })
        if not cliente_doc:
            cliente_id = ObjectId()
            fecha_nac = None
            if row.get('fecha_nacimiento'):
                try:
                    fecha_nac = datetime.strptime(row['fecha_nacimiento'], '%Y-%m-%d')
                except Exception:
                    pass
            def safe_str(val):
                if not isinstance(val, str) or pd.isnull(val) or str(val).lower() == 'nan':
                    return ''
                return str(val)
            cliente_data = {
                "_id": cliente_id,
                "nombre_completo": safe_str(row.get('nombre_completo', f"Cliente Reserva {idx+1}")),
                "email": cliente_email,
                "telefono": safe_str(row.get('telefono', '')),
                "tipo_documento_identidad": safe_str(row.get('tipo_documento_identidad', '')),
                "numero_documento_identidad": safe_str(row.get('numero_documento_identidad', '')),
                "fecha_nacimiento": fecha_nac,
                "pais_origen_cliente": safe_str(row.get('pais_origen_cliente', 'Desconocido')),
                "es_huesped_recurrente_historico": bool(int(float(row.get('es_huesped_recurrente_historico', 0)))),
                "total_cancelaciones_previas_cliente": int(float(row.get('total_cancelaciones_previas_cliente', 0))),
                "total_reservas_previas_no_canceladas_cliente": int(float(row.get('total_reservas_previas_no_canceladas_cliente', 0))),
                "historial_ids_reservas": []
            }
            clientes_col.insert_one(cliente_data)
            cliente_doc = cliente_data
            clientes_insertados += 1
        cliente_id = cliente_doc['_id']

        # --- Crear ObjectId de Reserva antes ---
        reserva_id = ObjectId()

        # --- Detalles de Reserva ---
        tipo_habitacion_id = get_or_create_tipo_habitacion(row.get('tipo_habitacion_reservada', 'C'))
        detalle_reserva_data = {
            "_id": ObjectId(),
            "reserva_id": reserva_id,  # Ya asignado
            "pais_origen_reserva": row.get('pais_origen_cliente', 'Desconocido'),
            "es_huesped_recurrente_al_reservar": bool(int(float(row.get('es_huesped_recurrente_historico', 0)))),
            "cancelaciones_previas_cliente_al_reservar": int(float(row.get('total_cancelaciones_previas_cliente', 0))),
            "reservas_previas_no_canceladas_cliente_al_reservar": int(float(row.get('total_reservas_previas_no_canceladas_cliente', 0))),
            "tipo_habitacion_reservada": row.get('tipo_habitacion_reservada'),
            "tipo_habitacion_asignada": row.get('tipo_habitacion_asignada'),
            "cambios_en_reserva": int(float(row.get('cambios_en_reserva', 0))),
            "tipo_cliente_en_reserva": row.get('tipo_cliente_en_reserva')
        }
        detalles_reserva_col.insert_one(detalle_reserva_data)
        detalles_insertados += 1
        detalle_reserva_id = detalle_reserva_data["_id"]

        # --- Reserva ---
        fecha_llegada = None
        try:
            anio = int(row.get('anio_llegada'))
            mes = row.get('mes_llegada')
            dia = int(row.get('dia_llegada'))
            meses = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre']
            mes_num = meses.index(mes) + 1 if mes in meses else 1
            fecha_llegada = datetime(anio, mes_num, dia)
        except Exception:
            pass
        noches_estadia_weekend = int(float(row.get('noches_fin_semana', 0)))
        noches_estadia_week = int(float(row.get('noches_semana', 0)))
        noches_estadia_total = max(1, noches_estadia_weekend + noches_estadia_week)
        fecha_salida = None
        if fecha_llegada:
            fecha_salida = fecha_llegada + timedelta(days=noches_estadia_total)
        reserva_data = {
            "_id": reserva_id,
            "cliente_id": cliente_id,
            "detalle_reserva_id": detalle_reserva_id,
            "fecha_creacion_reserva": datetime.now(),
            "fue_cancelada": bool(int(float(row.get('fue_cancelada', 0)))),
            "tiempo_anticipacion_reserva_dias": int(float(row.get('tiempo_anticipacion_reserva_dias', 0))),
            "fecha_llegada": fecha_llegada,
            "fecha_salida": fecha_salida,
            "noches_estadia": noches_estadia_total,
            "estado_reserva": row.get('estado_reserva'),
            "fecha_estado_reserva": None,
            "adr": float(row.get('adr', 0.0)),
            "canal_reserva": row.get('canal_reserva')
        }
        fecha_estado = row.get('fecha_estado_reserva')
        if fecha_estado:
            try:
                reserva_data["fecha_estado_reserva"] = datetime.strptime(fecha_estado, '%d/%m/%y')
            except Exception:
                try:
                    reserva_data["fecha_estado_reserva"] = datetime.strptime(fecha_estado, '%Y-%m-%d')
                except Exception:
                    pass
        reservas_col.insert_one(reserva_data)
        reservas_insertadas += 1
        # Actualizar cliente con el ID de la reserva
        clientes_col.update_one({"_id": cliente_id}, {"$push": {"historial_ids_reservas": reserva_id}})
    except Exception as e:
        print(f"Error en fila {idx+2}: {e}")

print(f"\nResumen de carga:")
print(f"Clientes insertados: {clientes_insertados}")
print(f"Reservas insertadas: {reservas_insertadas}")
print(f"Detalles de reserva insertados: {detalles_insertados}")

client.close()
print("Conexión a MongoDB cerrada.") 