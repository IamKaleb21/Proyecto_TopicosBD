import csv
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

# --- Conexión a MongoDB ---
try:
    client = MongoClient('mongodb://localhost:27017/')
    db = client['CostaDelInkaDB']
    print("Conexión a MongoDB exitosa.")
except Exception as e:
    print(f"Error al conectar a MongoDB: {e}")
    exit()

# Colecciones
clientes_col = db['Clientes']
reservas_col = db['Reservas']
detalles_reserva_col = db['DetallesReserva']
tipos_habitacion_col = db['TiposHabitacion']
tipos_cliente_col = db['TiposCliente']
modalidades_pago_col = db['ModalidadesPago']

# --- Función para parsear fechas ---
def parse_csv_date(date_str, year_str=None, month_str=None, day_str=None):
    if date_str and date_str != 'NULL':
        try:
            return datetime.strptime(date_str, '%d/%m/%y')
        except ValueError:
            try:
                return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                print(f"Advertencia: No se pudo parsear la fecha '{date_str}'")
                return None
    elif year_str and month_str and day_str:
        try:
            month_number = datetime.strptime(month_str, "%B").month
            return datetime(int(year_str), month_number, int(day_str))
        except ValueError:
            print(f"Advertencia: No se pudo construir la fecha de llegada desde {year_str}-{month_str}-{day_str}")
            return None
    return None

# --- Función para crear o obtener tipo de habitación ---
def get_or_create_tipo_habitacion(tipo_habitacion):
    tipo_habitacion_doc = tipos_habitacion_col.find_one({"nombre_tipo_habitacion": tipo_habitacion})
    if not tipo_habitacion_doc:
        tipo_habitacion_id = ObjectId()
        tipo_habitacion_data = {
            "_id": tipo_habitacion_id,
            "nombre_tipo_habitacion": tipo_habitacion,
            "capacidad_maxima_adultos": 2,  # Valor por defecto
            "precio_base_noche": 100.0,  # Valor por defecto
            "activo": True
        }
        tipos_habitacion_col.insert_one(tipo_habitacion_data)
        return tipo_habitacion_id
    return tipo_habitacion_doc['_id']

# --- Función para crear o obtener tipo de cliente ---
def get_or_create_tipo_cliente(tipo_cliente):
    tipo_cliente_doc = tipos_cliente_col.find_one({"nombre_tipo_cliente": tipo_cliente})
    if not tipo_cliente_doc:
        tipo_cliente_id = ObjectId()
        tipo_cliente_data = {
            "_id": tipo_cliente_id,
            "nombre_tipo_cliente": tipo_cliente,
            "activo": True
        }
        tipos_cliente_col.insert_one(tipo_cliente_data)
        return tipo_cliente_id
    return tipo_cliente_doc['_id']

# --- Leer y Procesar el CSV ---
csv_file_path = 'hotel_bookings.csv'

try:
    with open(csv_file_path, mode='r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        count = 0
        for row in reader:
            count += 1
            print(f"\nProcesando fila {count}")

            # 1. Crear/Obtener Cliente
            cliente_email = f"cliente{count}@example.com"
            cliente_doc = clientes_col.find_one({"email": cliente_email})
            
            if not cliente_doc:
                cliente_id = ObjectId()
                cliente_data = {
                    "_id": cliente_id,
                    "nombre_completo": f"Cliente Reserva {count}",
                    "email": cliente_email,
                    "pais_origen_cliente": row.get('country') if row.get('country') != 'NULL' else "Desconocido",
                    "es_huesped_recurrente_historico": bool(int(row.get('is_repeated_guest', 0))),
                    "total_cancelaciones_previas_cliente": int(row.get('previous_cancellations', 0)),
                    "total_reservas_previas_no_canceladas_cliente": int(row.get('previous_bookings_not_canceled', 0)),
                    "historial_ids_reservas": []
                }
                clientes_col.insert_one(cliente_data)
                print(f"Cliente insertado con ID: {cliente_id}")
            else:
                cliente_id = cliente_doc['_id']
                print(f"Cliente encontrado con ID: {cliente_id}")

            # 2. Crear Detalles de Reserva
            detalle_reserva_id = ObjectId()
            tipo_habitacion_id = get_or_create_tipo_habitacion(row.get('reserved_room_type', 'C'))
            
            detalle_reserva_data = {
                "_id": detalle_reserva_id,
                "reserva_id": None,  # Se actualizará después
                "pais_origen_reserva": row.get('country') if row.get('country') != 'NULL' else "Desconocido",
                "es_huesped_recurrente_al_reservar": bool(int(row.get('is_repeated_guest', 0))),
                "cancelaciones_previas_cliente_al_reservar": int(row.get('previous_cancellations', 0)),
                "reservas_previas_no_canceladas_cliente_al_reservar": int(row.get('previous_bookings_not_canceled', 0)),
                "tipo_habitacion_reservada": row.get('reserved_room_type'),
                "tipo_habitacion_asignada": row.get('assigned_room_type'),
                "cambios_en_reserva": int(row.get('booking_changes', 0)),
                "tipo_cliente_en_reserva": row.get('customer_type')
            }

            # 3. Crear Reserva Principal
            reserva_id = ObjectId()
            fecha_llegada = parse_csv_date(
                None, 
                row.get('arrival_date_year'), 
                row.get('arrival_date_month'), 
                row.get('arrival_date_day_of_month')
            )
            
            noches_estadia_weekend = int(row.get('stays_in_weekend_nights', 0))
            noches_estadia_week = int(row.get('stays_in_week_nights', 0))
            noches_estadia_total = max(1, noches_estadia_weekend + noches_estadia_week)  # Mínimo 1 noche

            fecha_salida = None
            if fecha_llegada:
                from datetime import timedelta
                fecha_salida = fecha_llegada + timedelta(days=noches_estadia_total)

            reserva_data = {
                "_id": reserva_id,
                "cliente_id": cliente_id,
                "detalle_reserva_id": detalle_reserva_id,
                "fecha_creacion_reserva": datetime.now(),
                "fue_cancelada": bool(int(row.get('is_canceled', 0))),
                "tiempo_anticipacion_reserva_dias": int(row.get('lead_time', 0)),
                "fecha_llegada": fecha_llegada,
                "fecha_salida": fecha_salida,
                "noches_estadia": noches_estadia_total,
                "estado_reserva": row.get('reservation_status'),
                "fecha_estado_reserva": parse_csv_date(row.get('reservation_status_date')),
                "adr": float(row.get('adr', 0.0)),
                "canal_reserva": row.get('distribution_channel')
            }

            # Actualizar detalle_reserva_data con el reserva_id
            detalle_reserva_data["reserva_id"] = reserva_id

            try:
                # Insertar en orden para manejar referencias
                detalles_reserva_col.insert_one(detalle_reserva_data)
                print(f"DetalleReserva insertado con ID: {detalle_reserva_id}")
                
                reservas_col.insert_one(reserva_data)
                print(f"Reserva insertada con ID: {reserva_id}")

                # Actualizar cliente con el ID de la reserva
                clientes_col.update_one(
                    {"_id": cliente_id},
                    {"$push": {"historial_ids_reservas": reserva_id}}
                )
                print(f"Cliente actualizado con la nueva reserva.")

            except Exception as e:
                print(f"Error al insertar documentos para la fila {count}: {e}")

            if count >= 10000:  # Limitar a 10000 inserciones
                print("\nLímite de 10000 filas procesadas.")
                break
        
        print(f"\nProceso completado. {count} filas procesadas.")

except FileNotFoundError:
    print(f"Error: El archivo {csv_file_path} no fue encontrado.")
except Exception as e:
    print(f"Ocurrió un error general: {e}")
finally:
    client.close()
    print("Conexión a MongoDB cerrada.")