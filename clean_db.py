from pymongo import MongoClient

# --- Conexión a MongoDB ---
try:
    client = MongoClient('mongodb://localhost:27017/')
    db = client['CostaDelInkaDB']
    print("Conexión a MongoDB exitosa.")
except Exception as e:
    print(f"Error al conectar a MongoDB: {e}")
    exit()

# Lista de colecciones a limpiar
colecciones = [
    'Clientes',
    'Reservas',
    'DetallesReserva',
    'TiposHabitacion',
    'TiposCliente',
    'ModalidadesPago',
    'TiposDocumentoPago'
]

# Limpiar cada colección
for coleccion in colecciones:
    try:
        resultado = db[coleccion].delete_many({})
        print(f"Colección {coleccion}: {resultado.deleted_count} documentos eliminados.")
    except Exception as e:
        print(f"Error al limpiar la colección {coleccion}: {e}")

print("\nLimpieza de la base de datos completada.")
client.close()
print("Conexión a MongoDB cerrada.") 