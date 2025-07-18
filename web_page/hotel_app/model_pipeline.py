import pandas as pd
import numpy as np
from pymongo import MongoClient
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.pipeline import Pipeline
import lightgbm as lgb
import joblib
import warnings

warnings.filterwarnings('ignore')

# --- CONFIGURACIÓN DE LA BASE DE DATOS ---
# Reemplaza con tu cadena de conexión a MongoDB Atlas
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "CostaDelInkaDB"

def create_and_save_pipeline():
    """
    Función principal para conectar a la DB, procesar los datos,
    entrenar el pipeline final y guardarlo en un archivo.
    """
    print("--- Iniciando proceso de creación del pipeline ---")
    
    # --- 1. Conexión y Extracción de Datos ---
    print("Conectando a MongoDB y extrayendo datos...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    pipeline_agg = [
        # ... (Pega aquí tu pipeline de agregación completo) ...
        {'$lookup': {'from': 'DetallesReserva', 'localField': 'detalle_reserva_id', 'foreignField': '_id', 'as': 'detalle'}},
        {'$unwind': '$detalle'},
        {'$lookup': {'from': 'Clientes', 'localField': 'cliente_id', 'foreignField': '_id', 'as': 'cliente'}},
        {'$unwind': '$cliente'},
        {'$project': {
            'fue_cancelada': 1, 'tiempo_anticipacion_reserva_dias': 1, 'fecha_llegada': 1,
            'noches_estadia': 1, 'adr': 1, 'canal_reserva': 1,
            'pais_origen_reserva': '$detalle.pais_origen_reserva',
            'es_huesped_recurrente_al_reservar': '$detalle.es_huesped_recurrente_al_reservar',
            'cancelaciones_previas_cliente_al_reservar': '$detalle.cancelaciones_previas_cliente_al_reservar',
            'reservas_previas_no_canceladas_cliente_al_reservar': '$detalle.reservas_previas_no_canceladas_cliente_al_reservar',
            'tipo_habitacion_reservada': '$detalle.tipo_habitacion_reservada',
            'tipo_habitacion_asignada': '$detalle.tipo_habitacion_asignada',
            'cambios_en_reserva': '$detalle.cambios_en_reserva',
            'tipo_cliente_en_reserva': '$detalle.tipo_cliente_en_reserva',
            '_id': 0
        }}
    ]
    reservas_cursor = db.Reservas.aggregate(pipeline_agg)
    df = pd.DataFrame(list(reservas_cursor))
    print(f"Datos extraídos. {df.shape[0]} registros.")

    # --- 2. Limpieza y Feature Engineering ---
    print("Limpiando datos y creando nuevas características...")
    df['fecha_llegada'] = pd.to_datetime(df['fecha_llegada'])
    df['adr'] = df['adr'].abs()
    df = df[df['noches_estadia'] > 0].drop_duplicates()

    df['mes_llegada'] = df['fecha_llegada'].dt.month
    df['dia_del_anio_llegada'] = df['fecha_llegada'].dt.dayofyear
    df['dia_de_la_semana_llegada'] = df['fecha_llegada'].dt.dayofweek
    df['cambio_habitacion'] = (df['tipo_habitacion_reservada'] != df['tipo_habitacion_asignada']).astype(int)
    df['ratio_cancelacion_previo'] = df['cancelaciones_previas_cliente_al_reservar'] / (df['cancelaciones_previas_cliente_al_reservar'] + df['reservas_previas_no_canceladas_cliente_al_reservar'] + 1)
    df['es_huesped_nuevo'] = ((df['es_huesped_recurrente_al_reservar'] == False) & (df['cancelaciones_previas_cliente_al_reservar'] == 0) & (df['reservas_previas_no_canceladas_cliente_al_reservar'] == 0)).astype(int)
    
    df_model = df.drop([
        'fecha_llegada', 'tipo_habitacion_asignada', 
        'cancelaciones_previas_cliente_al_reservar', 
        'reservas_previas_no_canceladas_cliente_al_reservar'
    ], axis=1)

    # --- 3. Definición del Pipeline de Preprocesamiento y Modelo ---
    print("Definiendo el pipeline final...")
    X = df_model.drop('fue_cancelada', axis=1)
    y = df_model['fue_cancelada']

    categorical_features = X.select_dtypes(include=['object', 'category']).columns.tolist()
    numerical_features = X.select_dtypes(include=np.number).columns.tolist()

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), numerical_features),
            ('cat', OneHotEncoder(handle_unknown='ignore', drop='first'), categorical_features)
        ],
        remainder='passthrough'
    )

    final_pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('classifier', lgb.LGBMClassifier(random_state=42, class_weight='balanced'))
    ])

    # --- 4. Entrenamiento del Pipeline Final ---
    print("Entrenando el pipeline con todos los datos...")
    final_pipeline.fit(X, y)
    print("¡Entrenamiento completado!")

    # --- 5. Guardar el Pipeline ---
    model_filename = "cancellation_model_pipeline.joblib"
    joblib.dump(final_pipeline, model_filename)
    print(f"Pipeline guardado exitosamente como '{model_filename}'")


if __name__ == "__main__":
    create_and_save_pipeline()