#!/usr/bin/env python3
"""
Script para probar el modelo predictivo de cancelaciones
con diferentes ejemplos y ver los resultados de riesgo.
"""

import joblib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from pymongo import MongoClient

# Conexión a MongoDB para obtener valores válidos
def get_valid_values():
    """Obtener valores válidos de la base de datos para evitar warnings."""
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['CostaDelInkaDB']
        
        # Obtener valores únicos de las colecciones
        canales = list(db.Reservas.distinct("canal_reserva"))
        paises = list(db.DetallesReserva.distinct("pais_origen_reserva"))
        tipos_habitacion = list(db.DetallesReserva.distinct("tipo_habitacion_reservada"))
        tipos_cliente = list(db.DetallesReserva.distinct("tipo_cliente_en_reserva"))
        
        # Filtrar valores nulos
        canales = [c for c in canales if c]
        paises = [p for p in paises if p]
        tipos_habitacion = [t for t in tipos_habitacion if t]
        tipos_cliente = [t for t in tipos_cliente if t]
        
        client.close()
        
        return {
            'canales': canales,
            'paises': paises,
            'tipos_habitacion': tipos_habitacion,
            'tipos_cliente': tipos_cliente
        }
    except Exception as e:
        print(f"⚠️ No se pudo conectar a la base de datos: {e}")
        # Valores por defecto basados en datos típicos
        return {
            'canales': ['Direct', 'Online TA', 'Corporate', 'Offline TA/TO'],
            'paises': ['PRT', 'GBR', 'USA', 'ESP', 'IRL', 'FRA', 'ROU', 'NOR', 'OMN', 'ARG'],
            'tipos_habitacion': ['City Hotel', 'Resort Hotel'],
            'tipos_cliente': ['Transient', 'Corporate', 'Transient-Party', 'Contract']
        }

def load_model():
    """Cargar el modelo predictivo."""
    model_path = "cancellation_model_pipeline.joblib"
    if os.path.exists(model_path):
        try:
            model = joblib.load(model_path)
            print("✅ Modelo cargado exitosamente!")
            return model
        except Exception as e:
            print(f"❌ Error al cargar el modelo: {e}")
            return None
    else:
        print(f"❌ Modelo no encontrado en: {model_path}")
        return None

def predict_cancellation(model, data):
    """Hacer predicción de cancelación."""
    if model is None:
        return None, None
    
    try:
        df_input = pd.DataFrame([data])
        prediction = model.predict(df_input)[0]
        probability = model.predict_proba(df_input)[0][1]
        return prediction, probability
    except Exception as e:
        print(f"❌ Error en la predicción: {e}")
        return None, None

def get_risk_level(probability):
    """Determinar el nivel de riesgo basado en la probabilidad."""
    probability_percentage = probability * 100
    if probability_percentage > 70:
        return "🔴 ALTO", probability_percentage
    elif probability_percentage > 40:
        return "🟡 MEDIO", probability_percentage
    else:
        return "🟢 BAJO", probability_percentage

def create_test_scenario(
    tiempo_anticipacion=30,
    noches_estadia=3,
    adr=150.0,
    canal_reserva="Direct",
    pais_origen="PRT",
    es_recurrente=False,
    tipo_habitacion_reservada="City Hotel",
    tipo_habitacion_asignada="City Hotel",
    cambios_en_reserva=0,
    tipo_cliente="Transient",
    fecha_llegada=None
):
    """Crear un escenario de prueba con los parámetros dados."""
    
    if fecha_llegada is None:
        fecha_llegada = datetime.now() + timedelta(days=tiempo_anticipacion)
    
    # Preparar datos según las características del modelo
    data = {
        'tiempo_anticipacion_reserva_dias': tiempo_anticipacion,
        'noches_estadia': noches_estadia,
        'adr': adr,
        'canal_reserva': canal_reserva,
        'pais_origen_reserva': pais_origen,
        'es_huesped_recurrente_al_reservar': es_recurrente,
        'cancelaciones_previas_cliente_al_reservar': 0,
        'reservas_previas_no_canceladas_cliente_al_reservar': 0,
        'tipo_habitacion_reservada': tipo_habitacion_reservada,
        'tipo_habitacion_asignada': tipo_habitacion_asignada,
        'cambios_en_reserva': cambios_en_reserva,
        'tipo_cliente_en_reserva': tipo_cliente,
        'mes_llegada': fecha_llegada.month,
        'dia_del_anio_llegada': fecha_llegada.timetuple().tm_yday,
        'dia_de_la_semana_llegada': fecha_llegada.weekday(),
        'cambio_habitacion': 1 if tipo_habitacion_reservada != tipo_habitacion_asignada else 0,
        'ratio_cancelacion_previo': 0.0,
        'es_huesped_nuevo': 1 if not es_recurrente else 0
    }
    
    return data

def test_scenario(model, scenario_name, data):
    """Probar un escenario específico."""
    print(f"\n{'='*60}")
    print(f"🧪 PROBANDO: {scenario_name}")
    print(f"{'='*60}")
    
    # Mostrar datos de entrada más relevantes
    print("📋 Datos de entrada principales:")
    relevant_keys = [
        'tiempo_anticipacion_reserva_dias', 'noches_estadia', 'adr', 
        'canal_reserva', 'pais_origen_reserva', 'es_huesped_recurrente_al_reservar',
        'tipo_habitacion_reservada', 'tipo_habitacion_asignada', 'cambios_en_reserva',
        'tipo_cliente_en_reserva', 'cambio_habitacion'
    ]
    
    for key in relevant_keys:
        if key in data:
            print(f"  {key}: {data[key]}")
    
    # Hacer predicción
    prediction, probability = predict_cancellation(model, data)
    
    if prediction is not None and probability is not None:
        risk_level, prob_percentage = get_risk_level(probability)
        
        print(f"\n📊 RESULTADOS:")
        print(f"  Predicción: {'CANCELACIÓN' if prediction == 1 else 'NO CANCELACIÓN'}")
        print(f"  Probabilidad: {prob_percentage:.1f}%")
        print(f"  Nivel de Riesgo: {risk_level}")
        
        # Recomendaciones
        if prob_percentage > 70:
            print(f"  💡 Recomendación: Contactar cliente, ofrecer flexibilidad")
        elif prob_percentage > 40:
            print(f"  💡 Recomendación: Enviar confirmación, mantener comunicación")
        else:
            print(f"  💡 Recomendación: Proceder con confianza")
    else:
        print("❌ No se pudo hacer la predicción")

def find_scenarios_by_risk(model, valid_values, target_risk="ALTO", max_iterations=10000):
    """
    Buscar escenarios que generen un nivel de riesgo específico.
    
    Args:
        model: Modelo predictivo cargado
        valid_values: Valores válidos de la base de datos
        target_risk: "ALTO", "MEDIO", o "BAJO"
        max_iterations: Número máximo de iteraciones a probar
    
    Returns:
        Lista de escenarios encontrados
    """
    import random
    
    print(f"\n🔍 BUSCANDO ESCENARIOS CON RIESGO {target_risk}...")
    print(f"Probando hasta {max_iterations} combinaciones...")
    
    found_scenarios = []
    iterations = 0
    
    # Definir rangos de búsqueda según el riesgo objetivo
    if target_risk == "ALTO":
        target_range = (70, 100)
        anticipacion_range = (0, 10)  # Poca anticipación
        noches_range = (1, 3)  # Pocas noches
        adr_range = (50, 120)  # ADR bajo/medio
        cambios_range = (1, 5)  # Más cambios
        recurrente_prob = 0.2  # Menos probable que sea recurrente
    elif target_risk == "MEDIO":
        target_range = (40, 70)
        anticipacion_range = (5, 30)
        noches_range = (2, 7)
        adr_range = (80, 250)
        cambios_range = (0, 3)
        recurrente_prob = 0.4
    else:  # BAJO
        target_range = (0, 40)
        anticipacion_range = (15, 120)  # Mucha anticipación
        noches_range = (3, 14)  # Más noches
        adr_range = (100, 300)  # ADR medio/alto
        cambios_range = (0, 1)  # Pocos cambios
        recurrente_prob = 0.7  # Más probable que sea recurrente
    
    while iterations < max_iterations and len(found_scenarios) < 5:
        iterations += 1
        
        # Generar parámetros aleatorios dentro de los rangos
        tiempo_anticipacion = random.randint(*anticipacion_range)
        noches_estadia = random.randint(*noches_range)
        adr = random.uniform(*adr_range)
        canal_reserva = random.choice(valid_values['canales'])
        pais_origen = random.choice(valid_values['paises'])
        es_recurrente = random.random() < recurrente_prob
        tipo_habitacion_reservada = random.choice(valid_values['tipos_habitacion'])
        tipo_habitacion_asignada = random.choice(valid_values['tipos_habitacion'])
        cambios_en_reserva = random.randint(*cambios_range)
        tipo_cliente = random.choice(valid_values['tipos_cliente'])
        
        # Crear escenario
        data = create_test_scenario(
            tiempo_anticipacion=tiempo_anticipacion,
            noches_estadia=noches_estadia,
            adr=adr,
            canal_reserva=canal_reserva,
            pais_origen=pais_origen,
            es_recurrente=es_recurrente,
            tipo_habitacion_reservada=tipo_habitacion_reservada,
            tipo_habitacion_asignada=tipo_habitacion_asignada,
            cambios_en_reserva=cambios_en_reserva,
            tipo_cliente=tipo_cliente
        )
        
        # Hacer predicción
        prediction, probability = predict_cancellation(model, data)
        
        if prediction is not None and probability is not None:
            probability_percentage = probability * 100
            
            # Verificar si está en el rango objetivo
            if target_range[0] <= probability_percentage <= target_range[1]:
                scenario_info = {
                    'data': data,
                    'probability': probability_percentage,
                    'prediction': prediction,
                    'description': f"Riesgo {target_risk} - {probability_percentage:.1f}%"
                }
                found_scenarios.append(scenario_info)
                print(f"  ✅ Encontrado #{len(found_scenarios)}: {probability_percentage:.1f}% (iteración {iterations})")
        
        # Mostrar progreso cada 100 iteraciones
        if iterations % 100 == 0:
            print(f"  🔄 Iteración {iterations}/{max_iterations} - Encontrados: {len(found_scenarios)}")
    
    print(f"🎯 Búsqueda completada: {len(found_scenarios)} escenarios encontrados en {iterations} iteraciones")
    return found_scenarios

def analyze_risk_factors(model, valid_values, num_tests=500):
    """
    Analizar qué factores más influyen en el riesgo de cancelación.
    
    Args:
        model: Modelo predictivo cargado
        valid_values: Valores válidos de la base de datos
        num_tests: Número de pruebas aleatorias a realizar
    
    Returns:
        Análisis de factores de riesgo
    """
    import random
    
    print(f"\n📊 ANALIZANDO FACTORES DE RIESGO...")
    print(f"Realizando {num_tests} pruebas aleatorias...")
    
    results = []
    
    for i in range(num_tests):
        # Generar parámetros completamente aleatorios
        tiempo_anticipacion = random.randint(0, 365)
        noches_estadia = random.randint(1, 30)
        adr = random.uniform(30, 500)
        canal_reserva = random.choice(valid_values['canales'])
        pais_origen = random.choice(valid_values['paises'])
        es_recurrente = random.choice([True, False])
        tipo_habitacion_reservada = random.choice(valid_values['tipos_habitacion'])
        tipo_habitacion_asignada = random.choice(valid_values['tipos_habitacion'])
        cambios_en_reserva = random.randint(0, 10)
        tipo_cliente = random.choice(valid_values['tipos_cliente'])
        
        data = create_test_scenario(
            tiempo_anticipacion=tiempo_anticipacion,
            noches_estadia=noches_estadia,
            adr=adr,
            canal_reserva=canal_reserva,
            pais_origen=pais_origen,
            es_recurrente=es_recurrente,
            tipo_habitacion_reservada=tipo_habitacion_reservada,
            tipo_habitacion_asignada=tipo_habitacion_asignada,
            cambios_en_reserva=cambios_en_reserva,
            tipo_cliente=tipo_cliente
        )
        
        prediction, probability = predict_cancellation(model, data)
        
        if prediction is not None and probability is not None:
            probability_percentage = probability * 100
            
            results.append({
                'tiempo_anticipacion': tiempo_anticipacion,
                'noches_estadia': noches_estadia,
                'adr': adr,
                'canal_reserva': canal_reserva,
                'pais_origen': pais_origen,
                'es_recurrente': es_recurrente,
                'tipo_habitacion_reservada': tipo_habitacion_reservada,
                'tipo_habitacion_asignada': tipo_habitacion_asignada,
                'cambios_en_reserva': cambios_en_reserva,
                'tipo_cliente': tipo_cliente,
                'cambio_habitacion': 1 if tipo_habitacion_reservada != tipo_habitacion_asignada else 0,
                'probability': probability_percentage
            })
    
    # Analizar resultados
    df_results = pd.DataFrame(results)
    
    print(f"\n📈 ANÁLISIS DE RESULTADOS:")
    print(f"  Total pruebas: {len(results)}")
    print(f"  Probabilidad promedio: {df_results['probability'].mean():.1f}%")
    print(f"  Probabilidad mínima: {df_results['probability'].min():.1f}%")
    print(f"  Probabilidad máxima: {df_results['probability'].max():.1f}%")
    
    # Encontrar los casos de mayor riesgo
    high_risk = df_results[df_results['probability'] > 70]
    medium_risk = df_results[(df_results['probability'] > 40) & (df_results['probability'] <= 70)]
    low_risk = df_results[df_results['probability'] <= 40]
    
    print(f"\n🔴 ALTO RIESGO (>70%): {len(high_risk)} casos ({len(high_risk)/len(results)*100:.1f}%)")
    print(f"🟡 MEDIO RIESGO (40-70%): {len(medium_risk)} casos ({len(medium_risk)/len(results)*100:.1f}%)")
    print(f"🟢 BAJO RIESGO (<40%): {len(low_risk)} casos ({len(low_risk)/len(results)*100:.1f}%)")
    
    # Mostrar patrones de alto riesgo
    if len(high_risk) > 0:
        print(f"\n🔍 PATRONES DE ALTO RIESGO:")
        print(f"  Anticipación promedio: {high_risk['tiempo_anticipacion'].mean():.1f} días")
        print(f"  Noches promedio: {high_risk['noches_estadia'].mean():.1f}")
        print(f"  ADR promedio: ${high_risk['adr'].mean():.2f}")
        print(f"  Cambios promedio: {high_risk['cambios_en_reserva'].mean():.1f}")
        print(f"  % Recurrentes: {high_risk['es_recurrente'].mean()*100:.1f}%")
        print(f"  % Cambio habitación: {high_risk['cambio_habitacion'].mean()*100:.1f}%")
        
        # Mostrar los canales más riesgosos
        canal_risk = high_risk['canal_reserva'].value_counts()
        print(f"  Canales más riesgosos: {dict(canal_risk.head(3))}")
    
    return df_results

def main():
    """Función principal para probar diferentes escenarios."""
    print("🔮 PROBADOR DE MODELO PREDICTIVO DE CANCELACIONES")
    print("=" * 60)
    
    # Cargar modelo
    model = load_model()
    if model is None:
        return
    
    # Obtener valores válidos
    print("📊 Obteniendo valores válidos de la base de datos...")
    valid_values = get_valid_values()
    print(f"✅ Valores disponibles:")
    print(f"  Canales: {valid_values['canales']}")
    print(f"  Países: {valid_values['paises'][:5]}... (y más)")
    print(f"  Tipos habitación: {valid_values['tipos_habitacion']}")
    print(f"  Tipos cliente: {valid_values['tipos_cliente']}")
    
    # Menú de opciones
    print(f"\n{'='*60}")
    print("🎯 OPCIONES DISPONIBLES:")
    print("1. Ejecutar escenarios predefinidos")
    print("2. Buscar escenarios de ALTO riesgo")
    print("3. Buscar escenarios de MEDIO riesgo") 
    print("4. Buscar escenarios de BAJO riesgo")
    print("5. Análisis completo de factores de riesgo")
    print("6. Ejecutar todo")
    print(f"{'='*60}")
    
    try:
        choice = input("\nSelecciona una opción (1-6) o presiona Enter para ejecutar todo: ").strip()
        if not choice:
            choice = "6"
    except:
        choice = "6"
    
    if choice in ["1", "6"]:
        print(f"\n{'='*60}")
        print("🧪 EJECUTANDO ESCENARIOS PREDEFINIDOS")
        print(f"{'='*60}")
        
        # Definir escenarios de prueba con valores válidos
        scenarios = [
        {
            "name": "Escenario 1: Cliente Recurrente - Bajo Riesgo",
            "data": create_test_scenario(
                tiempo_anticipacion=45,
                noches_estadia=3,
                adr=150.0,
                canal_reserva=valid_values['canales'][0] if valid_values['canales'] else "Direct",
                pais_origen=valid_values['paises'][0] if valid_values['paises'] else "PRT",
                es_recurrente=True,
                tipo_habitacion_reservada=valid_values['tipos_habitacion'][0] if valid_values['tipos_habitacion'] else "City Hotel",
                tipo_habitacion_asignada=valid_values['tipos_habitacion'][0] if valid_values['tipos_habitacion'] else "City Hotel",
                cambios_en_reserva=0,
                tipo_cliente=valid_values['tipos_cliente'][0] if valid_values['tipos_cliente'] else "Transient"
            )
        },
        {
            "name": "Escenario 2: Reserva Última Hora - Alto Riesgo",
            "data": create_test_scenario(
                tiempo_anticipacion=1,
                noches_estadia=1,
                adr=80.0,
                canal_reserva="Online TA" if "Online TA" in valid_values['canales'] else valid_values['canales'][1] if len(valid_values['canales']) > 1 else valid_values['canales'][0],
                pais_origen=valid_values['paises'][1] if len(valid_values['paises']) > 1 else valid_values['paises'][0],
                es_recurrente=False,
                tipo_habitacion_reservada=valid_values['tipos_habitacion'][1] if len(valid_values['tipos_habitacion']) > 1 else valid_values['tipos_habitacion'][0],
                tipo_habitacion_asignada=valid_values['tipos_habitacion'][0],
                cambios_en_reserva=2,
                tipo_cliente=valid_values['tipos_cliente'][0]
            )
        },
        {
            "name": "Escenario 3: Cliente Corporativo - Bajo Riesgo",
            "data": create_test_scenario(
                tiempo_anticipacion=21,
                noches_estadia=5,
                adr=180.0,
                canal_reserva="Corporate" if "Corporate" in valid_values['canales'] else valid_values['canales'][0],
                pais_origen=valid_values['paises'][0],
                es_recurrente=True,
                tipo_habitacion_reservada=valid_values['tipos_habitacion'][0],
                tipo_habitacion_asignada=valid_values['tipos_habitacion'][0],
                cambios_en_reserva=0,
                tipo_cliente="Corporate" if "Corporate" in valid_values['tipos_cliente'] else valid_values['tipos_cliente'][0]
            )
        },
        {
            "name": "Escenario 4: Turista Internacional - Riesgo Medio",
            "data": create_test_scenario(
                tiempo_anticipacion=10,
                noches_estadia=7,
                adr=200.0,
                canal_reserva="Online TA" if "Online TA" in valid_values['canales'] else valid_values['canales'][1] if len(valid_values['canales']) > 1 else valid_values['canales'][0],
                pais_origen=valid_values['paises'][2] if len(valid_values['paises']) > 2 else valid_values['paises'][0],
                es_recurrente=False,
                tipo_habitacion_reservada=valid_values['tipos_habitacion'][0],
                tipo_habitacion_asignada=valid_values['tipos_habitacion'][0],
                cambios_en_reserva=1,
                tipo_cliente=valid_values['tipos_cliente'][0]
            )
        },
        {
            "name": "Escenario 5: Reserva Económica - Riesgo Variable",
            "data": create_test_scenario(
                tiempo_anticipacion=15,
                noches_estadia=2,
                adr=60.0,
                canal_reserva="Online TA" if "Online TA" in valid_values['canales'] else valid_values['canales'][1] if len(valid_values['canales']) > 1 else valid_values['canales'][0],
                pais_origen=valid_values['paises'][3] if len(valid_values['paises']) > 3 else valid_values['paises'][0],
                es_recurrente=False,
                tipo_habitacion_reservada=valid_values['tipos_habitacion'][0],
                tipo_habitacion_asignada=valid_values['tipos_habitacion'][0],
                cambios_en_reserva=0,
                tipo_cliente=valid_values['tipos_cliente'][0]
            )
        }
    ]
    
        # Probar cada escenario
        for scenario in scenarios:
            test_scenario(model, scenario["name"], scenario["data"])
    
    if choice in ["2", "6"]:
        # Buscar escenarios de ALTO riesgo
        high_risk_scenarios = find_scenarios_by_risk(model, valid_values, "ALTO", 10000)
        for i, scenario in enumerate(high_risk_scenarios):
            test_scenario(model, f"Alto Riesgo #{i+1}", scenario['data'])
    
    if choice in ["3", "6"]:
        # Buscar escenarios de MEDIO riesgo
        medium_risk_scenarios = find_scenarios_by_risk(model, valid_values, "MEDIO", 10000)
        for i, scenario in enumerate(medium_risk_scenarios):
            test_scenario(model, f"Medio Riesgo #{i+1}", scenario['data'])
    
    if choice in ["4", "6"]:
        # Buscar escenarios de BAJO riesgo
        low_risk_scenarios = find_scenarios_by_risk(model, valid_values, "BAJO", 500)
        for i, scenario in enumerate(low_risk_scenarios[:3]):  # Solo mostrar 3 para no saturar
            test_scenario(model, f"Bajo Riesgo #{i+1}", scenario['data'])
    
    if choice in ["5", "6"]:
        # Análisis completo de factores de riesgo
        df_analysis = analyze_risk_factors(model, valid_values, 1000)
        
        # Mostrar algunos ejemplos de los casos más extremos
        if len(df_analysis) > 0:
            print(f"\n{'='*60}")
            print("🔥 CASOS MÁS EXTREMOS ENCONTRADOS")
            print(f"{'='*60}")
            
            # Top 3 casos de mayor riesgo
            top_high_risk = df_analysis.nlargest(3, 'probability')
            for i, (_, row) in enumerate(top_high_risk.iterrows()):
                print(f"\n🔴 CASO DE MAYOR RIESGO #{i+1} - {row['probability']:.1f}%:")
                print(f"  Anticipación: {row['tiempo_anticipacion']} días")
                print(f"  Noches: {row['noches_estadia']}")
                print(f"  ADR: ${row['adr']:.2f}")
                print(f"  Canal: {row['canal_reserva']}")
                print(f"  País: {row['pais_origen']}")
                print(f"  Recurrente: {row['es_recurrente']}")
                print(f"  Cambios: {row['cambios_en_reserva']}")
                print(f"  Cambio habitación: {'Sí' if row['cambio_habitacion'] else 'No'}")
    
    print(f"\n{'='*60}")
    print("🎯 ANÁLISIS COMPLETADO")
    print("💡 Usa los patrones encontrados para entender mejor el modelo")
    print("💡 Los escenarios de alto riesgo te ayudarán a identificar reservas problemáticas")
    print(f"{'='*60}")

if __name__ == "__main__":
    main() 