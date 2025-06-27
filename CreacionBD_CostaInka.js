// Script de Creación de Base de Datos y Colecciones para CostaDelInkaDB

// 1. Seleccionar/Crear la Base de Datos
use CostaDelInkaDB;

// -----------------------------------------------------------------------------
// Colección: Clientes
// -----------------------------------------------------------------------------
db.createCollection("Clientes");
db.runCommand({
  collMod: "Clientes",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      title: "Validador de la Colección Clientes",
      required: ["nombre_completo", "email"],
      properties: {
        _id: { bsonType: "objectId" },
        nombre_completo: { bsonType: "string", description: "Debe ser un string y es requerido" },
        email: { bsonType: "string", description: "Debe ser un string, es requerido y debe ser único" },
        telefono: { bsonType: "string" },
        tipo_documento_identidad: { bsonType: "string" },
        numero_documento_identidad: { bsonType: "string", description: "Debe ser único en combinación con tipo_documento_identidad" },
        fecha_nacimiento: { bsonType: "date" },
        pais_origen_cliente: { bsonType: "string" },
        es_huesped_recurrente_historico: { bsonType: "bool" },
        total_cancelaciones_previas_cliente: { bsonType: "int", minimum: 0 },
        total_reservas_previas_no_canceladas_cliente: { bsonType: "int", minimum: 0 },
        historial_ids_reservas: {
          bsonType: "array",
          items: { bsonType: "objectId" }
        }
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
});

db.Clientes.createIndex({ email: 1 }, { unique: true });
db.Clientes.createIndex({ tipo_documento_identidad: 1, numero_documento_identidad: 1 }, { unique: true, sparse: true });

// -----------------------------------------------------------------------------
// Colección: Reservas
// -----------------------------------------------------------------------------
db.createCollection("Reservas");
db.runCommand({
  collMod: "Reservas",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      title: "Validador de la Colección Reservas",
      required: [
        "cliente_id", "detalle_reserva_id", "fecha_creacion_reserva", "fue_cancelada",
        "fecha_llegada", "fecha_salida", "noches_estadia", "estado_reserva",
        "fecha_estado_reserva", "adr"
      ],
      properties: {
        _id: { bsonType: "objectId" },
        cliente_id: { bsonType: "objectId", description: "FK a Clientes, requerido" },
        detalle_reserva_id: { bsonType: "objectId", description: "FK a DetallesReserva, requerido" },
        fecha_creacion_reserva: { bsonType: "date", description: "Requerido" },
        fue_cancelada: { bsonType: "bool", description: "Requerido" },
        tiempo_anticipacion_reserva_dias: { bsonType: "int", minimum: 0 },
        fecha_llegada: { bsonType: "date", description: "Requerido" },
        fecha_salida: { bsonType: "date", description: "Requerido" },
        noches_estadia: { bsonType: "int", minimum: 1, description: "Requerido" },
        estado_reserva: { bsonType: "string", description: "Requerido" },
        fecha_estado_reserva: { bsonType: "date", description: "Requerido" },
        adr: { bsonType: "double", minimum: 0, description: "Requerido" },
        canal_reserva: { bsonType: "string" }
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
});

db.Reservas.createIndex({ cliente_id: 1 });
db.Reservas.createIndex({ detalle_reserva_id: 1 }, { unique: true }); // Asumiendo 1 a 1
db.Reservas.createIndex({ fecha_llegada: 1 });
db.Reservas.createIndex({ estado_reserva: 1 });

// -----------------------------------------------------------------------------
// Colección: DetallesReserva
// -----------------------------------------------------------------------------
db.createCollection("DetallesReserva");
db.runCommand({
  collMod: "DetallesReserva",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      title: "Validador de la Colección DetallesReserva",
      required: ["reserva_id", "tipo_habitacion_reservada"],
      properties: {
        _id: { bsonType: "objectId" },
        reserva_id: { bsonType: "objectId", description: "FK a Reservas, requerido y único" },
        pais_origen_reserva: { bsonType: "string" },
        es_huesped_recurrente_al_reservar: { bsonType: "bool" },
        cancelaciones_previas_cliente_al_reservar: { bsonType: "int", minimum: 0 },
        reservas_previas_no_canceladas_cliente_al_reservar: { bsonType: "int", minimum: 0 },
        tipo_habitacion_reservada: { bsonType: "string", description: "Requerido" },
        tipo_habitacion_asignada: { bsonType: "string" },
        cambios_en_reserva: { bsonType: "int", minimum: 0 },
        tipo_cliente_en_reserva: { bsonType: "string" }
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
});

db.DetallesReserva.createIndex({ reserva_id: 1 }, { unique: true });
db.DetallesReserva.createIndex({ tipo_habitacion_reservada: 1 });

// -----------------------------------------------------------------------------
// Colección: Pagos
// -----------------------------------------------------------------------------
db.createCollection("Pagos");
db.runCommand({
  collMod: "Pagos",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      title: "Validador de la Colección Pagos",
      required: ["reserva_id", "monto_total", "moneda", "fecha_pago", "modalidad_pago_id", "estado_pago"],
      properties: {
        _id: { bsonType: "objectId" },
        reserva_id: { bsonType: "objectId", description: "FK a Reservas, requerido" },
        cliente_id: { bsonType: "objectId", description: "FK a Clientes" },
        monto_total: { bsonType: "double", minimum: 0, description: "Requerido" },
        moneda: { bsonType: "string", description: "Requerido" },
        fecha_pago: { bsonType: "date", description: "Requerido" },
        modalidad_pago_id: { bsonType: "objectId", description: "FK a ModalidadesPago, requerido" },
        estado_pago: { bsonType: "string", description: "Requerido" },
        tipo_documento_pago_id: { bsonType: "objectId", description: "FK a TiposDocumentoPago" },
        numero_documento_pago_emitido: { bsonType: "string" }
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
});

db.Pagos.createIndex({ reserva_id: 1 });
db.Pagos.createIndex({ cliente_id: 1 });
db.Pagos.createIndex({ modalidad_pago_id: 1 });
db.Pagos.createIndex({ fecha_pago: -1 });


// --- Colecciones de Catálogo ---

// -----------------------------------------------------------------------------
// Colección: TiposHabitacion
// -----------------------------------------------------------------------------
db.createCollection("TiposHabitacion");
db.runCommand({
  collMod: "TiposHabitacion",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      title: "Validador de la Colección TiposHabitacion",
      required: ["nombre_tipo_habitacion", "capacidad_maxima_adultos", "precio_base_noche", "activo"],
      properties: {
        _id: { bsonType: "objectId" },
        nombre_tipo_habitacion: { bsonType: "string", description: "Requerido y único" },
        codigo_interno_tipo: { bsonType: "string", description: "Único si se provee" },
        descripcion: { bsonType: "string" },
        capacidad_maxima_adultos: { bsonType: "int", minimum: 1, description: "Requerido" },
        capacidad_maxima_ninos: { bsonType: "int", minimum: 0 },
        precio_base_noche: { bsonType: "double", minimum: 0, description: "Requerido" },
        activo: { bsonType: "bool", description: "Requerido" },
        fotos_urls: {
          bsonType: "array",
          items: { bsonType: "string" }
        }
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
});

db.TiposHabitacion.createIndex({ nombre_tipo_habitacion: 1 }, { unique: true });
db.TiposHabitacion.createIndex({ codigo_interno_tipo: 1 }, { unique: true, sparse: true });

// -----------------------------------------------------------------------------
// Colección: TiposCliente
// -----------------------------------------------------------------------------
db.createCollection("TiposCliente");
db.runCommand({
  collMod: "TiposCliente",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      title: "Validador de la Colección TiposCliente",
      required: ["nombre_tipo_cliente"],
      properties: {
        _id: { bsonType: "objectId" },
        nombre_tipo_cliente: { bsonType: "string", description: "Requerido y único" },
        codigo_interno_tipo_cliente: { bsonType: "string", description: "Único si se provee" },
        descripcion: { bsonType: "string" },
        condiciones_especiales: { bsonType: "string" }
        // 'activo' no estaba en el último diagrama PlantUML para TiposCliente, si es necesario, añadirlo.
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
});

db.TiposCliente.createIndex({ nombre_tipo_cliente: 1 }, { unique: true });
db.TiposCliente.createIndex({ codigo_interno_tipo_cliente: 1 }, { unique: true, sparse: true });

// -----------------------------------------------------------------------------
// Colección: ModalidadesPago
// -----------------------------------------------------------------------------
db.createCollection("ModalidadesPago");
db.runCommand({
  collMod: "ModalidadesPago",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      title: "Validador de la Colección ModalidadesPago",
      required: ["nombre_modalidad", "activo"],
      properties: {
        _id: { bsonType: "objectId" },
        nombre_modalidad: { bsonType: "string", description: "Requerido y único" },
        descripcion: { bsonType: "string" },
        proveedor_pasarela: { bsonType: "string" },
        activo: { bsonType: "bool", description: "Requerido" }
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
});

db.ModalidadesPago.createIndex({ nombre_modalidad: 1 }, { unique: true });

// -----------------------------------------------------------------------------
// Colección: TiposDocumentoPago
// -----------------------------------------------------------------------------
db.createCollection("TiposDocumentoPago");
db.runCommand({
  collMod: "TiposDocumentoPago",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      title: "Validador de la Colección TiposDocumentoPago",
      required: ["nombre_documento", "activo"],
      properties: {
        _id: { bsonType: "objectId" },
        nombre_documento: { bsonType: "string", description: "Requerido y único" },
        requiere_datos_empresa_cliente: { bsonType: "bool" },
        activo: { bsonType: "bool", description: "Requerido" }
        // 'codigo_sunat', 'serie_predeterminada' no estaban en el último diagrama PlantUML.
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
});

db.TiposDocumentoPago.createIndex({ nombre_documento: 1 }, { unique: true });


print("Base de datos CostaDelInkaDB y colecciones creadas con validadores e índices.");