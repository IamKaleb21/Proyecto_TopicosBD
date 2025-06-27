import os
from datetime import datetime
import subprocess

# Crear directorio de backups si no existe
backup_dir = "mongodb_backups"
if not os.path.exists(backup_dir):
    os.makedirs(backup_dir)

# Generar nombre del backup con fecha y hora
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
backup_name = f"costadelinka_backup_{timestamp}"

# Ruta completa del backup
backup_path = os.path.join(backup_dir, backup_name)

# Comando mongodump
command = [
    "mongodump",
    "--db", "CostaDelInkaDB",
    "--out", backup_path
]

try:
    print(f"Iniciando backup de la base de datos...")
    print(f"El backup se guardará en: {backup_path}")
    
    # Ejecutar el comando
    result = subprocess.run(command, capture_output=True, text=True)
    
    if result.returncode == 0:
        print("Backup completado exitosamente!")
        print(f"Ubicación del backup: {backup_path}")
    else:
        print("Error al realizar el backup:")
        print(result.stderr)
        
except Exception as e:
    print(f"Error al ejecutar el backup: {e}") 