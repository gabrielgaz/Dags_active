#!/usr/bin/env python3
"""
Script de prueba para verificar y subir DAGs de Apache Airflow

Requisitos:
- Tener configurado el directorio de DAGs de Airflow
- Tener permisos para escribir en dicho directorio
"""

import os
import shutil
import sys
import logging
from datetime import datetime

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuración (ajusta estos valores según tu entorno)
AIRFLOW_DAGS_DIR = "/opt/airflow/dags"  # Ruta típica en una instalación local
BACKUP_DIR = "/tmp/airflow_dags_backup"  # Directorio para backups

def validate_dag_file(file_path):
    """Valida que un archivo tenga la estructura básica de un DAG"""
    required_keys = ["dag_id", "schedule_interval", "start_date"]
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            
        # Verificaciones básicas
        if "DAG(" not in content:
            logger.error(f"El archivo {file_path} no contiene la definición DAG()")
            return False
            
        if "default_args=" not in content:
            logger.warning(f"El archivo {file_path} no tiene default_args definido (recomendado)")
            
        return True
        
    except Exception as e:
        logger.error(f"Error al validar {file_path}: {str(e)}")
        return False

def backup_existing_dags():
    """Crea un backup de los DAGs existentes"""
    try:
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(BACKUP_DIR, f"dags_backup_{timestamp}")
        
        if os.path.exists(AIRFLOW_DAGS_DIR):
            shutil.copytree(AIRFLOW_DAGS_DIR, backup_path)
            logger.info(f"Backup creado en: {backup_path}")
            return True
        else:
            logger.warning(f"El directorio de DAGs {AIRFLOW_DAGS_DIR} no existe")
            return False
    except Exception as e:
        logger.error(f"Error al crear backup: {str(e)}")
        return False

def deploy_dags(source_dir):
    """Implementa los DAGs desde el directorio fuente al directorio de Airflow"""
    try:
        if not os.path.exists(source_dir):
            logger.error(f"Directorio fuente {source_dir} no existe")
            return False
            
        # Crear backup primero
        if not backup_existing_dags():
            logger.error("No se pudo crear backup, abortando despliegue")
            return False
            
        # Copiar archivos .py al directorio de DAGs
        deployed_count = 0
        for filename in os.listdir(source_dir):
            if filename.endswith(".py"):
                source_path = os.path.join(source_dir, filename)
                dest_path = os.path.join(AIRFLOW_DAGS_DIR, filename)
                
                # Validar el DAG antes de copiar
                if validate_dag_file(source_path):
                    shutil.copy2(source_path, dest_path)
                    logger.info(f"DAG {filename} implementado correctamente")
                    deployed_count += 1
                else:
                    logger.error(f"DAG {filename} no válido, no se implementará")
                    
        logger.info(f"Proceso completado. {deployed_count} DAGs implementados")
        return True
        
    except Exception as e:
        logger.error(f"Error en el despliegue: {str(e)}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python deploy_dags.py <directorio_con_dags>")
        sys.exit(1)
        
    source_directory = sys.argv[1]
    logger.info(f"Iniciando despliegue de DAGs desde {source_directory}")
    
    if deploy_dags(source_directory):
        logger.info("Despliegue completado con éxito")
        sys.exit(0)
    else:
        logger.error("Hubo errores en el despliegue")
        sys.exit(1)