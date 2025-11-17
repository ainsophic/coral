#!/usr/bin/env python3
"""
CORAL - Conda Operations, Reproduction & Automation Layer (Mejorado)
Herramienta revolucionaria para gestión inteligente de entornos Conda con
capacidades avanzadas de automatización, reproducibilidad y optimización.
"""

import os
import sys
import json
import yaml
import subprocess
import argparse
import logging
import threading
import time
import hashlib
import re
import concurrent.futures
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union, Callable
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
import tempfile
import shutil
import signal
import atexit
from collections import defaultdict
import psutil

# Configuración de logging avanzado con niveles personalizados
class LogLevelFilter(logging.Filter):
    """Filtro personalizado para niveles de logging."""
    def filter(self, record):
        # Filtrar mensajes muy verbosos a menos que estemos en modo debug
        if hasattr(record, 'verbose') and record.verbose and not logger.isEnabledFor(logging.DEBUG):
            return False
        return True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('coral.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('CORAL')
logger.addFilter(LogLevelFilter())

# Clase para manejar interrupciones de forma elegante
class GracefulExit:
    """Manejador de interrupciones para salida elegante."""
    def __init__(self):
        self.kill_now = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True

@dataclass
class EnvironmentSpec:
    """Especificación completa de un entorno con metadatos extendidos."""
    name: str
    python_version: str
    packages: List[str] = field(default_factory=list)
    channels: List[str] = field(default_factory=lambda: ['conda-forge'])
    pip_packages: List[str] = field(default_factory=list)
    description: str = ""
    tags: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    last_modified: str = field(default_factory=lambda: datetime.now().isoformat())
    environment_variables: Dict[str, str] = field(default_factory=dict)
    post_activate_scripts: List[str] = field(default_factory=list)
    pre_deactivate_scripts: List[str] = field(default_factory=list)
    requirements_files: List[str] = field(default_factory=list)
    size_mb: float = 0.0
    health_score: float = 100.0  # 0-100
    
    def __post_init__(self):
        # Validar nombre de entorno
        if not re.match(r'^[a-zA-Z0-9_-]+$', self.name):
            raise ValueError(f"Nombre de entorno inválido: {self.name}")
        
        # Normalizar versión de Python
        if not re.match(r'^\d+\.\d+(\.\d+)?$', self.python_version):
            raise ValueError(f"Versión de Python inválida: {self.python_version}")
        
        # Calcular hash único para esta especificación
        self.spec_hash = self._calculate_hash()
    
    def _calculate_hash(self) -> str:
        """Calcula hash único para esta especificación."""
        content = f"{self.name}{self.python_version}{sorted(self.packages)}{sorted(self.channels)}"
        return hashlib.md5(content.encode()).hexdigest()

class CoralCore:
    """Núcleo de CORAL con capacidades revolucionarias de gestión."""
    
    def __init__(self, config_path: str = "~/.coral"):
        self.config_path = Path(config_path).expanduser()
        self.config_path.mkdir(exist_ok=True)
        self.environments_db = self.config_path / "environments.json"
        self.templates_path = self.config_path / "templates"
        self.snapshots_path = self.config_path / "snapshots"
        self.watchers_path = self.config_path / "watchers"
        self.registry_path = self.config_path / "registry"
        self.logs_path = self.config_path / "logs"
        self.cache_path = self.config_path / "cache"
        
        # Crear directorios necesarios
        for path in [self.templates_path, self.snapshots_path, 
                    self.watchers_path, self.registry_path, 
                    self.logs_path, self.cache_path]:
            path.mkdir(exist_ok=True)
        
        # Configuración avanzada
        self.config = self._load_config()
        self.load_environment_database()
        
        # Estado interno
        self.active_watchers = {}
        self.dependency_cache = {}
        self.performance_metrics = defaultdict(list)
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.config.get('max_workers', 4)
        )
        self.graceful_exit = GracefulExit()
        
        # Registrar función de limpieza al salir
        atexit.register(self._cleanup)
        
        # Inicializar watchers persistentes
        self._load_persistent_watchers()
    
    def _load_config(self) -> Dict[str, Any]:
        """Carga configuración desde archivo o usa valores predeterminados."""
        config_file = self.config_path / "config.json"
        default_config = {
            'max_workers': 4,
            'default_channels': ['conda-forge', 'defaults'],
            'cache_expiry_hours': 24,
            'snapshot_retention_days': 30,
            'watcher_check_interval': 3600,
            'performance_monitoring': True,
            'auto_cleanup': True,
            'verbose': False,
            'timeout_seconds': 300,
            'max_retries': 3,
            'retry_delay': 5
        }
        
        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    user_config = json.load(f)
                    # Combinar configuración de usuario con valores predeterminados
                    config = {**default_config, **user_config}
                    return config
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Error cargando configuración: {e}. Usando valores predeterminados.")
        
        # Guardar configuración predeterminada si no existe
        with open(config_file, 'w') as f:
            json.dump(default_config, f, indent=2)
        
        return default_config
    
    def _cleanup(self):
        """Limpieza de recursos al salir."""
        logger.info("Realizando limpieza de recursos...")
        
        # Detener todos los watchers activos
        for watcher_id in list(self.active_watchers.keys()):
            self.stop_watcher(watcher_id)
        
        # Cerrar pool de hilos
        self.thread_pool.shutdown(wait=True)
        
        # Limpiar caché temporal si está habilitado
        if self.config.get('auto_cleanup', True):
            self._cleanup_cache()
    
    def _cleanup_cache(self):
        """Limpia archivos de caché antiguos."""
        now = datetime.now()
        expiry_hours = self.config.get('cache_expiry_hours', 24)
        
        for cache_file in self.cache_path.glob("*"):
            try:
                file_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
                if now - file_time > timedelta(hours=expiry_hours):
                    cache_file.unlink()
            except Exception as e:
                logger.warning(f"Error eliminando archivo de caché {cache_file}: {e}")
    
    def load_environment_database(self):
        """Carga base de datos de entornos con metadatos."""
        if self.environments_db.exists():
            try:
                with open(self.environments_db, 'r') as f:
                    self.env_db = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.error(f"Error cargando base de datos: {e}")
                self.env_db = {}
        else:
            self.env_db = {}
    
    def save_environment_database(self):
        """Persiste base de datos de entornos."""
        try:
            with open(self.environments_db, 'w') as f:
                json.dump(self.env_db, f, indent=2)
        except IOError as e:
            logger.error(f"Error guardando base de datos: {e}")
    
    def execute_conda_command(self, command: List[str], 
                             env: Dict[str, str] = None,
                             timeout: int = None,
                             retries: int = None) -> Tuple[bool, str, str]:
        """
        Ejecuta comandos conda con manejo robusto de errores, reintentos y tiempo de espera.
        
        Args:
            command: Comando a ejecutar
            env: Variables de entorno adicionales
            timeout: Tiempo de espera en segundos
            retries: Número de reintentos
            
        Returns:
            Tupla (éxito, stdout, stderr)
        """
        if timeout is None:
            timeout = self.config.get('timeout_seconds', 300)
        if retries is None:
            retries = self.config.get('max_retries', 3)
        
        # Preparar entorno de ejecución
        exec_env = os.environ.copy()
        if env:
            exec_env.update(env)
        
        # Configurar proxy si está definido
        if 'http_proxy' in self.config:
            exec_env['http_proxy'] = self.config['http_proxy']
        if 'https_proxy' in self.config:
            exec_env['https_proxy'] = self.config['https_proxy']
        
        # Intentar ejecutar con reintentos
        for attempt in range(retries + 1):
            try:
                start_time = time.time()
                result = subprocess.run(
                    ['conda'] + command,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    env=exec_env
                )
                
                # Registrar métricas de rendimiento
                execution_time = time.time() - start_time
                self.performance_metrics[f"conda_{' '.join(command[:2])}"].append(execution_time)
                
                if result.returncode == 0:
                    return True, result.stdout, result.stderr
                else:
                    # Si no es el último intento, esperar antes de reintentar
                    if attempt < retries:
                        retry_delay = self.config.get('retry_delay', 5)
                        logger.warning(f"Comando falló (intento {attempt + 1}/{retries + 1}), reintentando en {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Comando falló después de {retries + 1} intentos: {result.stderr}")
                        return False, result.stdout, result.stderr
            
            except subprocess.TimeoutExpired:
                logger.error(f"Timeout: Comando excedió tiempo límite de {timeout}s")
                if attempt < retries:
                    logger.warning(f"Reintentando comando...")
                    continue
                return False, "", f"Timeout: Comando excedió tiempo límite de {timeout}s"
            
            except Exception as e:
                logger.error(f"Error ejecutando comando: {str(e)}")
                if attempt < retries:
                    logger.warning(f"Reintentando comando...")
                    continue
                return False, "", f"Error ejecutando comando: {str(e)}"
        
        return False, "", "Error desconocido"
    
    def smart_dependency_resolution(self, packages: List[str], 
                                   channels: List[str] = None,
                                   python_version: str = None) -> Dict[str, Any]:
        """
        FUNCIONALIDAD CRÍTICA 1: Resolución Inteligente de Dependencias
        Analiza conflictos antes de instalación y sugiere soluciones automáticas.
        """
        logger.info("Iniciando resolución inteligente de dependencias")
        
        if channels is None:
            channels = self.config.get('default_channels', ['conda-forge', 'defaults'])
        
        # Verificar caché primero
        cache_key = self._generate_cache_key(packages, channels, python_version)
        if cache_key in self.dependency_cache:
            cached_result = self.dependency_cache[cache_key]
            # Verificar si el caché aún es válido
            cache_time = datetime.fromisoformat(cached_result['timestamp'])
            if datetime.now() - cache_time < timedelta(hours=self.config.get('cache_expiry_hours', 24)):
                logger.info("Usando resultado en caché")
                return cached_result
        
        resolution_report = {
            'packages': packages,
            'channels': channels,
            'python_version': python_version,
            'conflicts': [],
            'suggestions': [],
            'optimal_solution': [],
            'estimated_size': 0,
            'resolution_time': 0,
            'timestamp': datetime.now().isoformat(),
            'dependency_graph': {},
            'alternatives': {}
        }
        
        start_time = datetime.now()
        
        # Crear entorno temporal para análisis
        temp_env = f"coral_temp_{int(datetime.now().timestamp())}"
        
        try:
            # Análisis de conflictos potenciales
            cmd = ['create', '-n', temp_env, '--dry-run', '--json']
            
            # Agregar versión de Python si se especifica
            if python_version:
                cmd.append(f'python={python_version}')
            
            # Agregar canales
            for channel in channels:
                cmd.extend(['-c', channel])
            
            # Agregar paquetes
            cmd.extend(packages)
            
            success, output, stderr = self.execute_conda_command(cmd)
            
            if success:
                try:
                    result = json.loads(output)
                    if 'actions' in result:
                        actions = result['actions']
                        
                        # Analizar paquetes a instalar
                        if 'LINK' in actions:
                            install_packages = actions['LINK']
                            resolution_report['optimal_solution'] = [
                                f"{pkg['name']}={pkg['version']}" 
                                for pkg in install_packages
                            ]
                            resolution_report['estimated_size'] = sum(
                                pkg.get('size', 0) for pkg in install_packages
                            ) / (1024 * 1024)  # MB
                            
                            # Construir grafo de dependencias
                            resolution_report['dependency_graph'] = self._build_dependency_graph(install_packages)
                        
                        # Detectar conflictos
                        if 'UNLINK' in actions:
                            unlink_packages = [pkg['name'] for pkg in actions['UNLINK']]
                            if unlink_packages:
                                resolution_report['conflicts'].append(
                                    f"Paquetes a desinstalar: {', '.join(unlink_packages)}"
                                )
                                
                                # Generar sugerencias automáticas
                                for pkg in unlink_packages:
                                    if 'python' in pkg.lower():
                                        resolution_report['suggestions'].append(
                                            "Considerar usar un entorno con Python diferente"
                                        )
                                    elif 'numpy' in pkg.lower():
                                        resolution_report['suggestions'].append(
                                            "Usar canal conda-forge para NumPy compatible"
                                        )
                                    else:
                                        resolution_report['suggestions'].append(
                                            f"Revisar compatibilidad de: {pkg}"
                                        )
                
                except json.JSONDecodeError:
                    resolution_report['conflicts'].append("Error parseando análisis de dependencias")
            
            else:
                # Analizar stderr para identificar conflictos específicos
                if 'conflict' in stderr.lower():
                    conflicts = self._parse_conflict_messages(stderr)
                    resolution_report['conflicts'].extend(conflicts)
                    
                    # Generar soluciones automáticas
                    for conflict in conflicts:
                        if 'version' in conflict.lower():
                            resolution_report['suggestions'].append(
                                "Usar versiones específicas: conda install package=version"
                            )
                        elif 'channel' in conflict.lower():
                            resolution_report['suggestions'].append(
                                "Probar canal alternativo: conda install -c conda-forge"
                            )
                        
                        # Generar alternativas
                        pkg_name = self._extract_package_name(conflict)
                        if pkg_name:
                            alternatives = self._find_alternatives(pkg_name, channels)
                            if alternatives:
                                resolution_report['alternatives'][pkg_name] = alternatives
        
        finally:
            # Limpiar entorno temporal
            self.execute_conda_command(['env', 'remove', '-n', temp_env, '-y'])
        
        resolution_report['resolution_time'] = (datetime.now() - start_time).total_seconds()
        
        # Cachear resultado para futuras consultas
        self.dependency_cache[cache_key] = resolution_report
        
        logger.info(f"Resolución completada en {resolution_report['resolution_time']:.2f}s")
        return resolution_report
    
    def _generate_cache_key(self, packages: List[str], channels: List[str], python_version: str = None) -> str:
        """Genera clave única para caché basada en parámetros."""
        content = f"{sorted(packages)}{sorted(channels)}{python_version}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def _build_dependency_graph(self, packages: List[Dict]) -> Dict[str, List[str]]:
        """Construye un grafo de dependencias a partir de la lista de paquetes."""
        graph = {}
        
        for pkg in packages:
            name = pkg['name']
            dependencies = pkg.get('depends', [])
            graph[name] = dependencies
        
        return graph
    
    def _extract_package_name(self, conflict_msg: str) -> Optional[str]:
        """Extrae nombre de paquete de mensaje de conflicto."""
        # Patrones comunes en mensajes de conflicto
        patterns = [
            r"package (\S+) has conflict",
            r"unsatisfiable requirements for package (\S+)",
            r"conflicting packages: (\S+)"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, conflict_msg, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def _find_alternatives(self, package_name: str, channels: List[str]) -> List[str]:
        """Busca alternativas para un paquete con conflictos."""
        alternatives = []
        
        # Buscar en diferentes canales
        for channel in channels:
            success, output, _ = self.execute_conda_command(
                ['search', package_name, '-c', channel, '--info', '--json']
            )
            
            if success:
                try:
                    results = json.loads(output)
                    if package_name.lower() in results:
                        versions = [pkg['version'] for pkg in results[package_name.lower()]]
                        alternatives.extend([f"{package_name}={v}" for v in versions[:3]])  # Top 3
                except (json.JSONDecodeError, KeyError):
                    pass
        
        return alternatives
    
    def _parse_conflict_messages(self, stderr: str) -> List[str]:
        """Parsea mensajes de error de conda para extraer conflictos."""
        conflicts = []
        lines = stderr.split('\n')
        
        for line in lines:
            if 'conflict' in line.lower() or 'incompatible' in line.lower() or 'unsatisfiable' in line.lower():
                conflicts.append(line.strip())
        
        return conflicts
    
    def environment_snapshot_system(self, env_name: str, 
                                   snapshot_name: str = None,
                                   include_performance_data: bool = True) -> Dict[str, Any]:
        """
        FUNCIONALIDAD CRÍTICA 2: Sistema de Snapshots Automáticos
        Captura estados completos de entornos con metadatos de rendimiento.
        """
        logger.info(f"Creando snapshot del entorno: {env_name}")
        
        if snapshot_name is None:
            snapshot_name = f"{env_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        snapshot_data = {
            'name': snapshot_name,
            'environment': env_name,
            'timestamp': datetime.now().isoformat(),
            'conda_version': None,
            'python_version': None,
            'packages': [],
            'environment_yaml': None,
            'pip_freeze': None,
            'system_info': {},
            'performance_metrics': {},
            'size_mb': 0,
            'restore_command': None,
            'checksum': None,
            'health_score': 0
        }
        
        try:
            # Obtener información del entorno
            success, env_info, _ = self.execute_conda_command(['info', '-e'])
            if success:
                # Extraer ruta del entorno
                for line in env_info.split('\n'):
                    if env_name in line:
                        env_path = line.split()[-1]
                        snapshot_data['environment_path'] = env_path
                        break
            
            # Capturar lista completa de paquetes
            success, packages_json, _ = self.execute_conda_command(
                ['list', '-n', env_name, '--json']
            )
            
            if success:
                packages = json.loads(packages_json)
                snapshot_data['packages'] = packages
                
                # Calcular tamaño total
                total_size = sum(pkg.get('size', 0) for pkg in packages)
                snapshot_data['size_mb'] = total_size / (1024 * 1024)
                
                # Identificar versión de Python
                python_pkg = next((p for p in packages if p['name'] == 'python'), None)
                if python_pkg:
                    snapshot_data['python_version'] = python_pkg['version']
                
                # Calcular checksum de paquetes para detección de cambios
                package_info = ''.join([f"{p['name']}={p['version']}" for p in packages])
                snapshot_data['checksum'] = hashlib.md5(package_info.encode()).hexdigest()
            
            # Exportar environment.yml
            success, env_yaml, _ = self.execute_conda_command(
                ['env', 'export', '-n', env_name, '--no-builds']
            )
            
            if success:
                snapshot_data['environment_yaml'] = env_yaml
            
            # Capturar pip freeze
            success, pip_output, _ = self.execute_conda_command(
                ['run', '-n', env_name, 'pip', 'freeze']
            )
            
            if success:
                snapshot_data['pip_freeze'] = pip_output
            
            # Información del sistema
            success, conda_info, _ = self.execute_conda_command(['info', '--json'])
            if success:
                info_data = json.loads(conda_info)
                snapshot_data['conda_version'] = info_data.get('conda_version')
                snapshot_data['system_info'] = {
                    'platform': info_data.get('platform'),
                    'python_version': info_data.get('python_version'),
                    'conda_prefix': info_data.get('conda_prefix'),
                    'envs_dirs': info_data.get('envs_dirs', []),
                    'channels': info_data.get('channels', [])
                }
            
            # Análisis de salud del entorno
            health_report = self.analyze_environment_health(env_name)
            snapshot_data['health_score'] = health_report.get('health_score', 0)
            
            # Métricas de rendimiento (si se solicita)
            if include_performance_data:
                perf_metrics = self._collect_performance_metrics(env_name)
                snapshot_data['performance_metrics'] = perf_metrics
            
            # Generar comando de restauración
            snapshot_data['restore_command'] = f"coral snapshot restore {snapshot_name}"
            
            # Guardar snapshot
            snapshot_file = self.snapshots_path / f"{snapshot_name}.json"
            with open(snapshot_file, 'w') as f:
                json.dump(snapshot_data, f, indent=2)
            
            logger.info(f"Snapshot '{snapshot_name}' creado exitosamente")
            return snapshot_data
        
        except Exception as e:
            logger.error(f"Error creando snapshot: {e}")
            return {'error': str(e)}
    
    def _collect_performance_metrics(self, env_name: str) -> Dict[str, Any]:
        """Recopila métricas de rendimiento del entorno."""
        metrics = {
            'activation_time': 0,
            'import_times': {},
            'memory_usage': 0,
            'disk_usage': 0
        }
        
        try:
            # Tiempo de activación
            start_time = time.time()
            test_cmd = ['run', '-n', env_name, 'python', '-c', 'import sys; import os; print("OK")']
            success, _, _ = self.execute_conda_command(test_cmd, timeout=30)
            
            if success:
                metrics['activation_time'] = time.time() - start_time
            
            # Tiempos de importación de paquetes comunes
            common_packages = ['numpy', 'pandas', 'scipy', 'matplotlib', 'sklearn']
            for pkg in common_packages:
                try:
                    start_time = time.time()
                    test_cmd = ['run', '-n', env_name, 'python', '-c', f'import {pkg}; print("OK")']
                    success, _, _ = self.execute_conda_command(test_cmd, timeout=30)
                    
                    if success:
                        metrics['import_times'][pkg] = time.time() - start_time
                except Exception:
                    metrics['import_times'][pkg] = None
            
            # Uso de memoria (requiere psutil en el entorno)
            try:
                mem_cmd = ['run', '-n', env_name, 'python', '-c', 
                          'import psutil; print(psutil.virtual_memory().used)']
                success, output, _ = self.execute_conda_command(mem_cmd, timeout=30)
                
                if success:
                    metrics['memory_usage'] = int(output.strip()) / (1024 * 1024)  # MB
            except Exception:
                pass
            
            # Uso de disco del entorno
            success, env_info, _ = self.execute_conda_command(['info', '-e'])
            if success:
                for line in env_info.split('\n'):
                    if env_name in line:
                        env_path = line.split()[-1]
                        if os.path.exists(env_path):
                            metrics['disk_usage'] = sum(
                                f.stat().st_size for f in Path(env_path).rglob('*') if f.is_file()
                            ) / (1024 * 1024)  # MB
                        break
        
        except Exception as e:
            logger.warning(f"Error recopilando métricas de rendimiento: {e}")
        
        return metrics
    
    def restore_from_snapshot(self, snapshot_name: str, 
                             target_env: str = None,
                             exclude_packages: List[str] = None) -> bool:
        """Restaurar entorno desde snapshot con opciones avanzadas."""
        snapshot_file = self.snapshots_path / f"{snapshot_name}.json"
        
        if not snapshot_file.exists():
            logger.error(f"Snapshot '{snapshot_name}' no encontrado")
            return False
        
        with open(snapshot_file, 'r') as f:
            snapshot_data = json.load(f)
        
        if target_env is None:
            target_env = f"{snapshot_data['environment']}_restored"
        
        # Crear entorno desde environment.yml
        if snapshot_data.get('environment_yaml'):
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
                yaml_content = snapshot_data['environment_yaml']
                # Modificar nombre del entorno
                yaml_data = yaml.safe_load(yaml_content)
                yaml_data['name'] = target_env
                
                # Excluir paquetes si se especifica
                if exclude_packages:
                    deps = yaml_data.get('dependencies', [])
                    filtered_deps = []
                    for dep in deps:
                        if isinstance(dep, str):
                            pkg_name = dep.split('=')[0]
                            if pkg_name not in exclude_packages:
                                filtered_deps.append(dep)
                        elif isinstance(dep, dict) and 'pip' in dep:
                            pip_deps = dep['pip']
                            filtered_pip = [
                                p for p in pip_deps 
                                if p.split('==')[0] not in exclude_packages
                            ]
                            if filtered_pip:
                                dep['pip'] = filtered_pip
                                filtered_deps.append(dep)
                    
                    yaml_data['dependencies'] = filtered_deps
                
                yaml.dump(yaml_data, f)
                temp_file = f.name
            
            try:
                success, stdout, stderr = self.execute_conda_command(
                    ['env', 'create', '-f', temp_file]
                )
                
                if success:
                    # Instalar paquetes pip adicionales si no estaban en el YAML
                    if snapshot_data.get('pip_freeze'):
                        pip_packages = snapshot_data['pip_freeze'].split('\n')
                        pip_packages = [p.strip() for p in pip_packages if p.strip()]
                        
                        # Filtrar paquetes ya instalados
                        if exclude_packages:
                            pip_packages = [
                                p for p in pip_packages 
                                if p.split('==')[0] not in exclude_packages
                            ]
                        
                        if pip_packages:
                            cmd = ['run', '-n', target_env, 'pip', 'install'] + pip_packages
                            self.execute_conda_command(cmd)
                    
                    # Restaurar variables de entorno si existen
                    if 'environment_variables' in snapshot_data:
                        env_vars = snapshot_data['environment_variables']
                        for var, value in env_vars.items():
                            self._set_environment_variable(target_env, var, value)
                    
                    logger.info(f"Entorno restaurado exitosamente: {target_env}")
                    return True
                else:
                    logger.error(f"Error restaurando entorno: {stderr}")
                    return False
            
            finally:
                os.unlink(temp_file)
        
        return False
    
    def _set_environment_variable(self, env_name: str, var_name: str, value: str):
        """Establece variable de entorno en el archivo activate.d del entorno."""
        try:
            # Obtener ruta del entorno
            success, env_info, _ = self.execute_conda_command(['info', '-e'])
            if not success:
                return
            
            env_path = None
            for line in env_info.split('\n'):
                if env_name in line:
                    env_path = line.split()[-1]
                    break
            
            if not env_path:
                return
            
            # Crear directorio activate.d si no existe
            activate_d = Path(env_path) / 'etc' / 'conda' / 'activate.d'
            activate_d.mkdir(parents=True, exist_ok=True)
            
            # Crear script para establecer variable
            env_file = activate_d / f"{var_name}.sh"
            with open(env_file, 'w') as f:
                f.write(f"#!/bin/bash\nexport {var_name}={value}\n")
            
            # Hacer ejecutable
            os.chmod(env_file, 0o755)
            
        except Exception as e:
            logger.warning(f"Error estableciendo variable de entorno {var_name}: {e}")
    
    def automatic_environment_watcher(self, env_name: str, 
                                     watch_config: Dict[str, Any] = None,
                                     start_immediately: bool = True) -> str:
        """
        FUNCIONALIDAD CRÍTICA 3: Monitoreo Automático y Auto-Reparación
        Vigila entornos y ejecuta acciones correctivas automáticamente.
        """
        logger.info(f"Iniciando watcher automático para: {env_name}")
        
        if watch_config is None:
            watch_config = {
                'check_interval': self.config.get('watcher_check_interval', 3600),  # 1 hora
                'auto_repair': True,
                'auto_update': False,
                'alert_threshold': 'warning',
                'backup_on_change': True,
                'notifications': True,
                'health_threshold': 80,  # Puntuación mínima de salud
                'max_repairs': 3,  # Máximo de reparaciones automáticas
                'repair_strategies': ['reinstall', 'update', 'clean']
            }
        
        watcher_id = f"watcher_{env_name}_{int(datetime.now().timestamp())}"
        
        watcher_data = {
            'id': watcher_id,
            'environment': env_name,
            'config': watch_config,
            'status': 'active',
            'created_at': datetime.now().isoformat(),
            'last_check': None,
            'alerts': [],
            'repairs_performed': [],
            'checks_performed': 0,
            'health_history': [],
            'thread_id': None
        }
        
        # Guardar configuración del watcher
        watcher_file = self.watchers_path / f"{watcher_id}.json"
        with open(watcher_file, 'w') as f:
            json.dump(watcher_data, f, indent=2)
        
        # Agregar a watchers activos
        self.active_watchers[watcher_id] = watcher_data
        
        # Iniciar hilo de monitoreo si se solicita
        if start_immediately:
            thread = threading.Thread(
                target=self._watcher_thread,
                args=(watcher_id,),
                daemon=True
            )
            thread.start()
            watcher_data['thread_id'] = thread.ident
        
        logger.info(f"Watcher {watcher_id} iniciado exitosamente")
        return watcher_id
    
    def _load_persistent_watchers(self):
        """Carga watchers persistentes desde archivos."""
        for watcher_file in self.watchers_path.glob("*.json"):
            try:
                with open(watcher_file, 'r') as f:
                    watcher_data = json.load(f)
                
                # Solo reiniciar watchers activos
                if watcher_data.get('status') == 'active':
                    watcher_id = watcher_data['id']
                    self.active_watchers[watcher_id] = watcher_data
                    
                    # Iniciar hilo de monitoreo
                    thread = threading.Thread(
                        target=self._watcher_thread,
                        args=(watcher_id,),
                        daemon=True
                    )
                    thread.start()
                    watcher_data['thread_id'] = thread.ident
                    
                    logger.info(f"Watcher persistente {watcher_id} reiniciado")
            
            except Exception as e:
                logger.warning(f"Error cargando watcher persistente {watcher_file}: {e}")
    
    def _watcher_thread(self, watcher_id: str):
        """Hilo de ejecución para watcher de entorno."""
        while watcher_id in self.active_watchers:
            # Verificar si se solicitó salida elegante
            if self.graceful_exit.kill_now:
                logger.info(f"Deteniendo watcher {watcher_id} por solicitud de salida")
                break
            
            watcher = self.active_watchers[watcher_id]
            config = watcher['config']
            check_interval = config.get('check_interval', 3600)
            
            # Ejecutar verificación
            self._execute_watcher_check(watcher_id)
            
            # Esperar hasta próxima verificación
            time.sleep(check_interval)
    
    def _execute_watcher_check(self, watcher_id: str) -> Dict[str, Any]:
        """Ejecutar verificación de watcher."""
        if watcher_id not in self.active_watchers:
            return {'error': 'Watcher no encontrado'}
        
        watcher = self.active_watchers[watcher_id]
        env_name = watcher['environment']
        config = watcher['config']
        
        check_result = {
            'timestamp': datetime.now().isoformat(),
            'status': 'healthy',
            'issues_found': [],
            'repairs_attempted': [],
            'success': True,
            'health_score': 100
        }
        
        try:
            # Verificar salud del entorno
            health_report = self.analyze_environment_health(env_name)
            health_score = health_report.get('health_score', 0)
            check_result['health_score'] = health_score
            
            # Actualizar historial de salud
            watcher['health_history'].append({
                'timestamp': check_result['timestamp'],
                'score': health_score
            })
            
            # Limitar historial a últimas 100 entradas
            if len(watcher['health_history']) > 100:
                watcher['health_history'] = watcher['health_history'][-100:]
            
            # Determinar si se requiere acción
            health_threshold = config.get('health_threshold', 80)
            requires_action = health_score < health_threshold
            
            if requires_action:
                check_result['status'] = health_report['status']
                check_result['issues_found'] = health_report['issues']
                
                # Intentar reparaciones automáticas si está habilitado
                if config.get('auto_repair', False):
                    max_repairs = config.get('max_repairs', 3)
                    repairs_done = len(watcher.get('repairs_performed', []))
                    
                    if repairs_done < max_repairs:
                        repairs = self._attempt_auto_repairs(env_name, health_report['issues'], config)
                        check_result['repairs_attempted'] = repairs
                    else:
                        check_result['issues_found'].append(
                            f"Límite de reparaciones automáticas alcanzado ({max_repairs})"
                        )
                
                # Crear backup si hay cambios y está habilitado
                if config.get('backup_on_change', False):
                    snapshot_name = f"auto_backup_{env_name}_{int(datetime.now().timestamp())}"
                    self.environment_snapshot_system(env_name, snapshot_name)
                    check_result['backup_created'] = snapshot_name
                
                # Enviar notificación si está habilitado
                if config.get('notifications', False):
                    self._send_notification(
                        f"Problemas detectados en entorno {env_name}",
                        f"Salud: {health_score}/100\nIssues: {len(check_result['issues_found'])}"
                    )
            
            # Actualizar estadísticas del watcher
            watcher['last_check'] = datetime.now().isoformat()
            watcher['checks_performed'] += 1
            
            # Agregar alerta si es necesaria
            if check_result['status'] != 'healthy':
                alert = {
                    'timestamp': datetime.now().isoformat(),
                    'severity': check_result['status'],
                    'message': f"Problemas detectados en {env_name}",
                    'issues': check_result['issues_found'],
                    'health_score': health_score
                }
                watcher['alerts'].append(alert)
            
            # Guardar estado actualizado
            watcher_file = self.watchers_path / f"{watcher_id}.json"
            with open(watcher_file, 'w') as f:
                json.dump(watcher, f, indent=2)
        
        except Exception as e:
            check_result['success'] = False
            check_result['error'] = str(e)
            logger.error(f"Error en watcher check: {e}")
        
        return check_result
    
    def _send_notification(self, title: str, message: str):
        """Envía notificación al usuario (implementación básica)."""
        try:
            # Intentar usar notificaciones del sistema si están disponibles
            if sys.platform == 'darwin':  # macOS
                subprocess.run([
                    'osascript', '-e', 
                    f'display notification "{message}" with title "{title}"'
                ], check=False)
            elif sys.platform.startswith('linux'):
                subprocess.run([
                    'notify-send', title, message
                ], check=False)
            elif sys.platform == 'win32':  # Windows
                from win10toast import ToastNotifier
                toaster = ToastNotifier()
                toaster.show_toast(title, message, duration=10)
        except Exception as e:
            logger.warning(f"No se pudo enviar notificación: {e}")
    
    def _attempt_auto_repairs(self, env_name: str, issues: List[str], 
                             config: Dict[str, Any]) -> List[Dict]:
        """Intentar reparaciones automáticas basadas en issues detectados."""
        repairs = []
        repair_strategies = config.get('repair_strategies', ['reinstall', 'update', 'clean'])
        
        for issue in issues:
            repair_attempt = {
                'issue': issue,
                'action': None,
                'success': False,
                'timestamp': datetime.now().isoformat()
            }
            
            try:
                if 'missing' in issue.lower() or 'broken' in issue.lower():
                    if 'reinstall' in repair_strategies:
                        # Intentar reinstalación forzada
                        repair_attempt['action'] = 'force_reinstall'
                        success, _, _ = self.execute_conda_command(
                            ['install', '-n', env_name, '--force-reinstall', '--all', '-y']
                        )
                        repair_attempt['success'] = success
                
                elif 'dependency' in issue.lower():
                    if 'update' in repair_strategies:
                        # Intentar resolver dependencias
                        repair_attempt['action'] = 'resolve_dependencies'
                        success, _, _ = self.execute_conda_command(
                            ['update', '-n', env_name, '--all', '-y']
                        )
                        repair_attempt['success'] = success
                
                elif 'space' in issue.lower() or 'disk' in issue.lower():
                    if 'clean' in repair_strategies:
                        # Limpiar cache
                        repair_attempt['action'] = 'clean_cache'
                        success, _, _ = self.execute_conda_command(
                            ['clean', '--all', '-y']
                        )
                        repair_attempt['success'] = success
                
                elif 'corrupted' in issue.lower():
                    # Estrategia más agresiva: reconstruir desde cero
                    repair_attempt['action'] = 'rebuild_environment'
                    success = self._rebuild_environment(env_name)
                    repair_attempt['success'] = success
            
            except Exception as e:
                repair_attempt['error'] = str(e)
            
            repairs.append(repair_attempt)
        
        return repairs
    
    def _rebuild_environment(self, env_name: str) -> bool:
        """Reconstruye un entorno desde cero usando snapshot o export."""
        try:
            # Crear snapshot antes de reconstruir
            snapshot_name = f"pre_rebuild_{env_name}_{int(datetime.now().timestamp())}"
            self.environment_snapshot_system(env_name, snapshot_name)
            
            # Exportar configuración actual
            success, env_yaml, _ = self.execute_conda_command(
                ['env', 'export', '-n', env_name, '--no-builds']
            )
            
            if not success:
                return False
            
            # Eliminar entorno actual
            self.execute_conda_command(['env', 'remove', '-n', env_name, '-y'])
            
            # Crear archivo temporal con la configuración
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
                f.write(env_yaml)
                temp_file = f.name
            
            try:
                # Recrear entorno desde archivo
                success, _, _ = self.execute_conda_command(['env', 'create', '-f', temp_file])
                return success
            finally:
                os.unlink(temp_file)
        
        except Exception as e:
            logger.error(f"Error reconstruyendo entorno: {e}")
            return False
    
    def list_active_watchers(self) -> List[Dict]:
        """Listar watchers activos."""
        watchers = []
        for watcher_file in self.watchers_path.glob("*.json"):
            try:
                with open(watcher_file, 'r') as f:
                    watcher_data = json.load(f)
                    watchers.append({
                        'id': watcher_data['id'],
                        'environment': watcher_data['environment'],
                        'status': watcher_data['status'],
                        'last_check': watcher_data.get('last_check'),
                        'checks_performed': watcher_data.get('checks_performed', 0),
                        'alerts_count': len(watcher_data.get('alerts', [])),
                        'health_score': self._calculate_average_health(watcher_data.get('health_history', []))
                    })
            except Exception as e:
                logger.warning(f"Error cargando watcher {watcher_file}: {e}")
        
        return watchers
    
    def _calculate_average_health(self, health_history: List[Dict]) -> float:
        """Calcula puntuación de salud promedio del historial."""
        if not health_history:
            return 0.0
        
        total = sum(entry.get('score', 0) for entry in health_history)
        return total / len(health_history)
    
    def stop_watcher(self, watcher_id: str) -> bool:
        """Detener watcher específico."""
        watcher_file = self.watchers_path / f"{watcher_id}.json"
        
        if not watcher_file.exists():
            return False
        
        try:
            with open(watcher_file, 'r') as f:
                watcher_data = json.load(f)
            
            watcher_data['status'] = 'stopped'
            watcher_data['stopped_at'] = datetime.now().isoformat()
            
            with open(watcher_file, 'w') as f:
                json.dump(watcher_data, f, indent=2)
            
            # Remover de watchers activos
            if watcher_id in self.active_watchers:
                del self.active_watchers[watcher_id]
            
            logger.info(f"Watcher {watcher_id} detenido")
            return True
        
        except Exception as e:
            logger.error(f"Error deteniendo watcher: {e}")
            return False
    
    def create_environment_advanced(self, spec: EnvironmentSpec, 
                                   use_smart_resolution: bool = True,
                                   progress_callback: Callable = None) -> bool:
        """Creación avanzada de entornos con resolución inteligente de dependencias."""
        logger.info(f"Creando entorno avanzado: {spec.name}")
        
        # Validación de especificación
        if not self._validate_environment_spec(spec):
            return False
        
        # Usar resolución inteligente de dependencias si está habilitada
        if use_smart_resolution:
            if progress_callback:
                progress_callback("Resolviendo dependencias...")
            
            resolution = self.smart_dependency_resolution(
                spec.packages, spec.channels, spec.python_version
            )
            
            if resolution['conflicts']:
                logger.warning(f"Conflictos detectados: {resolution['conflicts']}")
                if resolution['suggestions']:
                    logger.info(f"Sugerencias: {resolution['suggestions']}")
                
                # Usar solución optimizada si está disponible
                if resolution['optimal_solution']:
                    spec.packages = resolution['optimal_solution']
        
        # Construcción de comando optimizado
        cmd = ['create', '-n', spec.name, f'python={spec.python_version}', '-y']
        
        # Agregar canales en orden de prioridad
        for channel in spec.channels:
            cmd.extend(['-c', channel])
        
        # Agregar paquetes conda
        cmd.extend(spec.packages)
        
        if progress_callback:
            progress_callback("Creando entorno base...")
        
        success, stdout, stderr = self.execute_conda_command(cmd)
        
        if success:
            if progress_callback:
                progress_callback("Instalando paquetes adicionales...")
            
            # Instalar paquetes pip si existen
            if spec.pip_packages:
                self._install_pip_packages(spec.name, spec.pip_packages)
            
            # Configurar variables de entorno
            if spec.environment_variables:
                for var, value in spec.environment_variables.items():
                    self._set_environment_variable(spec.name, var, value)
            
            # Configurar scripts de activación/desactivación
            if spec.post_activate_scripts:
                self._setup_activation_scripts(spec.name, spec.post_activate_scripts, 'activate')
            
            if spec.pre_deactivate_scripts:
                self._setup_activation_scripts(spec.name, spec.pre_deactivate_scripts, 'deactivate')
            
            # Instalar desde archivos de requisitos si existen
            for req_file in spec.requirements_files:
                if os.path.exists(req_file):
                    self._install_from_requirements(spec.name, req_file)
            
            # Registrar en base de datos
            self.env_db[spec.name] = asdict(spec)
            self.save_environment_database()
            
            # Calcular tamaño del entorno
            size = self._calculate_environment_size(spec.name)
            self.env_db[spec.name]['size_mb'] = size
            
            # Análisis de salud inicial
            health = self.analyze_environment_health(spec.name)
            self.env_db[spec.name]['health_score'] = health.get('health_score', 0)
            
            self.save_environment_database()
            
            logger.info(f"Entorno {spec.name} creado exitosamente")
            return True
        else:
            logger.error(f"Error creando entorno {spec.name}: {stderr}")
            return False
    
    def _setup_activation_scripts(self, env_name: str, scripts: List[str], script_type: str):
        """Configura scripts que se ejecutan al activar/desactivar el entorno."""
        try:
            # Obtener ruta del entorno
            success, env_info, _ = self.execute_conda_command(['info', '-e'])
            if not success:
                return
            
            env_path = None
            for line in env_info.split('\n'):
                if env_name in line:
                    env_path = line.split()[-1]
                    break
            
            if not env_path:
                return
            
            # Crear directorio correspondiente
            if script_type == 'activate':
                script_dir = Path(env_path) / 'etc' / 'conda' / 'activate.d'
            else:  # deactivate
                script_dir = Path(env_path) / 'etc' / 'conda' / 'deactivate.d'
            
            script_dir.mkdir(parents=True, exist_ok=True)
            
            # Crear scripts
            for i, script in enumerate(scripts):
                script_file = script_dir / f"{script_type}_{i}.sh"
                with open(script_file, 'w') as f:
                    f.write(f"#!/bin/bash\n{script}\n")
                
                # Hacer ejecutable
                os.chmod(script_file, 0o755)
        
        except Exception as e:
            logger.warning(f"Error configurando scripts de {script_type}: {e}")
    
    def _install_from_requirements(self, env_name: str, req_file: str):
        """Instala paquetes desde archivo de requisitos."""
        try:
            # Determinar si es un archivo conda o pip
            with open(req_file, 'r') as f:
                content = f.read()
            
            if 'conda:' in content or 'name:' in content:
                # Archivo de entorno conda
                success, _, _ = self.execute_conda_command(
                    ['env', 'update', '-n', env_name, '-f', req_file]
                )
            else:
                # Archivo de requisitos pip
                cmd = ['run', '-n', env_name, 'pip', 'install', '-r', req_file]
                self.execute_conda_command(cmd)
        
        except Exception as e:
            logger.warning(f"Error instalando desde {req_file}: {e}")
    
    def _calculate_environment_size(self, env_name: str) -> float:
        """Calcula el tamaño en MB de un entorno."""
        try:
            # Obtener ruta del entorno
            success, env_info, _ = self.execute_conda_command(['info', '-e'])
            if not success:
                return 0.0
            
            env_path = None
            for line in env_info.split('\n'):
                if env_name in line:
                    env_path = line.split()[-1]
                    break
            
            if not env_path or not os.path.exists(env_path):
                return 0.0
            
            # Calcular tamaño
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(env_path):
                for filename in filenames:
                    file_path = os.path.join(dirpath, filename)
                    if os.path.exists(file_path):
                        total_size += os.path.getsize(file_path)
            
            return total_size / (1024 * 1024)  # Convertir a MB
        
        except Exception as e:
            logger.warning(f"Error calculando tamaño del entorno {env_name}: {e}")
            return 0.0
    
    def _validate_environment_spec(self, spec: EnvironmentSpec) -> bool:
        """Validación exhaustiva de especificaciones de entorno."""
        if not spec.name or not re.match(r'^[a-zA-Z0-9_-]+$', spec.name):
            logger.error("Nombre de entorno inválido")
            return False
        
        if not spec.python_version or not re.match(r'^\d+\.\d+(\.\d+)?$', spec.python_version):
            logger.error("Versión de Python inválida")
            return False
        
        # Verificar conflictos de nombres
        if spec.name in self.env_db:
            logger.error(f"Entorno {spec.name} ya existe")
            return False
        
        # Verificar que los canales son válidos
        for channel in spec.channels:
            if not channel or not isinstance(channel, str):
                logger.error(f"Canal inválido: {channel}")
                return False
        
        return True
    
    def _install_pip_packages(self, env_name: str, packages: List[str]) -> bool:
        """Instalación de paquetes pip en entorno específico."""
        try:
            cmd = ['run', '-n', env_name, 'pip', 'install'] + packages
            success, stdout, stderr = self.execute_conda_command(cmd)
            if not success:
                logger.warning(f"Algunos paquetes pip fallaron: {stderr}")
            return success
        except Exception as e:
            logger.error(f"Error instalando paquetes pip: {e}")
            return False
    
    def clone_environment_intelligent(self, source: str, target: str, 
                                   modifications: Dict[str, Any] = None,
                                   deep_clone: bool = False) -> bool:
        """Clonación inteligente con modificaciones automáticas."""
        logger.info(f"Clonando entorno {source} -> {target}")
        
        # Verificar que el entorno origen existe
        success, envs_output, _ = self.execute_conda_command(['env', 'list'])
        if not success or source not in envs_output:
            logger.error(f"Entorno origen {source} no encontrado")
            return False
        
        # Verificar que el entorno destino no existe
        if target in envs_output:
            logger.error(f"Entorno destino {target} ya existe")
            return False
        
        # Exportar entorno origen
        success, env_yaml, stderr = self.execute_conda_command(
            ['env', 'export', '-n', source, '--no-builds']
        )
        
        if not success:
            logger.error(f"Error exportando entorno {source}: {stderr}")
            return False
        
        # Procesar y modificar especificación
        env_spec = yaml.safe_load(env_yaml)
        env_spec['name'] = target
        
        if modifications:
            env_spec = self._apply_modifications(env_spec, modifications)
        
        # Clonación profunda si se solicita
        if deep_clone:
            # Obtener ruta del entorno origen
            success, env_info, _ = self.execute_conda_command(['info', '-e'])
            if success:
                for line in env_info.split('\n'):
                    if source in line:
                        source_path = line.split()[-1]
                        break
                
                # Copiar archivos locales del entorno
                if source_path and os.path.exists(source_path):
                    # Obtener ruta del entorno destino (se creará después)
                    target_path = os.path.join(
                        os.path.dirname(source_path), target
                    )
                    
                    # Crear directorio de destino
                    os.makedirs(target_path, exist_ok=True)
                    
                    # Copiar archivos de configuración personalizados
                    for src_dir, dst_dir in [
                        ('etc/conda/activate.d', 'etc/conda/activate.d'),
                        ('etc/conda/deactivate.d', 'etc/conda/deactivate.d'),
                        ('Lib/site-packages/custom', 'Lib/site-packages/custom')
                    ]:
                        src_full = os.path.join(source_path, src_dir)
                        dst_full = os.path.join(target_path, dst_dir)
                        
                        if os.path.exists(src_full):
                            os.makedirs(os.path.dirname(dst_full), exist_ok=True)
                            shutil.copytree(src_full, dst_full, dirs_exist_ok=True)
        
        # Crear archivo temporal
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            yaml.dump(env_spec, f)
            temp_file = f.name
        
        try:
            # Crear entorno desde archivo
            success, stdout, stderr = self.execute_conda_command(
                ['env', 'create', '-f', temp_file]
            )
            
            if success:
                # Registrar en base de datos
                if source in self.env_db:
                    cloned_spec = self.env_db[source].copy()
                    cloned_spec['name'] = target
                    cloned_spec['created_at'] = datetime.now().isoformat()
                    cloned_spec['description'] = f"Clonado de {source}"
                    self.env_db[target] = cloned_spec
                    self.save_environment_database()
                
                logger.info(f"Entorno clonado exitosamente: {target}")
                return True
            else:
                logger.error(f"Error clonando entorno: {stderr}")
                return False
        
        finally:
            os.unlink(temp_file)
    
    def _apply_modifications(self, env_spec: Dict, modifications: Dict) -> Dict:
        """Aplica modificaciones inteligentes a especificación de entorno."""
        if 'python_version' in modifications:
            # Actualizar versión de Python en dependencies
            deps = env_spec.get('dependencies', [])
            for i, dep in enumerate(deps):
                if isinstance(dep, str) and dep.startswith('python='):
                    deps[i] = f"python={modifications['python_version']}"
        
        if 'add_packages' in modifications:
            env_spec.setdefault('dependencies', []).extend(modifications['add_packages'])
        
        if 'remove_packages' in modifications:
            deps = env_spec.get('dependencies', [])
            for pkg in modifications['remove_packages']:
                deps = [d for d in deps if not d.startswith(pkg)]
            env_spec['dependencies'] = deps
        
        if 'add_pip_packages' in modifications:
            pip_deps = env_spec.get('dependencies', [])
            pip_section = None
            
            # Buscar sección pip existente
            for dep in pip_deps:
                if isinstance(dep, dict) and 'pip' in dep:
                    pip_section = dep
                    break
            
            if pip_section:
                pip_section['pip'].extend(modifications['add_pip_packages'])
            else:
                pip_deps.append({'pip': modifications['add_pip_packages']})
            
            env_spec['dependencies'] = pip_deps
        
        if 'channels' in modifications:
            env_spec['channels'] = modifications['channels']
        
        if 'prefix' in modifications:
            env_spec['prefix'] = modifications['prefix']
        
        return env_spec
    
    def analyze_environment_health(self, env_name: str) -> Dict[str, Any]:
        """Análisis exhaustivo de salud del entorno."""
        logger.info(f"Analizando salud del entorno: {env_name}")
        
        health_report = {
            'environment': env_name,
            'timestamp': datetime.now().isoformat(),
            'status': 'healthy',
            'issues': [],
            'recommendations': [],
            'package_count': 0,
            'size_mb': 0,
            'health_score': 100,  # 0-100
            'details': {}
        }
        
        try:
            # Verificar existencia
            success, envs_output, _ = self.execute_conda_command(['env', 'list'])
            if not success or env_name not in envs_output:
                health_report['status'] = 'missing'
                health_report['issues'].append('Entorno no encontrado')
                health_report['health_score'] = 0
                return health_report
            
            # Analizar paquetes
            success, packages_output, _ = self.execute_conda_command(
                ['list', '-n', env_name, '--json']
            )
            
            if success:
                try:
                    packages = json.loads(packages_output)
                    health_report['package_count'] = len(packages)
                    health_report['details']['packages'] = packages
                    
                    # Detectar paquetes problemáticos
                    problematic = []
                    outdated = []
                    security_issues = []
                    
                    for pkg in packages:
                        # Paquetes rotos
                        if 'broken' in pkg.get('build_string', '').lower():
                            problematic.append(pkg['name'])
                            health_report['health_score'] -= 10
                        
                        # Paquetes desactualizados (simplificado)
                        if pkg.get('version') and pkg['version'].count('.') == 0:
                            outdated.append(pkg['name'])
                            health_report['health_score'] -= 5
                    
                    if problematic:
                        health_report['issues'].append(f"Paquetes problemáticos: {problematic}")
                        health_report['recommendations'].append(
                            "Reinstalar paquetes problemáticos: conda install --force-reinstall"
                        )
                    
                    if outdated:
                        health_report['issues'].append(f"Paquetes potencialmente desactualizados: {outdated}")
                        health_report['recommendations'].append(
                            "Actualizar paquetes: conda update --all"
                        )
                
                except json.JSONDecodeError:
                    health_report['issues'].append('Error analizando lista de paquetes')
                    health_report['health_score'] -= 20
            
            # Verificar consistencia
            success, check_output, check_stderr = self.execute_conda_command(
                ['check', '-n', env_name]
            )
            
            if not success:
                if 'missing' in check_stderr.lower():
                    health_report['issues'].append('Dependencias faltantes detectadas')
                    health_report['recommendations'].append('Ejecutar: conda install --force-reinstall')
                    health_report['health_score'] -= 30
                
                if 'incompatible' in check_stderr.lower():
                    health_report['issues'].append('Dependencias incompatibles')
                    health_report['recommendations'].append('Revisar compatibilidad de paquetes')
                    health_report['health_score'] -= 25
            
            # Verificar uso de disco
            size = self._calculate_environment_size(env_name)
            health_report['size_mb'] = size
            
            # Penalizar entornos muy grandes (>5GB)
            if size > 5120:  # 5GB en MB
                health_report['issues'].append(f"Entorno muy grande: {size:.1f}MB")
                health_report['recommendations'].append(
                    "Considerar limpiar paquetes innecesarios: conda clean --all"
                )
                health_report['health_score'] -= 15
            
            # Verificar rendimiento básico
            perf_metrics = self._collect_performance_metrics(env_name)
            health_report['details']['performance'] = perf_metrics
            
            # Penalizar tiempos de activación lentos (>5s)
            if perf_metrics.get('activation_time', 0) > 5:
                health_report['issues'].append("Tiempo de activación lento")
                health_report['recommendations'].append(
                    "Optimizar entorno eliminando paquetes innecesarios"
                )
                health_report['health_score'] -= 10
            
            # Asegurar que la puntuación esté en el rango 0-100
            health_report['health_score'] = max(0, min(100, health_report['health_score']))
            
            # Determinar estado general
            if health_report['health_score'] >= 80:
                health_report['status'] = 'healthy'
            elif health_report['health_score'] >= 50:
                health_report['status'] = 'warning'
            else:
                health_report['status'] = 'critical'
        
        except Exception as e:
            health_report['status'] = 'error'
            health_report['issues'].append(f"Error en análisis: {str(e)}")
            health_report['health_score'] = 0
            logger.error(f"Error analizando salud del entorno: {e}")
        
        return health_report
    
    def bulk_operations(self, operation: str, environments: List[str], 
                       params: Dict = None,
                       parallel: bool = False,
                       progress_callback: Callable = None) -> List[Dict]:
        """Operaciones en lote con paralelización opcional."""
        logger.info(f"Ejecutando operación en lote: {operation} en {len(environments)} entornos")
        
        if params is None:
            params = {}
        
        results = []
        
        if parallel and len(environments) > 1:
            # Ejecución en paralelo
            futures = []
            for env_name in environments:
                future = self.thread_pool.submit(
                    self._execute_bulk_operation, operation, env_name, params
                )
                futures.append((env_name, future))
            
            # Recopilar resultados
            for env_name, future in futures:
                try:
                    result = future.result(timeout=params.get('timeout', 600))  # 10 min por defecto
                    results.append(result)
                    
                    if progress_callback:
                        progress = len(results) / len(environments)
                        progress_callback(f"Procesado {env_name} ({progress:.1%})")
                
                except Exception as e:
                    results.append({
                        'environment': env_name,
                        'operation': operation,
                        'timestamp': datetime.now().isoformat(),
                        'success': False,
                        'message': f'Error: {str(e)}'
                    })
        else:
            # Ejecución secuencial
            for i, env_name in enumerate(environments):
                result = self._execute_bulk_operation(operation, env_name, params)
                results.append(result)
                
                if progress_callback:
                    progress = (i + 1) / len(environments)
                    progress_callback(f"Procesado {env_name} ({progress:.1%})")
        
        return results
    
    def _execute_bulk_operation(self, operation: str, env_name: str, params: Dict) -> Dict:
        """Ejecuta una operación individual de bulk."""
        result = {
            'environment': env_name,
            'operation': operation,
            'timestamp': datetime.now().isoformat(),
            'success': False,
            'message': ''
        }
        
        try:
            if operation == 'update':
                success, stdout, stderr = self.execute_conda_command(
                    ['update', '-n', env_name, '--all', '-y']
                )
                result['success'] = success
                result['message'] = stderr if not success else 'Actualizado exitosamente'
            
            elif operation == 'clean':
                success, stdout, stderr = self.execute_conda_command(
                    ['clean', '-n', env_name, '--all', '-y']
                )
                result['success'] = success
                result['message'] = stderr if not success else 'Limpieza exitosa'
            
            elif operation == 'export':
                export_path = params.get('export_path', './exports')
                Path(export_path).mkdir(exist_ok=True)
                
                success, env_yaml, stderr = self.execute_conda_command(
                    ['env', 'export', '-n', env_name, '--no-builds']
                )
                
                if success:
                    with open(f"{export_path}/{env_name}.yml", 'w') as f:
                        f.write(env_yaml)
                    result['success'] = True
                    result['message'] = f'Exportado a {export_path}/{env_name}.yml'
                else:
                    result['message'] = stderr
            
            elif operation == 'health_check':
                health = self.analyze_environment_health(env_name)
                result['success'] = health['status'] != 'critical'
                result['message'] = f"Estado: {health['status']}"
                result['health_report'] = health
            
            elif operation == 'snapshot':
                snapshot_name = params.get('snapshot_name', f"bulk_{env_name}_{int(datetime.now().timestamp())}")
                snapshot = self.environment_snapshot_system(env_name, snapshot_name)
                result['success'] = 'error' not in snapshot
                result['message'] = f"Snapshot {'creado' if result['success'] else 'falló'}: {snapshot_name}"
                result['snapshot_name'] = snapshot_name
            
        except Exception as e:
            result['message'] = f'Error: {str(e)}'
        
        return result
    
    def create_template(self, name: str, spec: EnvironmentSpec) -> bool:
        """Crear plantillas reutilizables de entornos."""
        template_file = self.templates_path / f"{name}.json"
        
        template_data = {
            'name': name,
            'description': spec.description,
            'spec': asdict(spec),
            'created_at': datetime.now().isoformat()
        }
        
        try:
            with open(template_file, 'w') as f:
                json.dump(template_data, f, indent=2)
            
            logger.info(f"Plantilla '{name}' creada exitosamente")
            return True
        except IOError as e:
            logger.error(f"Error creando plantilla: {e}")
            return False
    
    def list_templates(self) -> List[Dict]:
        """Listar plantillas disponibles."""
        templates = []
        for template_file in self.templates_path.glob("*.json"):
            try:
                with open(template_file, 'r') as f:
                    template_data = json.load(f)
                    templates.append({
                        'name': template_data['name'],
                        'description': template_data.get('description', ''),
                        'created_at': template_data.get('created_at', ''),
                        'python_version': template_data.get('spec', {}).get('python_version', ''),
                        'package_count': len(template_data.get('spec', {}).get('packages', []))
                    })
            except Exception as e:
                logger.warning(f"Error cargando plantilla {template_file}: {e}")
        
        return templates
    
    def create_from_template(self, template_name: str, env_name: str, 
                           customizations: Dict = None,
                           progress_callback: Callable = None) -> bool:
        """Crear entorno desde plantilla con personalizaciones."""
        template_file = self.templates_path / f"{template_name}.json"
        
        if not template_file.exists():
            logger.error(f"Plantilla '{template_name}' no encontrada")
            return False
        
        try:
            with open(template_file, 'r') as f:
                template_data = json.load(f)
            
            spec_data = template_data['spec'].copy()
            spec_data['name'] = env_name
            
            # Aplicar personalizaciones
            if customizations:
                spec_data.update(customizations)
            
            spec = EnvironmentSpec(**spec_data)
            return self.create_environment_advanced(spec, progress_callback=progress_callback)
        
        except Exception as e:
            logger.error(f"Error creando entorno desde plantilla: {e}")
            return False
    
    def optimize_environment(self, env_name: str, strategy: str = 'balanced') -> Dict[str, Any]:
        """
        Optimiza un entorno según la estrategia especificada.
        
        Args:
            env_name: Nombre del entorno a optimizar
            strategy: Estrategia de optimización ('size', 'performance', 'balanced')
            
        Returns:
            Diccionario con resultados de la optimización
        """
        logger.info(f"Optimizando entorno {env_name} con estrategia {strategy}")
        
        optimization_result = {
            'environment': env_name,
            'strategy': strategy,
            'timestamp': datetime.now().isoformat(),
            'actions_performed': [],
            'size_before': 0,
            'size_after': 0,
            'performance_before': {},
            'performance_after': {},
            'success': False
        }
        
        try:
            # Medir estado inicial
            optimization_result['size_before'] = self._calculate_environment_size(env_name)
            optimization_result['performance_before'] = self._collect_performance_metrics(env_name)
            
            # Crear snapshot antes de optimizar
            snapshot_name = f"pre_optimize_{env_name}_{int(datetime.now().timestamp())}"
            self.environment_snapshot_system(env_name, snapshot_name)
            optimization_result['actions_performed'].append(f"Snapshot creado: {snapshot_name}")
            
            # Ejecutar optimizaciones según estrategia
            if strategy in ['size', 'balanced']:
                # Limpiar caché de conda
                success, _, _ = self.execute_conda_command(['clean', '--all', '-y'])
                if success:
                    optimization_result['actions_performed'].append("Caché de conda limpiada")
                
                # Identificar y eliminar paquetes grandes y poco utilizados
                unused_packages = self._identify_unused_packages(env_name)
                if unused_packages:
                    cmd = ['remove', '-n', env_name, '-y'] + unused_packages
                    success, _, _ = self.execute_conda_command(cmd)
                    if success:
                        optimization_result['actions_performed'].append(
                            f"Eliminados paquetes poco utilizados: {', '.join(unused_packages)}"
                        )
            
            if strategy in ['performance', 'balanced']:
                # Actualizar paquetes críticos para rendimiento
                critical_packages = ['numpy', 'scipy', 'pandas', 'python']
                cmd = ['update', '-n', env_name, '-y'] + critical_packages
                success, _, _ = self.execute_conda_command(cmd)
                if success:
                    optimization_result['actions_performed'].append(
                        f"Actualizados paquetes críticos: {', '.join(critical_packages)}"
                    )
                
                # Precompilar bytecode de Python
                success, _, _ = self.execute_conda_command(
                    ['run', '-n', env_name, 'python', '-m', 'compileall', '-q']
                )
                if success:
                    optimization_result['actions_performed'].append("Bytecode de Python precompilado")
            
            # Medir estado final
            optimization_result['size_after'] = self._calculate_environment_size(env_name)
            optimization_result['performance_after'] = self._collect_performance_metrics(env_name)
            
            # Calcular mejoras
            size_reduction = optimization_result['size_before'] - optimization_result['size_after']
            if size_reduction > 0:
                optimization_result['actions_performed'].append(
                    f"Reducción de tamaño: {size_reduction:.1f}MB"
                )
            
            # Actualizar puntuación de salud
            health = self.analyze_environment_health(env_name)
            if env_name in self.env_db:
                self.env_db[env_name]['health_score'] = health.get('health_score', 0)
                self.env_db[env_name]['last_modified'] = datetime.now().isoformat()
                self.save_environment_database()
            
            optimization_result['success'] = True
            logger.info(f"Optimización completada para {env_name}")
        
        except Exception as e:
            optimization_result['actions_performed'].append(f"Error: {str(e)}")
            logger.error(f"Error optimizando entorno {env_name}: {e}")
        
        return optimization_result
    
    def _identify_unused_packages(self, env_name: str, threshold_days: int = 30) -> List[str]:
        """Identifica paquetes que no se han utilizado recientemente."""
        # Implementación simplificada - en un caso real se analizarían archivos de log
        # o patrones de uso para determinar qué paquetes no se utilizan
        
        # Por ahora, solo identificar paquetes que suelen ser opcionales
        optional_packages = [
            'jupyter', 'ipython', 'notebook', 'spyder', 'pycharm',
            'matplotlib', 'seaborn', 'plotly', 'bokeh',
            'pytest', 'nose', 'coverage', 'black', 'flake8'
        ]
        
        # Verificar qué paquetes opcionales están instalados
        success, packages_json, _ = self.execute_conda_command(
            ['list', '-n', env_name, '--json']
        )
        
        if not success:
            return []
        
        try:
            packages = json.loads(packages_json)
            installed_packages = {pkg['name'] for pkg in packages}
            
            # Devolver paquetes opcionales que están instalados
            return [pkg for pkg in optional_packages if pkg in installed_packages]
        
        except json.JSONDecodeError:
            return []
    
    def benchmark_environment(self, env_name: str, tests: List[str] = None) -> Dict[str, Any]:
        """
        Ejecuta pruebas de benchmark en un entorno.
        
        Args:
            env_name: Nombre del entorno a probar
            tests: Lista de pruebas a ejecutar (por defecto: ['import', 'computation', 'memory'])
            
        Returns:
            Diccionario con resultados de benchmark
        """
        if tests is None:
            tests = ['import', 'computation', 'memory']
        
        logger.info(f"Ejecutando benchmark en entorno: {env_name}")
        
        benchmark_results = {
            'environment': env_name,
            'timestamp': datetime.now().isoformat(),
            'tests': {},
            'overall_score': 0,
            'comparison': {}
        }
        
        try:
            # Verificar que el entorno existe
            success, envs_output, _ = self.execute_conda_command(['env', 'list'])
            if not success or env_name not in envs_output:
                benchmark_results['error'] = f"Entorno {env_name} no encontrado"
                return benchmark_results
            
            # Ejecutar pruebas según lo solicitado
            if 'import' in tests:
                benchmark_results['tests']['import'] = self._benchmark_import_times(env_name)
            
            if 'computation' in tests:
                benchmark_results['tests']['computation'] = self._benchmark_computation(env_name)
            
            if 'memory' in tests:
                benchmark_results['tests']['memory'] = self._benchmark_memory_usage(env_name)
            
            # Calcular puntuación general (simplificado)
            scores = []
            for test_name, test_result in benchmark_results['tests'].items():
                if 'score' in test_result:
                    scores.append(test_result['score'])
            
            if scores:
                benchmark_results['overall_score'] = sum(scores) / len(scores)
            
            # Comparar con entornos similares si existen
            benchmark_results['comparison'] = self._compare_with_similar_environments(
                env_name, benchmark_results
            )
        
        except Exception as e:
            benchmark_results['error'] = str(e)
            logger.error(f"Error en benchmark: {e}")
        
        return benchmark_results
    
    def _benchmark_import_times(self, env_name: str) -> Dict[str, Any]:
        """Mide tiempos de importación de paquetes comunes."""
        result = {
            'test': 'import_times',
            'packages': {},
            'average_time': 0,
            'score': 0
        }
        
        try:
            # Paquetes comunes para probar
            packages = ['numpy', 'pandas', 'scipy', 'matplotlib', 'sklearn']
            times = []
            
            for pkg in packages:
                # Verificar si el paquete está instalado
                success, _, _ = self.execute_conda_command(
                    ['run', '-n', env_name, 'python', '-c', f'import {pkg}']
                )
                
                if success:
                    # Medir tiempo de importación
                    start_time = time.time()
                    success, _, _ = self.execute_conda_command(
                        ['run', '-n', env_name, 'python', '-c', f'import {pkg}']
                    )
                    
                    if success:
                        import_time = time.time() - start_time
                        result['packages'][pkg] = import_time
                        times.append(import_time)
            
            if times:
                result['average_time'] = sum(times) / len(times)
                # Calcular puntuación (menos tiempo es mejor, normalizado a 0-100)
                # Basado en que 1s = 0 puntos, 0.1s = 100 puntos
                result['score'] = max(0, min(100, 100 * (1 - result['average_time'])))
        
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def _benchmark_computation(self, env_name: str) -> Dict[str, Any]:
        """Mide rendimiento computacional básico."""
        result = {
            'test': 'computation',
            'operations': {},
            'score': 0
        }
        
        try:
            # Prueba de NumPy (si está disponible)
            numpy_test = """
import time
import numpy as np

# Prueba de multiplicación de matrices
start = time.time()
a = np.random.rand(1000, 1000)
b = np.random.rand(1000, 1000)
c = np.dot(a, b)
numpy_time = time.time() - start

# Prueba de operaciones vectorizadas
start = time.time()
x = np.random.rand(1000000)
y = np.sin(x) * np.cos(x) + np.sqrt(x**2 + 1)
vector_time = time.time() - start

print(f"numpy:{numpy_time},vector:{vector_time}")
"""
            
            success, output, _ = self.execute_conda_command(
                ['run', '-n', env_name, 'python', '-c', numpy_test]
            )
            
            if success:
                parts = output.strip().split(',')
                for part in parts:
                    if ':' in part:
                        test_name, test_time = part.split(':')
                        result['operations'][test_name] = float(test_time)
                
                # Calcular puntuación basada en tiempo promedio
                if result['operations']:
                    avg_time = sum(result['operations'].values()) / len(result['operations'])
                    # Normalizado: 5s = 0 puntos, 0.5s = 100 puntos
                    result['score'] = max(0, min(100, 100 * (1 - (avg_time - 0.5) / 4.5)))
        
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def _benchmark_memory_usage(self, env_name: str) -> Dict[str, Any]:
        """Mide uso de memoria del entorno."""
        result = {
            'test': 'memory_usage',
            'metrics': {},
            'score': 0
        }
        
        try:
            # Medir uso de memoria básico
            memory_test = """
import psutil
import os

# Obtener proceso actual
process = psutil.Process(os.getpid())
mem_info = process.memory_info()

# Medir uso de RSS (Resident Set Size)
rss_mb = mem_info.rss / (1024 * 1024)

# Medir uso de VMS (Virtual Memory Size)
vms_mb = mem_info.vms / (1024 * 1024)

print(f"rss:{rss_mb},vms:{vms_mb}")
"""
            
            success, output, _ = self.execute_conda_command(
                ['run', '-n', env_name, 'python', '-c', memory_test]
            )
            
            if success:
                parts = output.strip().split(',')
                for part in parts:
                    if ':' in part:
                        metric_name, metric_value = part.split(':')
                        result['metrics'][metric_name] = float(metric_value)
                
                # Calcular puntuación basada en uso de RSS
                if 'rss' in result['metrics']:
                    rss_mb = result['metrics']['rss']
                    # Normalizado: 500MB = 0 puntos, 50MB = 100 puntos
                    result['score'] = max(0, min(100, 100 * (1 - (rss_mb - 50) / 450)))
        
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def _compare_with_similar_environments(self, env_name: str, 
                                         benchmark_results: Dict) -> Dict[str, Any]:
        """Compara resultados con entornos similares."""
        comparison = {
            'similar_environments': [],
            'better_than': [],
            'worse_than': []
        }
        
        try:
            # Buscar entornos con la misma versión de Python
            if env_name in self.env_db:
                python_version = self.env_db[env_name].get('python_version', '')
                
                similar_envs = [
                    name for name, spec in self.env_db.items()
                    if name != env_name and spec.get('python_version') == python_version
                ]
                
                # En una implementación completa, se cargarían benchmarks anteriores
                # y se compararían los resultados. Por ahora, solo listar entornos similares
                comparison['similar_environments'] = similar_envs[:5]  # Top 5
        
        except Exception as e:
            comparison['error'] = str(e)
        
        return comparison
    
    def migrate_environment(self, source_env: str, target_system: str, 
                           target_path: str = None) -> Dict[str, Any]:
        """
        Migra un entorno a otro sistema.
        
        Args:
            source_env: Nombre del entorno a migrar
            target_system: Sistema de destino ('docker', 'file', 'cloud')
            target_path: Ruta de destino (según el sistema)
            
        Returns:
            Diccionario con resultados de la migración
        """
        logger.info(f"Migrando entorno {source_env} a {target_system}")
        
        migration_result = {
            'source_env': source_env,
            'target_system': target_system,
            'target_path': target_path,
            'timestamp': datetime.now().isoformat(),
            'success': False,
            'steps': [],
            'error': None
        }
        
        try:
            # Verificar que el entorno origen existe
            success, envs_output, _ = self.execute_conda_command(['env', 'list'])
            if not success or source_env not in envs_output:
                migration_result['error'] = f"Entorno origen {source_env} no encontrado"
                return migration_result
            
            # Crear snapshot del entorno
            snapshot_name = f"migrate_{source_env}_{int(datetime.now().timestamp())}"
            snapshot = self.environment_snapshot_system(source_env, snapshot_name)
            
            if 'error' in snapshot:
                migration_result['error'] = f"Error creando snapshot: {snapshot['error']}"
                return migration_result
            
            migration_result['steps'].append(f"Snapshot creado: {snapshot_name}")
            
            # Exportar environment.yml
            success, env_yaml, _ = self.execute_conda_command(
                ['env', 'export', '-n', source_env, '--no-builds']
            )
            
            if not success:
                migration_result['error'] = "Error exportando entorno"
                return migration_result
            
            migration_result['steps'].append("Entorno exportado a YAML")
            
            # Procesar según sistema de destino
            if target_system == 'docker':
                migration_result = self._migrate_to_docker(
                    source_env, env_yaml, snapshot, target_path, migration_result
                )
            elif target_system == 'file':
                migration_result = self._migrate_to_file(
                    source_env, env_yaml, snapshot, target_path, migration_result
                )
            elif target_system == 'cloud':
                migration_result = self._migrate_to_cloud(
                    source_env, env_yaml, snapshot, target_path, migration_result
                )
            else:
                migration_result['error'] = f"Sistema de destino no soportado: {target_system}"
                return migration_result
            
            migration_result['success'] = True
            logger.info(f"Migración completada exitosamente")
        
        except Exception as e:
            migration_result['error'] = str(e)
            logger.error(f"Error en migración: {e}")
        
        return migration_result
    
    def _migrate_to_docker(self, source_env: str, env_yaml: str, 
                          snapshot: Dict, target_path: str, 
                          migration_result: Dict) -> Dict[str, Any]:
        """Migra entorno a Docker."""
        try:
            # Crear Dockerfile
            dockerfile_content = f"""
FROM continuumio/miniconda3

# Copiar environment.yml
COPY environment.yml /tmp/

# Crear entorno
RUN conda env create -f /tmp/environment.yml

# Activar entorno por defecto
ENV PATH /opt/conda/envs/{source_env}/bin:$PATH

# Establecer directorio de trabajo
WORKDIR /workspace

# Comando por defecto
CMD ["bash"]
"""
            
            # Guardar Dockerfile
            if target_path is None:
                target_path = f"./docker_{source_env}"
            
            os.makedirs(target_path, exist_ok=True)
            
            with open(os.path.join(target_path, "Dockerfile"), 'w') as f:
                f.write(dockerfile_content)
            
            with open(os.path.join(target_path, "environment.yml"), 'w') as f:
                f.write(env_yaml)
            
            # Guardar snapshot para referencia
            with open(os.path.join(target_path, "snapshot.json"), 'w') as f:
                json.dump(snapshot, f, indent=2)
            
            # Crear script de construcción
            build_script = f"""#!/bin/bash
docker build -t {source_env}:latest .
"""
            
            with open(os.path.join(target_path, "build.sh"), 'w') as f:
                f.write(build_script)
            
            os.chmod(os.path.join(target_path, "build.sh"), 0o755)
            
            migration_result['steps'].append(f"Dockerfile creado en {target_path}")
            migration_result['steps'].append("Ejecute 'bash build.sh' para construir la imagen")
            
        except Exception as e:
            migration_result['error'] = str(e)
        
        return migration_result
    
    def _migrate_to_file(self, source_env: str, env_yaml: str, 
                        snapshot: Dict, target_path: str, 
                        migration_result: Dict) -> Dict[str, Any]:
        """Migra entorno a archivos."""
        try:
            if target_path is None:
                target_path = f"./{source_env}_migration"
            
            os.makedirs(target_path, exist_ok=True)
            
            # Guardar environment.yml
            with open(os.path.join(target_path, "environment.yml"), 'w') as f:
                f.write(env_yaml)
            
            # Guardar snapshot
            with open(os.path.join(target_path, "snapshot.json"), 'w') as f:
                json.dump(snapshot, f, indent=2)
            
            # Crear script de restauración
            restore_script = f"""#!/bin/bash
# Script para restaurar el entorno {source_env}

# Crear entorno desde YAML
conda env create -f environment.yml

# Activar entorno
conda activate {source_env}

echo "Entorno {source_env} restaurado exitosamente"
"""
            
            with open(os.path.join(target_path, "restore.sh"), 'w') as f:
                f.write(restore_script)
            
            os.chmod(os.path.join(target_path, "restore.sh"), 0o755)
            
            migration_result['steps'].append(f"Archivos de migración creados en {target_path}")
            migration_result['steps'].append("Ejecute 'bash restore.sh' para restaurar el entorno")
            
        except Exception as e:
            migration_result['error'] = str(e)
        
        return migration_result
    
    def _migrate_to_cloud(self, source_env: str, env_yaml: str, 
                         snapshot: Dict, target_path: str, 
                         migration_result: Dict) -> Dict[str, Any]:
        """Migra entorno a la nube (implementación básica)."""
        try:
            if target_path is None:
                target_path = f"coral_migrations/{source_env}"
            
            # En una implementación completa, aquí se subiría a un servicio de almacenamiento
            # como AWS S3, Google Cloud Storage, etc.
            
            # Por ahora, solo preparar archivos para subida manual
            temp_dir = f"./temp_cloud_{source_env}"
            os.makedirs(temp_dir, exist_ok=True)
            
            # Guardar environment.yml
            with open(os.path.join(temp_dir, "environment.yml"), 'w') as f:
                f.write(env_yaml)
            
            # Guardar snapshot
            with open(os.path.join(temp_dir, "snapshot.json"), 'w') as f:
                json.dump(snapshot, f, indent=2)
            
            # Crear script de restauración para la nube
            restore_script = f"""#!/bin/bash
# Script para restaurar el entorno {source_env} desde la nube

# Descargar archivos (reemplazar con comando real de descarga)
# wget {target_path}/environment.yml
# wget {target_path}/snapshot.json

# Crear entorno desde YAML
conda env create -f environment.yml

# Activar entorno
conda activate {source_env}

echo "Entorno {source_env} restaurado exitosamente desde la nube"
"""
            
            with open(os.path.join(temp_dir, "restore_from_cloud.sh"), 'w') as f:
                f.write(restore_script)
            
            os.chmod(os.path.join(temp_dir, "restore_from_cloud.sh"), 0o755)
            
            migration_result['steps'].append(f"Archivos preparados en {temp_dir}")
            migration_result['steps'].append(f"Suba el contenido de {temp_dir} a {target_path}")
            
        except Exception as e:
            migration_result['error'] = str(e)
        
        return migration_result
    
    def schedule_operation(self, operation: str, schedule: str, 
                          environments: List[str] = None,
                          params: Dict = None) -> str:
        """
        Programa una operación para ejecución periódica.
        
        Args:
            operation: Operación a programar ('update', 'snapshot', 'health_check')
            schedule: Programación en formato cron ('0 2 * * *' para 2 AM diariamente)
            environments: Lista de entornos (si no se especifica, se aplican a todos)
            params: Parámetros adicionales para la operación
            
        Returns:
            ID de la tarea programada
        """
        logger.info(f"Programando operación {operation} con schedule {schedule}")
        
        task_id = f"task_{operation}_{int(datetime.now().timestamp())}"
        
        task_data = {
            'id': task_id,
            'operation': operation,
            'schedule': schedule,
            'environments': environments or list(self.env_db.keys()),
            'params': params or {},
            'created_at': datetime.now().isoformat(),
            'last_run': None,
            'next_run': self._calculate_next_run(schedule),
            'active': True,
            'run_count': 0,
            'results': []
        }
        
        # Guardar tarea programada
        task_file = self.config_path / "tasks" / f"{task_id}.json"
        task_file.parent.mkdir(exist_ok=True)
        
        with open(task_file, 'w') as f:
            json.dump(task_data, f, indent=2)
        
        logger.info(f"Tarea programada creada: {task_id}")
        return task_id
    
    def _calculate_next_run(self, schedule: str) -> str:
        """Calcula próxima ejecución basada en schedule (implementación simplificada)."""
        # En una implementación completa, se usaría una librería como croniter
        # Por ahora, solo devuelve un timestamp 24 horas en el futuro
        next_run = datetime.now() + timedelta(days=1)
        return next_run.isoformat()
    
    def list_scheduled_tasks(self) -> List[Dict]:
        """Lista tareas programadas."""
        tasks = []
        tasks_dir = self.config_path / "tasks"
        
        if not tasks_dir.exists():
            return tasks
        
        for task_file in tasks_dir.glob("*.json"):
            try:
                with open(task_file, 'r') as f:
                    task_data = json.load(f)
                    tasks.append({
                        'id': task_data['id'],
                        'operation': task_data['operation'],
                        'schedule': task_data['schedule'],
                        'environments_count': len(task_data.get('environments', [])),
                        'last_run': task_data.get('last_run'),
                        'next_run': task_data.get('next_run'),
                        'active': task_data.get('active', False),
                        'run_count': task_data.get('run_count', 0)
                    })
            except Exception as e:
                logger.warning(f"Error cargando tarea {task_file}: {e}")
        
        return tasks
    
    def cancel_scheduled_task(self, task_id: str) -> bool:
        """Cancela una tarea programada."""
        task_file = self.config_path / "tasks" / f"{task_id}.json"
        
        if not task_file.exists():
            return False
        
        try:
            with open(task_file, 'r') as f:
                task_data = json.load(f)
            
            task_data['active'] = False
            task_data['cancelled_at'] = datetime.now().isoformat()
            
            with open(task_file, 'w') as f:
                json.dump(task_data, f, indent=2)
            
            logger.info(f"Tarea {task_id} cancelada")
            return True
        
        except Exception as e:
            logger.error(f"Error cancelando tarea: {e}")
            return False

class CoralCLI:
    """Interfaz de línea de comandos revolucionaria para CORAL."""
    
    def __init__(self):
        self.core = CoralCore()
        self.parser = self._create_parser()
    
    def _create_parser(self) -> argparse.ArgumentParser:
        """Crear parser de argumentos con subcomandos revolucionarios."""
        parser = argparse.ArgumentParser(
            description='CORAL - Conda Operations, Reproduction & Automation Layer (Mejorado)',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Ejemplos de uso:
  coral create mi_entorno --python 3.9 --packages numpy pandas
  coral clone mi_entorno mi_entorno_v2 --modify-python 3.10
  coral resolve numpy pandas --channels conda-forge
  coral snapshot create mi_entorno --name backup_2023
  coral watch start mi_entorno --interval 3600 --auto-repair
  coral bulk update mi_entorno1 mi_entorno2 mi_entorno3 --parallel
  coral optimize mi_entorno --strategy size
  coral benchmark mi_entorno --tests import computation memory
  coral migrate mi_entorno docker --target-path ./docker_image
            """
        )
        
        # Argumentos globales
        parser.add_argument('--verbose', '-v', action='store_true', help='Modo verboso')
        parser.add_argument('--config', help='Ruta a archivo de configuración')
        
        subparsers = parser.add_subparsers(dest='command', help='Comandos disponibles')
        
        # Crear entorno avanzado
        create_parser = subparsers.add_parser('create', help='Crear entorno avanzado')
        create_parser.add_argument('name', help='Nombre del entorno')
        create_parser.add_argument('--python', default='3.9', help='Versión de Python')
        create_parser.add_argument('--packages', nargs='*', default=[], help='Paquetes conda')
        create_parser.add_argument('--pip-packages', nargs='*', default=[], help='Paquetes pip')
        create_parser.add_argument('--channels', nargs='*', default=['conda-forge'], help='Canales')
        create_parser.add_argument('--description', default='', help='Descripción')
        create_parser.add_argument('--tags', nargs='*', default=[], help='Tags')
        create_parser.add_argument('--requirements', nargs='*', default=[], help='Archivos de requisitos')
        create_parser.add_argument('--env-vars', nargs='*', default=[], help='Variables de entorno (VAR=VAL)')
        create_parser.add_argument('--no-smart-resolve', action='store_true', help='Desactivar resolución inteligente')
        
        # Clonar entorno
        clone_parser = subparsers.add_parser('clone', help='Clonar entorno inteligente')
        clone_parser.add_argument('source', help='Entorno origen')
        clone_parser.add_argument('target', help='Entorno destino')
        clone_parser.add_argument('--modify-python', help='Nueva versión de Python')
        clone_parser.add_argument('--add-packages', nargs='*', help='Paquetes adicionales')
        clone_parser.add_argument('--remove-packages', nargs='*', help='Paquetes a remover')
        clone_parser.add_argument('--add-pip-packages', nargs='*', help='Paquetes pip adicionales')
        clone_parser.add_argument('--deep', action='store_true', help='Clonación profunda')
        
        # Análisis de salud
        health_parser = subparsers.add_parser('health', help='Analizar salud de entorno')
        health_parser.add_argument('environment', help='Nombre del entorno')
        health_parser.add_argument('--detailed', action='store_true', help='Análisis detallado')
        
        # Operaciones en lote
        bulk_parser = subparsers.add_parser('bulk', help='Operaciones en lote')
        bulk_parser.add_argument('operation', choices=['update', 'clean', 'export', 'health_check', 'snapshot'])
        bulk_parser.add_argument('environments', nargs='+', help='Lista de entornos')
        bulk_parser.add_argument('--export-path', default='./exports', help='Ruta de exportación')
        bulk_parser.add_argument('--parallel', action='store_true', help='Ejecución en paralelo')
        bulk_parser.add_argument('--timeout', type=int, default=600, help='Tiempo de espera por entorno (segundos)')
        
        # Gestión de plantillas
        template_parser = subparsers.add_parser('template', help='Gestión de plantillas')
        template_subparsers = template_parser.add_subparsers(dest='template_action')
        
        # Crear plantilla
        create_template = template_subparsers.add_parser('create', help='Crear plantilla')
        create_template.add_argument('name', help='Nombre de la plantilla')
        create_template.add_argument('--from-env', help='Crear desde entorno existente')
        
        # Listar plantillas
        template_subparsers.add_parser('list', help='Listar plantillas')
        
        # Usar plantilla
        use_template = template_subparsers.add_parser('use', help='Usar plantilla')
        use_template.add_argument('template', help='Nombre de la plantilla')
        use_template.add_argument('environment', help='Nombre del nuevo entorno')
        use_template.add_argument('--modify-python', help='Versión de Python')
        use_template.add_argument('--add-packages', nargs='*', help='Paquetes adicionales')
        
        # Resolución inteligente de dependencias
        resolve_parser = subparsers.add_parser('resolve', help='Analizar dependencias')
        resolve_parser.add_argument('packages', nargs='+', help='Paquetes a analizar')
        resolve_parser.add_argument('--channels', nargs='*', default=['conda-forge'], help='Canales')
        resolve_parser.add_argument('--python', help='Versión de Python')
        resolve_parser.add_argument('--format', choices=['json', 'table'], default='table', help='Formato de salida')
        
        # Sistema de snapshots
        snapshot_parser = subparsers.add_parser('snapshot', help='Gestión de snapshots')
        snapshot_subparsers = snapshot_parser.add_subparsers(dest='snapshot_action')
        
        # Crear snapshot
        create_snapshot = snapshot_subparsers.add_parser('create', help='Crear snapshot')
        create_snapshot.add_argument('environment', help='Nombre del entorno')
        create_snapshot.add_argument('--name', help='Nombre del snapshot')
        create_snapshot.add_argument('--no-perf', action='store_true', help='No incluir métricas de rendimiento')
        
        # Listar snapshots
        snapshot_subparsers.add_parser('list', help='Listar snapshots')
        
        # Restaurar snapshot
        restore_snapshot = snapshot_subparsers.add_parser('restore', help='Restaurar snapshot')
        restore_snapshot.add_argument('snapshot', help='Nombre del snapshot')
        restore_snapshot.add_argument('--target', help='Nombre del entorno destino')
        restore_snapshot.add_argument('--exclude', nargs='*', help='Paquetes a excluir')
        
        # Sistema de watchers
        watch_parser = subparsers.add_parser('watch', help='Monitoreo automático')
        watch_subparsers = watch_parser.add_subparsers(dest='watch_action')
        
        # Iniciar watcher
        start_watch = watch_subparsers.add_parser('start', help='Iniciar watcher')
        start_watch.add_argument('environment', help='Entorno a monitorear')
        start_watch.add_argument('--interval', type=int, default=3600, help='Intervalo en segundos')
        start_watch.add_argument('--auto-repair', action='store_true', help='Auto-reparación')
        start_watch.add_argument('--auto-update', action='store_true', help='Auto-actualización')
        start_watch.add_argument('--health-threshold', type=int, default=80, help='Umbral de salud')
        
        # Listar watchers
        watch_subparsers.add_parser('list', help='Listar watchers activos')
        
        # Detener watcher
        stop_watch = watch_subparsers.add_parser('stop', help='Detener watcher')
        stop_watch.add_argument('watcher_id', help='ID del watcher')
        
        # Estado de watcher
        status_watch = watch_subparsers.add_parser('status', help='Estado de watcher')
        status_watch.add_argument('watcher_id', help='ID del watcher')
        
        # Optimización de entornos
        optimize_parser = subparsers.add_parser('optimize', help='Optimizar entorno')
        optimize_parser.add_argument('environment', help='Nombre del entorno')
        optimize_parser.add_argument('--strategy', choices=['size', 'performance', 'balanced'], 
                                    default='balanced', help='Estrategia de optimización')
        
        # Benchmark de entornos
        benchmark_parser = subparsers.add_parser('benchmark', help='Ejecutar benchmark')
        benchmark_parser.add_argument('environment', help='Nombre del entorno')
        benchmark_parser.add_argument('--tests', nargs='*', choices=['import', 'computation', 'memory'],
                                     default=['import', 'computation', 'memory'], help='Pruebas a ejecutar')
        
        # Migración de entornos
        migrate_parser = subparsers.add_parser('migrate', help='Migrar entorno')
        migrate_parser.add_argument('environment', help='Nombre del entorno')
        migrate_parser.add_argument('target_system', choices=['docker', 'file', 'cloud'], 
                                  help='Sistema de destino')
        migrate_parser.add_argument('--target-path', help='Ruta de destino')
        
        # Tareas programadas
        schedule_parser = subparsers.add_parser('schedule', help='Programar operación')
        schedule_parser.add_argument('operation', choices=['update', 'snapshot', 'health_check'],
                                   help='Operación a programar')
        schedule_parser.add_argument('schedule', help='Programación (formato cron)')
        schedule_parser.add_argument('--environments', nargs='*', help='Entornos (todos si no se especifica)')
        
        # Listar tareas programadas
        schedule_subparsers = subparsers.add_parser('schedule', help='Gestión de tareas programadas')
        schedule_subparsers_list = schedule_subparsers.add_subparsers(dest='schedule_action')
        schedule_subparsers_list.add_parser('list', help='Listar tareas programadas')
        
        # Cancelar tarea programada
        cancel_schedule = schedule_subparsers_list.add_parser('cancel', help='Cancelar tarea programada')
        cancel_schedule.add_argument('task_id', help='ID de la tarea')
        
        return parser
    
    def run(self, args=None):
        """Ejecutar interfaz de línea de comandos."""
        args = self.parser.parse_args(args)
        
        if not args.command:
            self.parser.print_help()
            return
        
        # Configurar modo verboso si se solicita
        if args.verbose:
            logger.setLevel(logging.DEBUG)
        
# Cargar configuración personalizada si se especifica
if hasattr(args, 'config') and args.config:
    self.core.config_path = Path(args.config).expanduser()
    self.core.config = self.core._load_config()

try:
    if args.command == 'create':
        # Procesar variables de entorno si se proporcionan
        env_vars = {}
        if args.env_vars:
            for var in args.env_vars:
                if '=' in var:
                    key, value = var.split('=', 1)
                    env_vars[key] = value

        # Procesar archivos de requisitos
        requirements_files = []
        if args.requirements:
            for req_file in args.requirements:
                if os.path.exists(req_file):
                    requirements_files.append(req_file)
                else:
                    logger.warning(f"Archivo de requisitos no encontrado: {req_file}")

        spec = EnvironmentSpec(
            name=args.name,
            python_version=args.python,
            packages=args.packages,
            channels=args.channels,
            pip_packages=args.pip_packages,
            description=getattr(args, 'description', ''),
            tags=getattr(args, 'tags', []),
            environment_variables=env_vars,
            requirements_files=requirements_files
        )

        # Función de progreso para operaciones largas
        def progress_callback(message):
            print(f"  {message}")

        success = self.core.create_environment_advanced(
            spec,
            use_smart_resolution=not getattr(args, 'no_smart_resolve', False),
            progress_callback=progress_callback
        )

        if success:
            print(f"✓ Entorno '{args.name}' creado exitosamente")

            # Mostrar información del entorno creado
            if args.name in self.core.env_db:
                env_info = self.core.env_db[args.name]
                print(f"  - Python: {env_info.get('python_version', 'N/A')}")
                print(f"  - Paquetes: {len(env_info.get('packages', []))}")
                print(f"  - Tamaño: {env_info.get('size_mb', 0):.1f} MB")
                print(f"  - Salud: {env_info.get('health_score', 0)}/100")
        else:
            print(f"✗ Error creando entorno '{args.name}'")
            return 1

    elif args.command == 'clone':
        modifications = {}
        if args.modify_python:
            modifications['python_version'] = args.modify_python
        if args.add_packages:
            modifications['add_packages'] = args.add_packages
        if args.remove_packages:
            modifications['remove_packages'] = args.remove_packages
        if args.add_pip_packages:
            modifications['add_pip_packages'] = args.add_pip_packages

        success = self.core.clone_environment_intelligent(
            args.source, args.target, modifications, args.deep
        )

        if success:
            print(f"✓ Entorno '{args.target}' clonado exitosamente desde '{args.source}'")
            if args.deep:
                print("  - Clonación profunda completada (incluye archivos personalizados)")
        else:
            print(f"✗ Error clonando entorno")
            return 1

    elif args.command == 'health':
        health = self.core.analyze_environment_health(args.environment)

        if health.get('status') == 'missing':
            print(f"✗ Entorno '{args.environment}' no encontrado")
            return 1

        # Mostrar resumen de salud
        status_icon = {
            'healthy': '✓',
            'warning': '⚠',
            'critical': '✗',
            'error': '✗'
        }.get(health.get('status', 'unknown'), '?')

        print(f"{status_icon} Salud del entorno '{args.environment}': {health.get('status', 'unknown')}")
        print(f"  - Puntuación: {health.get('health_score', 0)}/100")
        print(f"  - Paquetes: {health.get('package_count', 0)}")
        print(f"  - Tamaño: {health.get('size_mb', 0):.1f} MB")

        if health.get('issues'):
            print("  - Issues detectados:")
            for issue in health.get('issues', []):
                print(f"    • {issue}")

        if health.get('recommendations'):
            print("  - Recomendaciones:")
            for rec in health.get('recommendations', []):
                print(f"    • {rec}")

        # Mostrar detalles si se solicita
        if getattr(args, 'detailed', False) and 'details' in health:
            print("\nDetalles adicionales:")
            details = health['details']

            if 'packages' in details:
                print("  Paquetes instalados:")
                for pkg in details['packages'][:10]:  # Mostrar primeros 10
                    print(f"    • {pkg['name']}={pkg['version']}")
                if len(details['packages']) > 10:
                    print(f"    ... y {len(details['packages']) - 10} más")

            if 'performance' in details:
                perf = details['performance']
                print("  Métricas de rendimiento:")
                if 'activation_time' in perf:
                    print(f"    • Tiempo de activación: {perf['activation_time']:.2f}s")
                if 'import_times' in perf:
                    print("    • Tiempos de importación:")
                    for pkg, time in perf['import_times'].items():
                        if time is not None:
                            print(f"      - {pkg}: {time:.3f}s")

    elif args.command == 'bulk':
        params = {}
        if hasattr(args, 'export_path'):
            params['export_path'] = args.export_path
        if hasattr(args, 'timeout'):
            params['timeout'] = args.timeout

        # Función de progreso para operaciones en lote
        def progress_callback(message):
            print(f"  {message}")

        results = self.core.bulk_operations(
            args.operation,
            args.environments,
            params,
            parallel=getattr(args, 'parallel', False),
            progress_callback=progress_callback
        )

        # Mostrar resumen de resultados
        successful = sum(1 for r in results if r['success'])
        total = len(results)

        print(f"\nResumen de operación '{args.operation}': {successful}/{total} exitosos")

        for result in results:
            status_icon = "✓" if result['success'] else "✗"
            print(f"{status_icon} {result['environment']}: {result['message']}")

    elif args.command == 'template':
        if args.template_action == 'list':
            templates = self.core.list_templates()

            if not templates:
                print("No hay plantillas disponibles")
            else:
                print("Plantillas disponibles:")
                for template in templates:
                    print(f"  • {template['name']}")
                    if template.get('description'):
                        print(f"    - {template['description']}")
                    print(f"    - Python: {template.get('python_version', 'N/A')}")
                    print(f"    - Paquetes: {template.get('package_count', 0)}")
                    print(f"    - Creada: {template.get('created_at', 'N/A')}")
                    print()

        elif args.template_action == 'create':
            if args.from_env:
                # Crear plantilla desde entorno existente
                if args.from_env in self.core.env_db:
                    env_spec = self.core.env_db[args.from_env]
                    spec = EnvironmentSpec(**env_spec)
                    spec.name = args.name
                    spec.description = f"Plantilla creada desde entorno {args.from_env}"

                    success = self.core.create_template(args.name, spec)
                    if success:
                        print(f"✓ Plantilla '{args.name}' creada desde entorno '{args.from_env}'")
                    else:
                        print(f"✗ Error creando plantilla")
                        return 1
                else:
                    print(f"✗ Entorno '{args.from_env}' no encontrado")
                    return 1
            else:
                print("✗ Especificar --from-env para crear plantilla desde entorno existente")
                return 1

        elif args.template_action == 'use':
            customizations = {}
            if getattr(args, 'modify_python', None):
                customizations['python_version'] = args.modify_python
            if getattr(args, 'add_packages', None):
                customizations['packages'] = args.add_packages

            def progress_callback(message):
                print(f"  {message}")

            success = self.core.create_from_template(
                args.template,
                args.environment,
                customizations,
                progress_callback
            )

            if success:
                print(f"✓ Entorno '{args.environment}' creado desde plantilla '{args.template}'")
            else:
                print(f"✗ Error creando entorno desde plantilla")
                return 1

    elif args.command == 'resolve':
        resolution = self.core.smart_dependency_resolution(
            args.packages,
            args.channels,
            getattr(args, 'python', None)
        )

        if getattr(args, 'format', 'table') == 'json':
            print(json.dumps(resolution, indent=2))
        else:
            print("Análisis de dependencias:")
            print(f"  - Paquetes: {', '.join(resolution['packages'])}")
            print(f"  - Canales: {', '.join(resolution['channels'])}")
            print(f"  - Tiempo de resolución: {resolution['resolution_time']:.2f}s")

            if resolution['conflicts']:
                print("  - Conflictos detectados:")
                for conflict in resolution['conflicts']:
                    print(f"    • {conflict}")

            if resolution['suggestions']:
                print("  - Sugerencias:")
                for suggestion in resolution['suggestions']:
                    print(f"    • {suggestion}")

            if resolution['optimal_solution']:
                print("  - Solución óptima:")
                for pkg in resolution['optimal_solution']:
                    print(f"    • {pkg}")

            if resolution['alternatives']:
                print("  - Alternativas:")
                for pkg, alternatives in resolution['alternatives'].items():
                    print(f"    • {pkg}: {', '.join(alternatives[:3])}")

            print(f"  - Tamaño estimado: {resolution['estimated_size']:.1f} MB")

    elif args.command == 'snapshot':
        if args.snapshot_action == 'list':
            snapshots = []
            for snapshot_file in self.core.snapshots_path.glob("*.json"):
                try:
                    with open(snapshot_file, 'r') as f:
                        snapshot_data = json.load(f)
                        snapshots.append({
                            'name': snapshot_data['name'],
                            'environment': snapshot_data['environment'],
                            'timestamp': snapshot_data['timestamp'],
                            'size_mb': snapshot_data.get('size_mb', 0),
                            'health_score': snapshot_data.get('health_score', 0)
                        })
                except Exception as e:
                    logger.warning(f"Error cargando snapshot {snapshot_file}: {e}")

            if not snapshots:
                print("No hay snapshots disponibles")
            else:
                print("Snapshots disponibles:")
                for snapshot in sorted(snapshots, key=lambda x: x['timestamp'], reverse=True):
                    print(f"  • {snapshot['name']}")
                    print(f"    - Entorno: {snapshot['environment']}")
                    print(f"    - Fecha: {snapshot['timestamp']}")
                    print(f"    - Tamaño: {snapshot['size_mb']:.1f} MB")
                    print(f"    - Salud: {snapshot['health_score']}/100")
                    print()

        elif args.snapshot_action == 'create':
            snapshot = self.core.environment_snapshot_system(
                args.environment,
                getattr(args, 'name', None),
                not getattr(args, 'no_perf', False)
            )

            if 'error' in snapshot:
                print(f"✗ Error creando snapshot: {snapshot['error']}")
                return 1
            else:
                print(f"✓ Snapshot '{snapshot['name']}' creado exitosamente")
                print(f"  - Entorno: {snapshot['environment']}")
                print(f"  - Tamaño: {snapshot['size_mb']:.1f} MB")
                print(f"  - Salud: {snapshot.get('health_score', 0)}/100")
                if 'performance_metrics' in snapshot:
                    perf = snapshot['performance_metrics']
                    if 'activation_time' in perf:
                        print(f"  - Tiempo de activación: {perf['activation_time']:.2f}s")

        elif args.snapshot_action == 'restore':
            exclude_packages = getattr(args, 'exclude', [])
            success = self.core.restore_from_snapshot(
                args.snapshot,
                getattr(args, 'target', None),
                exclude_packages
            )

            if success:
                target_env = getattr(args, 'target', None) or f"restored_{args.snapshot}"
                print(f"✓ Entorno '{target_env}' restaurado desde snapshot '{args.snapshot}'")
                if exclude_packages:
                    print(f"  - Paquetes excluidos: {', '.join(exclude_packages)}")
            else:
                print(f"✗ Error restaurando snapshot")
                return 1

    elif args.command == 'watch':
        if args.watch_action == 'list':
            watchers = self.core.list_active_watchers()

            if not watchers:
                print("No hay watchers activos")
            else:
                print("Watchers activos:")
                for watcher in watchers:
                    print(f"  • {watcher['id']}")
                    print(f"    - Entorno: {watcher['environment']}")
                    print(f"    - Estado: {watcher['status']}")
                    print(f"    - Última verificación: {watcher.get('last_check', 'Nunca')}")
                    print(f"    - Verificaciones: {watcher['checks_performed']}")
                    print(f"    - Alertas: {watcher['alerts_count']}")
                    print(f"    - Salud promedio: {watcher.get('health_score', 0):.1f}/100")
                    print()

        elif args.watch_action == 'start':
            watch_config = {
                'check_interval': getattr(args, 'interval', 3600),
                'auto_repair': getattr(args, 'auto_repair', False),
                'auto_update': getattr(args, 'auto_update', False),
                'health_threshold': getattr(args, 'health_threshold', 80)
            }

            watcher_id = self.core.automatic_environment_watcher(
                args.environment,
                watch_config
            )

            print(f"✓ Watcher iniciado: {watcher_id}")
            print(f"  - Entorno: {args.environment}")
            print(f"  - Intervalo: {watch_config['check_interval']}s")
            print(f"  - Auto-reparación: {'Sí' if watch_config['auto_repair'] else 'No'}")
            print(f"  - Umbral de salud: {watch_config['health_threshold']}")

        elif args.watch_action == 'stop':
            success = self.core.stop_watcher(args.watcher_id)

            if success:
                print(f"✓ Watcher {args.watcher_id} detenido")
            else:
                print(f"✗ Error deteniendo watcher {args.watcher_id}")
                return 1

        elif args.watch_action == 'status':
            # Buscar watcher
            watcher_file = self.core.watchers_path / f"{args.watcher_id}.json"

            if not watcher_file.exists():
                print(f"✗ Watcher {args.watcher_id} no encontrado")
                return 1

            with open(watcher_file, 'r') as f:
                watcher_data = json.load(f)

            print(f"Estado del watcher {args.watcher_id}:")
            print(f"  - Entorno: {watcher_data['environment']}")
            print(f"  - Estado: {watcher_data['status']}")
            print(f"  - Creado: {watcher_data['created_at']}")
            print(f"  - Última verificación: {watcher_data.get('last_check', 'Nunca')}")
            print(f"  - Verificaciones: {watcher_data.get('checks_performed', 0)}")

            config = watcher_data.get('config', {})
            print(f"  - Intervalo: {config.get('check_interval', 'N/A')}s")
            print(f"  - Auto-reparación: {'Sí' if config.get('auto_repair') else 'No'}")
            print(f"  - Umbral de salud: {config.get('health_threshold', 'N/A')}")

            alerts = watcher_data.get('alerts', [])
            if alerts:
                print(f"  - Alertas recientes ({len(alerts)}):")
                for alert in alerts[-5:]:  # Mostrar últimas 5
                    print(f"    • {alert['timestamp']}: {alert['severity']} - {alert['message']}")

            health_history = watcher_data.get('health_history', [])
            if health_history:
                recent_scores = [h['score'] for h in health_history[-10:]]
                avg_score = sum(recent_scores) / len(recent_scores)
                print(f"  - Salud promedio (últimas 10): {avg_score:.1f}/100")

    elif args.command == 'optimize':
        def progress_callback(message):
            print(f"  {message}")

        result = self.core.optimize_environment(
            args.environment,
            getattr(args, 'strategy', 'balanced')
        )

        if result['success']:
            print(f"✓ Entorno '{args.environment}' optimizado")
            print(f"  - Estrategia: {result['strategy']}")
            print(f"  - Tamaño antes: {result['size_before']:.1f} MB")
            print(f"  - Tamaño después: {result['size_after']:.1f} MB")

            size_reduction = result['size_before'] - result['size_after']
            if size_reduction > 0:
                print(f"  - Reducción de tamaño: {size_reduction:.1f} MB")

            print("  - Acciones realizadas:")
            for action in result['actions_performed']:
                print(f"    • {action}")
        else:
            print(f"✗ Error optimizando entorno")
            if 'error' in result:
                print(f"  - {result['error']}")
            return 1

    elif args.command == 'benchmark':
        result = self.core.benchmark_environment(
            args.environment,
            getattr(args, 'tests', ['import', 'computation', 'memory'])
        )

        if 'error' in result:
            print(f"✗ Error en benchmark: {result['error']}")
            return 1

        print(f"Benchmark del entorno '{args.environment}':")
        print(f"  - Puntuación general: {result['overall_score']:.1f}/100")

        for test_name, test_result in result['tests'].items():
            print(f"\n  Prueba: {test_result.get('test', test_name)}")

            if 'score' in test_result:
                print(f"    - Puntuación: {test_result['score']:.1f}/100")

            if 'packages' in test_result:
                print("    - Tiempos de importación:")
                for pkg, time in test_result['packages'].items():
                    print(f"      • {pkg}: {time:.3f}s")
                if 'average_time' in test_result:
                    print(f"      - Promedio: {test_result['average_time']:.3f}s")

            if 'operations' in test_result:
                print("    - Operaciones:")
                for op, time in test_result['operations'].items():
                    print(f"      • {op}: {time:.3f}s")

            if 'metrics' in test_result:
                print("    - Métricas de memoria:")
                for metric, value in test_result['metrics'].items():
                    print(f"      • {metric}: {value:.1f} MB")

        if 'comparison' in result:
            comparison = result['comparison']
            if comparison.get('similar_environments'):
                print(f"\n  Entornos similares: {', '.join(comparison['similar_environments'])}")

    elif args.command == 'migrate':
        result = self.core.migrate_environment(
            args.environment,
            args.target_system,
            getattr(args, 'target_path', None)
        )

        if result['success']:
            print(f"✓ Entorno '{args.environment}' migrado a {args.target_system}")
            print("  - Pasos realizados:")
            for step in result['steps']:
                print(f"    • {step}")

            if args.target_system == 'docker':
                print("\n  Para completar la migración a Docker:")
                print("    1. Navegue al directorio creado")
                print("    2. Ejecute 'bash build.sh' para construir la imagen")
                print("    3. Ejecute 'docker run -it {args.environment}:latest' para usar")

            elif args.target_system == 'file':
                print("\n  Para restaurar en otro sistema:")
                print("    1. Copie los archivos de migración")
                print("    2. Ejecute 'bash restore.sh' para restaurar el entorno")

            elif args.target_system == 'cloud':
                print("\n  Para completar la migración a la nube:")
                print("    1. Suba los archivos preparados al servicio de almacenamiento")
                print("    2. En el sistema destino, descargue y ejecute 'restore_from_cloud.sh'")
        else:
            print(f"✗ Error en migración: {result.get('error', 'Error desconocido')}")
            return 1

    elif args.command == 'schedule':
        if args.schedule_action == 'list':
            tasks = self.core.list_scheduled_tasks()

            if not tasks:
                print("No hay tareas programadas")
            else:
                print("Tareas programadas:")
                for task in tasks:
                    status_icon = "✓" if task['active'] else "✗"
                    print(f"  {status_icon} {task['id']}")
                    print(f"    - Operación: {task['operation']}")
                    print(f"    - Programación: {task['schedule']}")
                    print(f"    - Entornos: {task['environments_count']}")
                    print(f"    - Última ejecución: {task.get('last_run', 'Nunca')}")
                    print(f"    - Próxima ejecución: {task.get('next_run', 'N/A')}")
                    print(f"    - Ejecuciones: {task['run_count']}")
                    print()

        elif args.schedule_action == 'cancel':
            success = self.core.cancel_scheduled_task(args.task_id)

            if success:
                print(f"✓ Tarea {args.task_id} cancelada")
            else:
                print(f"✗ Error cancelando tarea {args.task_id}")
                return 1

        else:  # Crear nueva tarea programada
            task_id = self.core.schedule_operation(
                args.operation,
                args.schedule,
                getattr(args, 'environments', None)
            )

            print(f"✓ Tarea programada: {task_id}")
            print(f"  - Operación: {args.operation}")
            print(f"  - Programación: {args.schedule}")
            print(f"  - Entornos: {getattr(args, 'environments', 'Todos')}")

    else:
        print(f"Comando no reconocido: {args.command}")
        self.parser.print_help()
        return 1

except KeyboardInterrupt:
    print("\n✗ Operación cancelada por el usuario")
    return 1
except Exception as e:
    logger.error(f"Error ejecutando comando: {e}")
    if args.verbose if hasattr(args, 'verbose') else False:
        import traceback
        traceback.print_exc()
    else:
        print(f"✗ Error: {e}")
    return 1

return 0

def main():
    """Punto de entrada principal."""
    try:
        cli = CoralCLI()
        exit_code = cli.run()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n¡Hasta luego!")
        sys.exit(0)
    except Exception as e:
        print(f"Error fatal: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
