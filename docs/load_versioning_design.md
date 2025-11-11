# Diseño: Sistema de Versionado y Load

## Objetivo

Implementar una etapa de Load que persista datos procesados en S3 con un sistema de versionado, preparando el terreno para futuras proyecciones y staging.

## Estructura de Datos en S3

```
datasets/{dataset_id}/
├── index/                          # Puntero a la versión actual
│   └── current_version.txt         # Contiene el ID de la versión actual
│
└── versions/
    └── {version_id}/               # Ej: v20240115_143022, v1, v2, etc.
        ├── data/                   # Archivos parquet particionados
        │   ├── {internal_series_code}/
        │   │   ├── year=2024/
        │   │   │   ├── month=01/
        │   │   │   │   ├── part-00000.parquet
        │   │   │   │   └── part-00001.parquet
        │   │   │   └── month=02/
        │   │   │       └── ...
        │   │   └── year=2023/
        │   │       └── ...
        │   └── {otra_series_code}/
        │       └── ...
        └── manifest.json           # Changelog y metadata de la versión
```

**Nota**: El mismo patrón de particionamiento se usará en proyecciones futuras.

## Componentes Arquitectónicos

### 1. **PartitionStrategy** (Interfaz y Implementaciones)

#### 1.1. **Interfaz Base** (`src/infrastructure/versioning/partition_strategy.py`)
**Responsabilidad**: Definir la interfaz común para todas las estrategias de particionamiento

**Clase abstracta**:
```python
class PartitionStrategy(ABC):
    @abstractmethod
    def get_partition_path(data_point: Dict[str, Any]) -> str: ...
    @abstractmethod
    def group_by_partition(data: List[Dict]) -> Dict[str, List[Dict]]: ...
    @abstractmethod
    def parse_partition_path(partition_path: str) -> Dict[str, str]: ...
    @abstractmethod
    def get_all_partitions_from_paths(paths: List[str]) -> Set[str]: ...
```

#### 1.2. **Implementaciones Concretas**

**SeriesYearMonthPartitionStrategy** (`src/infrastructure/versioning/strategies/series_year_month.py`)
- **Patrón**: `{internal_series_code}/year={YYYY}/month={MM}/`
- **Default**: Estrategia por defecto si no se especifica otra
- **Implementación**:
  - Extrae `internal_series_code` del data point
  - Extrae `year` y `month` de `obs_time` (datetime)
  - Construye path: `f"{internal_series_code}/year={year}/month={month:02d}/"`
  - Maneja timezone-aware y naive datetimes correctamente

**Otras estrategias futuras** (ejemplos):
- `SeriesYearPartitionStrategy`: `{internal_series_code}/year={YYYY}/`
- `SeriesPartitionStrategy`: `{internal_series_code}/`
- `YearMonthPartitionStrategy`: `year={YYYY}/month={MM}/` (sin serie)

#### 1.3. **PartitionStrategyFactory** (`src/infrastructure/versioning/partition_strategy_factory.py`)
**Responsabilidad**: Crear la estrategia de particionamiento según la configuración del dataset

**Operaciones**:
- `create(config: Dict[str, Any]) -> PartitionStrategy`: Crea la estrategia según la configuración
- Lee `load.partition_strategy` del config (o usa default si no está)
- Sigue el mismo patrón que `StateManagerFactory` y `LockManagerFactory`

**Estrategias disponibles**:
- `series_year_month` (default): `SeriesYearMonthPartitionStrategy`
- Futuras estrategias se registran aquí

**Uso**:
```python
from src.infrastructure.versioning.partition_strategy_factory import PartitionStrategyFactory

strategy = PartitionStrategyFactory.create(config)
partition_path = strategy.get_partition_path(data_point)
grouped = strategy.group_by_partition(data_list)
```

**Reutilización**:
- Usado por `ParquetWriter` para escribir datos particionados
- Usado por futuros componentes de proyección (mismo factory, misma estrategia)
- Centralizado para evitar duplicación de lógica

### 2. **VersionManager** (`src/infrastructure/versioning/version_manager.py`)
**Responsabilidad**: Gestionar el ciclo de vida de versiones y el puntero `index/current_version.txt`

**Operaciones**:
- `create_new_version(dataset_id: str) -> str`: Genera un nuevo ID de versión y crea la estructura de carpetas
- `get_current_version(dataset_id: str) -> Optional[str]`: Lee el puntero actual
- `set_current_version(dataset_id: str, version_id: str) -> None`: Actualiza el puntero (operación atómica)
- `list_versions(dataset_id: str) -> List[str]`: Lista todas las versiones existentes

**Estrategia de versionado**:
- Opción 1: Timestamp-based: `v{YYYYMMDD}_{HHMMSS}` (ej: `v20240115_143022`)
- Opción 2: Secuencial: `v1`, `v2`, `v3` (requiere leer versiones existentes)
- **Recomendación**: Timestamp-based para evitar race conditions y ser más descriptivo

### 3. **ManifestManager** (`src/infrastructure/versioning/manifest_manager.py`)
**Responsabilidad**: Gestionar el `manifest.json` con changelog y metadata

**Estructura del manifest.json**:
```json
{
  "version_id": "v20240115_143022",
  "dataset_id": "bcra_infomondia_series",
  "created_at": "2024-01-15T14:30:22Z",
  "collection_date": "2024-01-15T14:25:00Z",  // collection_date de los datos
  "data_points_count": 15234,
  "series_count": 16,
  "series_codes": ["BCRA_TC_OFICIAL_A3500_PESOSxUSD_D", ...],
  "date_range": {
    "min_obs_time": "2020-01-01T00:00:00Z",
    "max_obs_time": "2024-01-15T00:00:00Z"
  },
  "changelog": {
    "new_series": ["BCRA_NEW_SERIES_D"],
    "updated_series": ["BCRA_TC_OFICIAL_A3500_PESOSxUSD_D"],
    "data_points_added": 234,
    "data_points_updated": 0
  },
  "parquet_files": [
    "data/BCRA_TC_OFICIAL_A3500_PESOSxUSD_D/year=2024/month=01/part-00000.parquet",
    "data/BCRA_TC_OFICIAL_A3500_PESOSxUSD_D/year=2024/month=02/part-00000.parquet",
    ...
  ],
  "partitions": [
    "BCRA_TC_OFICIAL_A3500_PESOSxUSD_D/year=2024/month=01",
    "BCRA_TC_OFICIAL_A3500_PESOSxUSD_D/year=2024/month=02",
    ...
  ],
  "partition_strategy": "series_year_month",
  "previous_version": "v20240114_120000"
}
```

**Operaciones**:
- `create_manifest(version_id: str, dataset_id: str, data: List[Dict], previous_version: Optional[str]) -> Dict`: Genera el manifest desde los datos
- `save_manifest(bucket: str, dataset_id: str, version_id: str, manifest: Dict) -> None`: Persiste el manifest en S3
- `load_manifest(bucket: str, dataset_id: str, version_id: str) -> Dict`: Carga un manifest existente
- `compare_with_previous(current_data: List[Dict], previous_manifest: Dict) -> Dict`: Genera el changelog comparando con versión anterior

### 4. **ParquetWriter** (`src/infrastructure/versioning/parquet_writer.py`)
**Responsabilidad**: Escribir datos a formato Parquet de manera eficiente con particionamiento

**Consideraciones**:
- Usar `pyarrow` o `pandas` para escribir parquet
- **Siempre usa particionamiento** según la `PartitionStrategy` configurada para el dataset
- Compresión: `snappy` (default) o `gzip` (más compresión, más lento)
- Schema: Definir schema explícito para consistencia
- Recibe `PartitionStrategy` como parámetro (creada por el factory según config)

**Operaciones**:
- `write_to_parquet(data: List[Dict], base_output_path: str, partition_strategy: PartitionStrategy) -> List[str]`: 
  - Agrupa datos por partición usando `partition_strategy.group_by_partition()`
  - Escribe un archivo parquet por partición (o múltiples partes si hay muchos datos en una partición)
  - Retorna lista de paths relativos de archivos generados
- `get_schema() -> pyarrow.Schema`: Retorna el schema esperado

**Estructura de datos en Parquet**:
- Columnas: `obs_time`, `internal_series_code`, `value`, `unit`, `frequency`, `collection_date`
- Tipos: `obs_time` (timestamp), `internal_series_code` (string), `value` (double), `unit` (string), `frequency` (string), `collection_date` (timestamp)
- **Nota**: Las columnas `year` y `month` NO se incluyen en el schema (son parte del path de partición)

### 5. **S3VersionedLoader** (`src/infrastructure/plugins/loaders/s3_versioned_loader.py`)
**Responsabilidad**: Implementar la interfaz `Loader` usando los componentes anteriores

**Flujo de ejecución**:
1. Recibe datos transformados del ETL y la configuración
2. Crea `PartitionStrategy` usando `PartitionStrategyFactory.create(config)` según la config del dataset
3. Crea nueva versión con `VersionManager`
4. Escribe datos a Parquet con `ParquetWriter` (pasando la `PartitionStrategy` creada)
5. Sube archivos Parquet a S3 en `datasets/{dataset_id}/versions/{version_id}/data/{partitions}/`
6. Genera manifest con `ManifestManager` (comparando con versión anterior si existe)
   - El manifest incluye la lista de particiones y archivos generados
   - El manifest también incluye qué `partition_strategy` se usó
7. Sube manifest a S3
8. Actualiza puntero `index/current_version.txt` (operación atómica al final)

**Configuración requerida** (en YAML):
```yaml
load:
  plugin: s3_versioned
  bucket: my-data-bucket
  version_strategy: timestamp  # o "sequential"
  compression: snappy  # o "gzip"
  partition_strategy: series_year_month  # opcional, default: "series_year_month"
  aws_region: us-east-1  # opcional
```

**Configuración de particionamiento**:
- Cada dataset puede definir su propia estrategia de particionamiento
- Si no se especifica, usa `series_year_month` (default)
- La estrategia se crea mediante `PartitionStrategyFactory.create(config)`
- Misma estrategia se usa en versiones y proyecciones para mantener consistencia

## Decisiones de Diseño

### 1. **Atomicidad del Versionado**
- El puntero `index/current_version.txt` se actualiza al FINAL, después de que todos los archivos estén escritos
- Si falla algo antes, la versión queda "huérfana" pero no afecta la versión actual
- Opcional: Implementar cleanup de versiones huérfanas en futuras ejecuciones

### 2. **Comparación con Versión Anterior**
- Para generar el changelog, necesitamos leer el manifest de la versión anterior
- Si no hay versión anterior, el changelog marca todo como "new"
- La comparación puede ser costosa si hay muchos datos → hacerlo a nivel de manifest/metadata, no cargando todos los datos

### 3. **Manejo de Errores**
- Si falla la escritura de Parquet: rollback (eliminar versión parcial)
- Si falla el upload a S3: retry con exponential backoff
- Si falla la actualización del puntero: la versión existe pero no está "activa" (requiere intervención manual o retry)

### 4. **Idempotencia**
- Si se ejecuta el ETL dos veces con los mismos datos, se crean dos versiones diferentes
- Esto es esperado: cada ejecución del ETL genera una nueva versión
- El changelog mostrará que no hay cambios reales (comparando con la versión anterior)

### 5. **Preparación para Proyecciones (Futuro)**
- La estructura actual no interfiere con futuras proyecciones
- Las proyecciones pueden vivir en `datasets/{dataset_id}/projections/`
- El staging puede ser `datasets/{dataset_id}/staging/`
- El versionado de datos procesados es independiente del versionado de proyecciones
- **`PartitionStrategyFactory` es reutilizable**: Las proyecciones usarán el mismo factory para crear la estrategia según la config del dataset
- **Consistencia**: Mismo patrón de partición en versiones y proyecciones para el mismo dataset

## Dependencias

- `boto3`: Ya presente en el proyecto
- `pyarrow` o `pandas`: Para escribir Parquet (agregar a requirements.txt)
- `pytz`: Ya presente (para timestamps)

## Integración con ETL Actual

**Cambio necesario en la interfaz `Loader`**:
- Actualmente: `load(data: Any) -> None`
- Nuevo: `load(data: Any, config: Dict[str, Any]) -> None`
- Similar a cómo `Parser.parse()` y `Transformer.transform()` reciben `config`

**Cambio en `ETLUseCase`**:
- Actualizar `_execute_etl()` para pasar `config` al loader: `self._loader.load(data, config)`

**Flujo**:
1. `ETLUseCase` llama a `loader.load(data, config)` después del transform
2. El loader recibe la configuración completa para poder crear la `PartitionStrategy` apropiada
3. La configuración se lee del YAML y se pasa al loader a través del `PluginRegistry`
4. El loader usa `PartitionStrategyFactory.create(config)` para crear la estrategia según `load.partition_strategy` en el config

## Testing Strategy

1. **Unit Tests**: Cada componente por separado con mocks de S3
2. **Integration Tests**: 
   - Test con S3 local (LocalStack) o bucket de test
   - Verificar estructura de carpetas
   - Verificar contenido de manifest
   - Verificar atomicidad del puntero
3. **Builder Pattern**: Usar los builders existentes para crear datos de test

## Preguntas Abiertas

1. **Retención de Versiones**: ¿Cuántas versiones mantener?
   - **Recomendación**: Por ahora, mantener todas. Agregar política de retención después.

2. **Compresión**: ¿snappy (rápido) o gzip (más compresión)?
   - **Recomendación**: snappy por defecto, configurable

3. **Schema Evolution**: ¿Cómo manejar cambios en el schema?
   - **Recomendación**: Por ahora, schema fijo. Agregar versionado de schema después si es necesario.

## Próximos Pasos (Post-Implementación)

1. Implementar proyecciones y staging
2. Agregar métricas/observabilidad (cuántas versiones, tamaño, etc.)
3. Implementar cleanup de versiones antiguas
4. Agregar validación de integridad (checksums)
5. Optimizar con particionamiento si es necesario (ya implementado desde el inicio)

