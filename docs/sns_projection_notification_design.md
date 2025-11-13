# Notificaciones SNS para Actualizaciones de Proyección

## Descripción

El pipeline ETL publica notificaciones SNS cuando se actualizan las proyecciones con datos nuevos. Esto permite a los consumidores saber cuándo hay nuevos datos disponibles sin tener que consultar S3 constantemente.

## Flujo

```
ETL → Load → Projection → [Si hay datos nuevos] → SNS Notification → Consumidores
```

Las notificaciones solo se publican cuando se escriben datos nuevos en las proyecciones. Si una versión ya estaba proyectada, no se envía notificación.

## Estructura del Evento SNS

El evento se publica como un mensaje JSON con la siguiente estructura:

```json
{
  "event": "projection_update",
  "dataset_id": "bcra_infomondia_series",
  "bucket": "ingestor-datasets",
  "version_manifest_path": "datasets/bcra_infomondia_series/versions/v20251111_014138_730866/manifest.json",
  "projections_path": "datasets/bcra_infomondia_series/projections/"
}
```

### Campos del Evento

- `event`: Tipo de evento, siempre `"projection_update"`
- `dataset_id`: Identificador del dataset actualizado
- `bucket`: Nombre del bucket S3 donde están las proyecciones
- `version_manifest_path`: Ruta al manifest de la versión proyectada en S3 (relativa al bucket)
- `projections_path`: Ruta base donde están las proyecciones en S3 (relativa al bucket)

### Subject del Mensaje SNS

El subject del mensaje SNS es: `projection_update:{dataset_id}`

Ejemplo: `projection_update:bcra_infomondia_series`

## Configuración

### Variable de Entorno

La configuración del SNS topic ARN se obtiene desde una variable de entorno:

```bash
export SNS_PROJECTION_TOPIC_ARN=arn:aws:sns:us-east-1:123456789012:projection-updates
```

Si la variable `SNS_PROJECTION_TOPIC_ARN` está configurada, el servicio de notificaciones se creará automáticamente y publicará eventos cuando se actualicen las proyecciones.

Si la variable no está configurada, el pipeline funcionará normalmente sin publicar notificaciones.

### Ejemplo de Uso

```bash
# Configurar variable de entorno
export SNS_PROJECTION_TOPIC_ARN=arn:aws:sns:us-east-1:123456789012:projection-updates

# Ejecutar ETL
python -m src.cli bcra_infomondia_series
```

## Estructura del Manifest de la Versión

El manifest de la versión contiene información detallada sobre los datos proyectados. Se puede leer desde `version_manifest_path` en el evento SNS.

### Estructura del Manifest

```json
{
  "version_id": "v20240116_120000",
  "dataset_id": "bcra_infomondia_series",
  "created_at": "2024-01-16T12:00:00Z",
  "collection_date": "2024-01-16T11:55:00Z",
  "data_points_count": 15234,
  "series_count": 16,
  "series_codes": [
    "BCRA_TC_OFICIAL_A3500_PESOSxUSD_D",
    "BCRA_RESERVAS_D",
    ...
  ],
  "date_range": {
    "min_obs_time": "2024-01-01T00:00:00Z",
    "max_obs_time": "2024-01-15T00:00:00Z"
  },
  "parquet_files": [
    "data/BCRA_TC_OFICIAL_A3500_PESOSxUSD_D/year=2024/month=01/data.parquet",
    "data/BCRA_RESERVAS_D/year=2024/month=01/data.parquet",
    ...
  ],
  "partitions": [
    "BCRA_TC_OFICIAL_A3500_PESOSxUSD_D/year=2024/month=01",
    "BCRA_RESERVAS_D/year=2024/month=01",
    ...
  ],
  "partition_strategy": "series_year_month"
}
```

### Campos del Manifest

- `version_id`: Identificador de la versión (ej: `"v20240116_120000"`)
- `dataset_id`: Identificador del dataset
- `created_at`: Timestamp de creación de la versión (ISO 8601)
- `collection_date`: Fecha de recolección de los datos (ISO 8601, puede ser `null`)
- `data_points_count`: Número total de puntos de datos en la versión
- `series_count`: Número de series únicas en la versión
- `series_codes`: Lista ordenada de códigos de series (`internal_series_code`)
- `date_range`: Rango de fechas de los datos
  - `min_obs_time`: Fecha mínima de observación (ISO 8601, puede ser `null`)
  - `max_obs_time`: Fecha máxima de observación (ISO 8601, puede ser `null`)
- `parquet_files`: Lista de rutas relativas a los archivos parquet generados
- `partitions`: Lista de particiones afectadas (ej: `"SERIES_CODE/year=2024/month=01"`)
- `partition_strategy`: Nombre de la estrategia de partición usada (ej: `"series_year_month"`)

## Cómo Leer las Proyecciones

Con la información del evento, los consumidores pueden:

1. **Conectarse a S3** usando el `bucket` del evento
2. **Leer el manifest de la versión** desde `version_manifest_path` para obtener información detallada sobre:
   - Series incluidas (`series_codes`)
   - Rango de fechas (`date_range`)
   - Particiones afectadas (`partitions`)
3. **Listar archivos en projections** usando `projections_path` como prefijo
4. **Leer archivos parquet** directamente desde las proyecciones

### Ejemplo de Consumidor

```python
import json
import boto3
import pyarrow.parquet as pq

# Parsear mensaje SNS
message = json.loads(sns_message)
event = json.loads(message["Message"])

# Conectarse a S3
s3 = boto3.client("s3", region_name="us-east-1")
bucket = event["bucket"]
projections_path = event["projections_path"]

# Leer manifest de la versión para obtener detalles
version_manifest_path = event["version_manifest_path"]
manifest_response = s3.get_object(Bucket=bucket, Key=version_manifest_path)
manifest = json.loads(manifest_response["Body"].read())

# Obtener información del manifest
series_codes = manifest.get("series_codes", [])
partitions = manifest.get("partitions", [])

# Listar archivos en projections
response = s3.list_objects_v2(Bucket=bucket, Prefix=projections_path)
for obj in response.get("Contents", []):
    if obj["Key"].endswith(".parquet"):
        # Leer archivo parquet
        table = pq.read_table(f"s3://{bucket}/{obj['Key']}")
        # Procesar datos...
```

## Manejo de Errores

Si la publicación a SNS falla, el error se registra pero **no se interrumpe la proyección**. La proyección es más importante que la notificación, por lo que los errores de notificación no afectan el flujo principal del ETL.

## Implementación

### Componentes

- **`ProjectionNotificationService`** (`src/infrastructure/notifications/projection_notification_service.py`): Servicio para publicar eventos SNS
- **`ProjectionUseCase`**: Orquesta la proyección y publica notificaciones si hay datos nuevos
- **`ProjectionManager`**: Retorna `True` si se escribieron datos nuevos, `False` si la versión ya estaba proyectada

### Flujo de Ejecución

1. `ProjectionUseCase.execute_projection()` ejecuta la proyección
2. `ProjectionManager.project_version()` retorna `True` si se escribieron datos nuevos
3. Si hay datos nuevos y el servicio de notificaciones está configurado, se publica el evento SNS
4. El evento se publica con la información necesaria para que los consumidores puedan leer las proyecciones
