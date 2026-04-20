# Documentación del Pipeline — Amazon Sales ETL

## 1. ¿Este proceso es ETL o ELT? ¿Por qué?

Este pipeline es **ETL**, y la razón es bastante directa: los datos se limpian y transforman en Python con pandas *antes* de llegar a la base de datos. Filtramos nulos, eliminamos cantidades inválidas, parseamos fechas y calculamos el `average_ticket` todo en memoria. Solo cuando el DataFrame ya está limpio y listo lo mandamos a PostgreSQL.

Con ELT sería al revés — los datos crudos caerían primero en la bodega y las transformaciones las haría el propio motor de base de datos usando SQL. Eso tiene sentido cuando tienes algo como Snowflake o BigQuery que tiene poder de cómputo nativo. Acá no es el caso: la fuente es un CSV plano, el volumen son ~129k filas, y pandas hace el trabajo más rápido y con menos fricción que armar transformaciones SQL sobre datos sucios.

---

## 2. ¿Qué cambiaría si los datos llegaran en streaming en lugar de archivos CSV?

Con CSV el pipeline se ejecuta cuando hay un archivo nuevo, procesa todo el lote de una sola vez y listo. Con streaming la historia es diferente porque los datos llegan registro a registro (o en micro-lotes) de forma continua, lo que cambia bastante cómo hay que diseñar todo:

| Aspecto | CSV / Batch | Streaming |
|---|---|---|
| **Ingesta** | `pd.read_csv()` sobre un archivo | Consumidor de Kafka, Kinesis o Pub/Sub |
| **Transformación** | DataFrame completo en memoria | Funciones sin estado por registro, o Flink / Spark Structured Streaming |
| **Ventanas de tiempo** | No aplica | Necesarias para agrupar eventos (ej. ingresos por hora) |
| **Gestión de estado** | Ninguna | Requerida para deduplicar entre micro-lotes y acumular métricas |
| **Errores** | Rollback del lote completo | Dead-letter queue por registro individual |
| **Latencia** | Minutos a horas | Segundos a milisegundos |
| **Cambios de esquema** | Correr el pipeline de nuevo | Schema Registry para no romper todo cuando cambia un campo |

Las tres funciones `extraer`, `transformar` y `cargar` seguirían existiendo conceptualmente, pero `extraer` pasaría a ser un loop de consumidor y `transformar` tendría que funcionar sobre un solo evento a la vez, sin asumir que tiene acceso a todo el dataset.

---

## 3. ¿Qué herramienta usarías para orquestar este pipeline en producción y por qué?

**Apache Airflow**, sin dudarlo. Y hay una razón concreta más allá de que sea popular: el cohort ya lo tiene desplegado en AWS con backend en PostgreSQL y corriendo como servicio con systemd, así que no partiríamos de cero.

Dicho eso, Airflow también gana por sus propios méritos. Modela el pipeline como un DAG, lo que significa que cada función — extraer, transformar, cargar — se convierte en una tarea independiente con sus propias reglas de reintento y dependencias explícitas. Si falla la carga pero la extracción y transformación fueron bien, Airflow puede reintentar solo esa tarea sin volver a correr todo.

Además, la interfaz web da visibilidad completa: historial de corridas, logs por tarea, duración, y alertas. Eso reemplaza los `logger.info()` del script actual con algo que puedes consultar días después o mostrarle a alguien del equipo sin necesidad de conectarse al servidor.

Y si el pipeline crece — por ejemplo, agregar un paso que suba un reporte de calidad a S3 o notifique por Slack cuando algo falla — Airflow lo maneja con sus operadores nativos sin tener que reescribir lógica de orquestación.

**Prefect** sería la alternativa si se quiere algo más ligero y sin la carga operativa de mantener una base de metadatos separada. Es más simple de arrancar en local, aunque le falta algo de flexibilidad en scheduling comparado con Airflow para cargas de trabajo más complejas.