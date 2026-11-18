# Pipeline de datos end-to-end con Azure Databricks

## 1. Descripción

Proyecto de ingeniería de datos que procesa tres entidades relacionadas —**Customers**, **Orders** y **Payments**— mediante una arquitectura **Bronze / Silver / Gold** implementada en Azure Databricks con PySpark y Delta Lake.

El proyecto se centra en cargas incrementales, transformación y validación de datos, persistencia en tablas Delta, auditoría por lotes, idempotencia, reintentos y recuperación de ejecuciones interrumpidas.

> **Alcance:** proyecto técnico y demostración de capacidades. No se declara como solución lista para producción. Los datos incluyen registros de prueba utilizados para validar distintos escenarios.

## 2. Objetivos técnicos

- Ingerir archivos de entrada y conservar los datos recibidos en Bronze.
- Validar y transformar registros en Silver, separando registros inválidos cuando corresponde.
- Publicar dimensiones y hechos en Gold mediante procesamiento incremental.
- Evitar reprocesamientos innecesarios mediante identificación de lotes e idempotencia.
- Registrar el estado de cada lote y sus intentos de ejecución.
- Recuperar ejecuciones interrumpidas y verificar resultados con pruebas de extremo a extremo.
- Orquestar los procesos mediante Databricks Workflows.

## 3. Tecnologías

| Tecnología | Uso |
|---|---|
| Azure Databricks | Plataforma de procesamiento y ejecución |
| Apache Spark / PySpark | Transformaciones y procesamiento distribuido |
| Delta Lake | Tablas transaccionales, operaciones `MERGE` e historial |
| Databricks SQL | Validaciones y consultas de integridad |
| Databricks Workflows | Ejecución de los pipelines |
| Python | Implementación de lógica reutilizable, auditoría y recuperación |
| Unity Catalog | Organización de tablas por capas y dominio de control |

## 4. Arquitectura

```text
Archivos de entrada
       │
       ▼
   Bronze (Delta)
       │
       ▼
   Silver (Delta) ──────► Registros inválidos / cuarentena
       │
       ▼
    Gold (Delta)
       │
       ├── dim_customers
       ├── fact_orders
       └── fact_payments

Control transversal:
  de_control.etl_batches
  de_control.etl_batch_attempts

Orquestación:
  Databricks Workflows
```

Las tablas del proyecto se organizan en el catálogo `workspace`:

| Esquema | Responsabilidad |
|---|---|
| `workspace.de_bronze` | Datos ingeridos |
| `workspace.de_silver` | Datos transformados y validados |
| `workspace.de_gold` | Dimensiones y hechos para consulta |
| `workspace.de_control` | Auditoría de lotes e intentos |

### 4.1 Bronze

Recibe los registros de entrada y conserva la información necesaria para el procesamiento posterior. La cantidad de registros de Bronze puede superar a la de Silver porque existen cargas sucesivas y actualizaciones incrementales.

### 4.2 Silver

Aplica validaciones y transformaciones de negocio. Los registros que no cumplen las reglas de calidad se separan para su revisión. Las entidades procesadas son `customers`, `orders` y `payments`.

### 4.3 Gold

Publica las tablas:

- `workspace.de_gold.dim_customers`: estado actual de clientes.
- `workspace.de_gold.fact_orders`: órdenes.
- `workspace.de_gold.fact_payments`: pagos.

Adicionalmente, `workspace.de_gold.dim_customers_scd2` contiene un **ejercicio independiente** de historial de clientes mediante Slowly Changing Dimension Type 2. No se presenta como una dimensión histórica sincronizada automáticamente con todas las cargas incrementales del pipeline principal.

## 5. Procesamiento incremental e idempotencia

Los pipelines utilizan operaciones sobre tablas Delta, incluyendo `MERGE`, para mantener los datos procesados y evitar duplicaciones en ejecuciones repetidas.

El control de lotes emplea un identificador derivado de la entidad y de la huella SHA-256 del contenido de entrada. El objetivo es reconocer cargas ya procesadas y permitir reintentos controlados.

**Importante:** la idempotencia se ha probado en escenarios del proyecto; no implica que todas las posibles condiciones de concurrencia estén resueltas.

## 6. Auditoría y recuperación

### 6.1 `etl_batches`

La tabla `workspace.de_control.etl_batches` mantiene el estado general de cada lote, incluidos datos de origen, hash de entrada, cantidades de registros, fechas de finalización por etapa, número de intentos y metadatos de control de ejecución.

Entre los estados utilizados se encuentran `PENDING`, `PROCESSING`, `COMPLETED` y `FAILED`.

### 6.2 `etl_batch_attempts`

La tabla `workspace.de_control.etl_batch_attempts` conserva los intentos individuales, su resultado, duración y detalles de error cuando corresponde.

### 6.3 Recuperación

El proyecto contempla:

- Adquisición de un intento y registro de su ejecución.
- Control de ejecución activa mediante un mecanismo de *lease*.
- Recuperación de intentos registrados cuyo *lease* haya expirado.
- Conciliación de intentos que permanecieron `PROCESSING` cuando el lote ya había quedado finalizado.
- Pruebas de reintento y de ejecución repetida sin reprocesamiento innecesario.

**Limitación conocida:** las modificaciones de `etl_batches` y `etl_batch_attempts` se realizan en transacciones Delta separadas. Por tanto, **no existe atomicidad transaccional conjunta entre ambas tablas**. Los mecanismos de recuperación reducen inconsistencias, pero no eliminan todas las carreras de concurrencia. Algunas reparaciones se han validado como procedimientos manuales y no deben describirse como recuperación automática general.

## 7. Orquestación con Databricks Workflows

Se implementaron ejecuciones para las tres entidades. Entre los notebooks de ejecución y sus *runners* se encuentran:

| Entidad | Notebook de ejecución | Runner |
|---|---|---|
| Customers | `14_customers_job` | `15_customers_job_runner` |
| Orders | `16_orders_job` | `17_orders_job_runner` |
| Payments | `18_payments_job` | `19_payments_job_runner` |

Los flujos se han ejecutado en escenarios de procesamiento inicial, reintento y regresión. La configuración exacta de los Jobs, sus parámetros y dependencias debe documentarse junto con la exportación de Workflows para garantizar reproducibilidad.

## 8. Resultados de validación

### 8.1 Volúmenes observados al cierre de la revisión

| Entidad | Bronze | Silver | Gold |
|---|---:|---:|---:|
| Customers | 31 | 15 | 15 |
| Orders | 16 | 10 | 10 |
| Payments | 14 | 12 | 12 |

Estas cifras corresponden al estado del proyecto durante la revisión técnica; no son objetivos de capacidad ni métricas de rendimiento.

### 8.2 Consistencia Silver ↔ Gold

Se compararon los campos de negocio mediante `EXCEPT` en ambas direcciones, excluyendo el campo técnico `processed_at`.

| Entidad | Silver menos Gold | Gold menos Silver | Resultado |
|---|---:|---:|---|
| Customers | 0 | 0 | PASSED |
| Orders | 0 | 0 | PASSED |
| Payments | 0 | 0 | PASSED |

En total, **37 registros** coincidieron en los campos seleccionados.

### 8.3 Integridad referencial y claves

Las consultas ejecutadas no encontraron:

- Duplicados por clave de negocio en `dim_customers`, `fact_orders` y `fact_payments`.
- Duplicados por `customer_sk` en `dim_customers_scd2`.
- Órdenes sin cliente relacionado.
- Pagos sin orden relacionada.

Estas verificaciones no sustituyen restricciones transaccionales ni cubren todas las reglas posibles de negocio.

### 8.4 Historial SCD Type 2

La tabla `dim_customers_scd2` contenía **7 versiones correspondientes a 5 clientes**. Las comprobaciones de intervalos temporales no encontraron intervalos inválidos ni solapamientos en los casos evaluados.

El notebook `09_customers_scd2.py` incluye carga inicial, detección de cambios, cierre e inserción de versiones, recuperación de una versión faltante y una variante de `MERGE` único. Parte de los cambios se genera con eventos definidos manualmente.

**Cobertura:** otros 10 clientes presentes en `dim_customers` no estaban representados en SCD2. Esta diferencia se documenta como consecuencia del alcance independiente del ejercicio, no como una sincronización completa entre ambas tablas.

### 8.5 Pruebas de recuperación

Se verificaron escenarios de recuperación por expiración de *lease*, reintento de Payments y conciliación de intentos finalizados de forma incompleta. También se ejecutaron pruebas de regresión que omitieron lotes previamente completados.

Los resultados documentan los escenarios probados; no constituyen una demostración de tolerancia a fallos bajo cualquier condición.

## 9. Limitaciones y decisiones de diseño

1. **Atomicidad de auditoría:** no hay transacción única que actualice simultáneamente las dos tablas de control.
2. **Concurrencia:** ciertos mecanismos de conciliación y reparación requieren mayor protección frente a ejecuciones simultáneas antes de usarse en producción.
3. **Renovación del lease:** existe lógica de renovación, pero no se ha integrado como *heartbeat* continuo de todos los Workflows de larga duración.
4. **SCD2:** se implementó como ejercicio técnico independiente, no como parte obligatoria del procesamiento incremental de Customers.
5. **Modelo analítico:** `fact_orders` y `fact_payments` conservan identificadores de negocio; no se implementó una unión temporal automática a la clave sustituta de SCD2.
6. **Reproducibilidad:** deben versionarse los recursos de infraestructura, configuración de Jobs, datos de ejemplo y dependencias para facilitar el despliegue en otro workspace.
7. **Escenario de negocio:** Customers, Orders y Payments constituyen dominios del proyecto.

## 10. Cómo reproducir el proyecto

La reproducción exacta depende de los notebooks, módulos Python, archivos de entrada, permisos de Unity Catalog y configuración de Databricks Workflows.

Secuencia conceptual:

1. Preparar el workspace de Databricks y los esquemas `de_bronze`, `de_silver`, `de_gold` y `de_control` en el catálogo `workspace`.
2. Crear las tablas Delta con los esquemas esperados.
3. Incorporar los módulos Python de procesamiento y auditoría.
4. Configurar las rutas de los archivos de entrada y los parámetros de ejecución.
5. Ejecutar Customers, Orders y Payments mediante sus respectivos Workflows.
6. Revisar el estado de los lotes y sus intentos en `de_control`.
7. Ejecutar las consultas de validación de claves, integridad referencial y consistencia Silver ↔ Gold.

## 11. Estado del proyecto

**Proyecto funcional completado y revisión técnica principal superada**, con limitaciones conocidas documentadas. La preparación del repositorio público y su reproducción en un workspace limpio son actividades de presentación y empaquetado que deben verificarse por separado.

### Notebooks de validación

Los notebooks `tests/02_batch_audit_tests.py` y `tests/10_pipeline_validation.py` se ejecutan en Databricks; no son tests `pytest`. El primero incluye escenarios de auditoría y referencias `%run` a notebooks del directorio `notebooks/`; el segundo contiene consultas SQL de validación y exploración. Su ejecución requiere disponer de las tablas de Unity Catalog correspondientes.

## 12. Definiciones de Databricks Workflows

El repositorio incluye `databricks.yml` y tres definiciones en `resources/` (Customers, Orders y Payments), transcritas de las configuraciones exportadas del Workspace. Se reemplazaron las rutas absolutas vinculadas al usuario por `${workspace.file_path}/notebooks/...`, para que apunten a archivos desplegados por un Databricks Asset Bundle.

- `customers_incremental_pipeline`: `process_customers`, `customers_batch_7.csv`, `lease_minutes=60`.
- `orders_incremental_pipeline`: `process_orders`, `orders_batch_1.csv`.
- `payments_incremental_pipeline`: `process_payments`, `payments_batch_1.csv`.

Los Jobs tienen `queue.enabled=true`, `performance_target=PERFORMANCE_OPTIMIZED` e intervalo mínimo entre reintentos de 15 minutos.
