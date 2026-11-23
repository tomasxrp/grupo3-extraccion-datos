# Experimento 2: efecto del nivel de confirmación (`acks`) en la escritura

**Encargado:** José Rivera
---

## 1. Información general

**Pregunta:** ¿cómo afecta el nivel de confirmación (`acks`) del productor a la velocidad de escritura (registros por segundo) y a la latencia promedio (ms) en Apache Kafka?

**Hipótesis:** con `acks=0` la escritura es más rápida y con menor latencia, porque el productor no espera confirmación. Con `acks=1` y `acks=all` es más lenta y, como hay un solo nodo, ambas deberían dar resultados parecidos.

| Elemento | Detalle |
|---|---|
| Variable que se modifica | `acks` del productor: `0`, `1` y `all` |
| Qué se mide | Throughput (records/s y MB/s) y latencia promedio (ms). Además p99 y latencia máxima. Todas son estadísticas que entrega la propia herramienta de Kafka al ingresar datos a los tópicos. |
| Qué se mantiene fijo | 1 broker Kafka, tópico de 1 partición y `replication-factor 1`, 2.000.000 de registros por ejecución, mensaje de 500 bytes, `batch.size=1024`, heap de 1 GB (`-Xmx1g`) y el mismo equipo |
| Repeticiones | 3 por cada configuración de `acks` (9 ejecuciones), intercaladas: 0, 1, all, 0, 1, all, 0, 1, all |
| Herramienta de medición | `kafka-producer-perf-test.sh` (incluida en Kafka) |

---

## 2. Requisitos previos

- Haber completado la instalación de la [Parte 1 del README principal](../README.md) (Docker Desktop funcionando y `docker-compose.yml` listo).
- Python 3 instalado.
- Espacio libre en disco: ~2,7 GB del dataset + ~0,8 GB del payload + ~10 GB para los datos que acumulan los tópicos durante las mediciones.
- Docker Desktop con al menos 4 GB de RAM asignados.

---

## 3. Archivos necesarios y dataset

### 3.1. Descargar el dataset

1. Entra a: **https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews** y descargar el datset .
2. Copia **`Books_rating.csv`** a la carpeta de trabajo (la misma donde estará `docker-compose.yml`). El archivo `books_data.csv` no se usa en este experimento.

### 3.2. Verificar que estén todos los archivos

La carpeta de trabajo debe contener, como mínimo:

```
carpeta-de-trabajo/
├── docker-compose.yml     (de la Parte 1 del README)
├── crear_payload.py       (script que genera el payload)
└── Books_rating.csv       (descargado de Kaggle, ~2,7 GB)
```

## 4. Datos del experimento

El script `crear_payload.py`:

1. Lee `Books_rating.csv` y toma la columna `review/text`.
2. Descarta las reseñas de menos de 500 bytes.
3. Recorta cada reseña restante a **exactamente 500 bytes**, para que todos los mensajes midan lo mismo.
4. Escribe una reseña por línea en `payload.txt`.

Cada medición usa `--num-records 2000000`. La herramienta elige líneas del payload al azar, así que cada ejecución envía 2.000.000 de registros (~1 GB) y algunos mensajes se repiten.

---

## 5. Paso a paso

> Los pasos 1 a 3 se ejecutan en el **computador**. Los pasos 4 a 7 se ejecutan **dentro de la consola de Kafka**.

### Paso 1. Levantar el contenedor de Kafka

Asegúrate de que `docker-compose.yml` esté en la carpeta.

```bash
docker-compose up -d
docker ps
```

### Paso 2. Crear el payload

```bash
python crear_payload.py
```

- Salida esperada: `Payload listo: 1540844 mensajes de 500 bytes en payload.txt`
- Comprueba el archivo si se creo el archivo payload.txt


### Paso 3. Copiar el payload al contenedor

```bash
docker cp payload.txt kafka:/tmp/payload.txt
```

### Paso 4. Entrar a la consola de Kafka y preparar la sesión

```bash
docker exec -it kafka sh
```

Dentro de la consola:

```bash
export KAFKA_HEAP_OPTS=-Xmx1g
cd /opt/kafka/bin
./kafka-topics.sh --version
```

> El `export` solo vale para la sesión actual.

### Paso 5. Crear un tópico por configuración

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic t-acks0 --partitions 1 --replication-factor 1
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic t-acks1 --partitions 1 --replication-factor 1
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic t-acksall --partitions 1 --replication-factor 1
```

### Paso 6. Ejecutar las mediciones

Hay un comando por configuración. Solo cambian `--topic` y `acks=`:

```bash
./kafka-producer-perf-test.sh --topic t-acks0 --num-records 2000000 --payload-file /tmp/payload.txt --throughput -1 --producer-props bootstrap.servers=localhost:9092 acks=0 linger.ms=0 batch.size=1024
```

```bash
./kafka-producer-perf-test.sh --topic t-acks1 --num-records 2000000 --payload-file /tmp/payload.txt --throughput -1 --producer-props bootstrap.servers=localhost:9092 acks=1 linger.ms=0 batch.size=1024
```

```bash
./kafka-producer-perf-test.sh --topic t-acksall --num-records 2000000 --payload-file /tmp/payload.txt --throughput -1 --producer-props bootstrap.servers=localhost:9092 acks=all linger.ms=0 batch.size=1024
```

**Orden de las 9 ejecuciones** (intercaladas, para reducir el efecto del calentamiento del sistema):

| Ejecución | Tópico | `acks` |
|---|---|---|
| 1 | `t-acks0` | 0 |
| 2 | `t-acks1` | 1 |
| 3 | `t-acksall` | all |
| 4 | `t-acks0` | 0 |
| 5 | `t-acks1` | 1 |
| 6 | `t-acksall` | all |
| 7 | `t-acks0` | 0 |
| 8 | `t-acks1` | 1 |
| 9 | `t-acksall` | all |

### Paso 7. Borrar los tópicos

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic t-acks0
./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic t-acks1
./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic t-acksall
```

Para salir de la consola: `exit`.

---

## 6. Resultados obtenidos

| Ejecución | acks | records/s | MB/s | Lat. prom. (ms) | p99 (ms) | Lat. máx. (ms) |
|---|---|---|---|---|---|---|
| 1 | 0 | 13562,172 | 6,47 | 2391,52 | 3005 | 3808 |
| 2 | 1 | 10589,798 | 5,05 | 3059,07 | 3704 | 3786 |
| 3 | all | 10501,829 | 5,01 | 3090,99 | 4191 | 4689 |
| 4 | 0 | 13546,647 | 6,46 | 2392,65 | 3248 | 3549 |
| 5 | 1 | 11591,246 | 5,53 | 2801,20 | 3534 | 3679 |
| 6 | all | 10604,510 | 5,06 | 3058,78 | 4010 | 4120 |
| 7 | 0 | 14419,610 | 6,88 | 2248,66 | 2896 | 3041 |
| 8 | 1 | 10977,068 | 5,23 | 2957,37 | 3934 | 4112 |
| 9 | all | 10256,462 | 4,89 | 3157,55 | 4925 | 5111 |

**Promedio de las 3 ejecuciones por configuración:**

| acks | records/s (promedio) | Lat. prom. (ms) (promedio) |
|---|---|---|
| 0 | 13.843 | 2.344 |
| 1 | 11.053 | 2.939 |
| all | 10.454 | 3.102 |

## 7. Gráfico de los resultados

<img width="1800" height="720" alt="image" src="https://github.com/user-attachments/assets/a8bd29e8-b17c-4b91-8943-4e92c991a9ea" />
