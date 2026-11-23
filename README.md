# Apache Kafka – Grupo 3

Extracción y Gestión de Datos Masivos 2026-2

**Integrantes:** Isidora Martínez, José Tomás Rivera, Johan Muñoz

Este repositorio contiene el paso a paso de la instalación reproducible de Apache Kafka (1 nodo) con Docker Compose y el código de los experimentos de la Etapa 3.

---

# Parte 1: Instalación general de Kafka en Docker

## Prerrequisitos

- Docker Desktop instalado y en ejecución.
- Mínimo 4 GB de RAM disponibles para Docker.
- Espacio en disco: ~400 MB para la imagen oficial, más el espacio del dataset de cada experimento.
- Espacio en disco para dataset: ~3 GB

## Paso 1. Crear el archivo `docker-compose.yml`

En una carpeta de trabajo, crear el archivo `docker-compose.yml` con este contenido, o directamente descargar el del repositorio:

```yaml
services:
  kafka:
    image: apache/kafka:latest
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      - KAFKA_NODE_ID=1
      - KAFKA_PROCESS_ROLES=broker,controller
      - KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092
      - KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      - KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093
      - KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
      - KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
      - KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
```
## Paso 2. Levantar el contenedor

Desde la carpeta donde está el archivo:

```bash
docker compose up -d
```

Verifica que el contenedor esté en ejecución (estado `Up`):

```bash
docker ps
```

## Paso 3. Entrar a la consola de Kafka

```bash
docker exec -it kafka bash
```

Los scripts de Kafka están en `/opt/kafka/bin`. Dentro de la consola, nos moveremos a esa ubicación con el comando:

```bash
cd /opt/kafka/bin
```

## Paso 4. Pruebas básicas

### Operación 1: crear un tópico

Dentro de la consola de Kafka:

```bash
./kafka-topics.sh --create --topic dataset-prueba --bootstrap-server localhost:9092
```

Salida esperada: `Created topic dataset-prueba.`

Para listar los tópicos existentes:

```bash
./kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Operación 2: cargar un dataset

**2.1.** En tu computador (fuera de Docker), crea un archivo `dataset.csv` o desacarga el que usamos en los experimentos en la carpeta del `docker-compose.yml`, por ejemplo:

```Ejemplo de csv para probar el funcionamiento
sensor_id,timestamp,temperatura,humedad
SENS-001,2026-11-07T10:00:00,22.5,45.2
SENS-002,2026-11-07T10:00:01,22.7,45.0
SENS-003,2026-11-07T10:00:02,22.4,45.5
SENS-001,2026-11-07T10:00:03,22.6,45.1
SENS-002,2026-11-07T10:00:04,22.8,44.9
SENS-003,2026-11-07T10:00:05,22.3,45.6
SENS-001,2026-11-07T10:00:06,22.5,45.3
SENS-002,2026-11-07T10:00:07,22.9,44.8
SENS-003,2026-11-07T10:00:08,22.2,45.7
SENS-001,2026-11-07T10:00:09,22.6,45.2
```

**2.2.** Copia el archivo al contenedor. Este comando se ejecuta **fuera** de la consola de Kafka (en tu computador):

```bash
docker cp dataset.csv kafka:/opt/kafka/bin/
```

**2.3.** Dentro de la consola de Kafka hay que enviar el archivo al tópico:

```bash
cd /opt/kafka/bin
cat dataset.csv | ./kafka-console-producer.sh --topic dataset-prueba --bootstrap-server localhost:9092
```

### Operación 3: consultar los datos

Dentro de la consola de Kafka, inicia un consumidor que lea todos los registros desde el principio:

```bash
./kafka-console-consumer.sh --topic dataset-prueba --from-beginning --bootstrap-server localhost:9092
```

Deben aparecer las 11 líneas del CSV (encabezado y 10 registros). Para detener el consumidor, presiona `Ctrl + C`.

## Paso 5. Salir y detener el entorno

Para salir de la consola de Kafka:

```bash
exit
```

Para detener y eliminar el contenedor (en la carpeta del `docker-compose.yml`):

```bash
docker compose down
```


# Parte 2: Experimentos

Cada experimento tiene su propia carpeta en este repositorio. Dentro de cada carpeta hay un archivo con las instrucciones paso a paso para realizar ese experimento en específico.

## Experimento 1

Instrucciones: [experimento1/INSTRUCCIONES.md](experimento1/INSTRUCCIONES-EXPERIMENTO1.md)

## Experimento 2

Instrucciones: [experimento2/INSTRUCCIONES.md](experimento2/INSTRUCCIONES-EXPERIMENTO2.md)

## Experimento 3

Instrucciones: [experimento3/INSTRUCCIONES.md](experimento3/INSTRUCCIONES-EXPERIMENTO3.md)

---
