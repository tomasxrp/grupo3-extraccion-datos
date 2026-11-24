# Experimento 3: efecto de la concurrencia de consumidores en la lectura

**Encargado:** Johan Muñoz
---

## 1. Información general

**Pregunta:** ¿cómo cambia la velocidad de lectura (registros por segundo) y el tiempo total de ejecución al aumentar la cantidad de consumidores en paralelo dentro de un mismo *Consumer Group* en Apache Kafka?

**Hipótesis:** al contar con un tópico dividido en 6 particiones, aumentar el número de consumidores en paralelo (`1`, `2`, `4` y `6`) permitirá repartir las particiones entre los miembros del grupo, incrementando la velocidad agregada de lectura (`records/s`) hasta alcanzar la asignación óptima 1:1 (6 consumidores para 6 particiones), aunque en un entorno de un solo nodo (1 broker) el rendimiento quedará limitado por la sobrecarga de coordinación (*rebalance*) y los recursos de CPU y disco del equipo.

| Elemento | Detalle |
|---|---|
| Variable que se modifica | Número de consumidores concurrentes en el mismo *Consumer Group*: `1`, `2`, `4` y `6` |
| Qué se mide | Throughput agregado (`records/s` y `MB/s`), tiempo de rebalanceo promedio (`ms`) y tiempo total real de ejecución (`s`). Son métricas entregadas directamente por la herramienta oficial de benchmarking de Kafka. |
| Qué se mantiene fijo | 1 broker Kafka (modo KRaft), tópico de **6 particiones** y `replication-factor 1`, 3.000.000 de registros totales leídos por configuración desde `Books_rating.csv` (~2,72 GB) y el mismo equipo |
| Ejecuciones | 4 configuraciones de concurrencia (13 instancias de consumidores en total), cada una con un `--group` independiente para asegurar la lectura completa desde el inicio (`offset 0`) |
| Herramienta de medición | `kafka-consumer-perf-test.sh` (incluida en Kafka) |

---

## 2. Requisitos previos

- Haber completado la instalación de la [Parte 1 del README principal](../README.md) (Docker Desktop funcionando y `docker-compose.yml` listo).
- Espacio libre en disco: ~2,7 GB del dataset `Books_rating.csv` + ~3 GB para los datos almacenados en las 6 particiones del tópico.
- Docker Desktop con al menos 4 GB de RAM asignados.

---

## 3. Archivos necesarios y dataset

### 3.1. Descargar el dataset

1. Entra a: **https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews** y descarga el dataset.
2. Descomprime y copia **`Books_rating.csv`** a la carpeta de trabajo (la misma donde estará `docker-compose.yml`). El archivo `books_data.csv` no se usa en este experimento.

### 3.2. Verificar que estén todos los archivos

La carpeta de trabajo debe contener, como mínimo:

```text
carpeta-de-trabajo/
├── docker-compose.yml     (de la Parte 1 del README)
├── run_exp3.sh            (script que automatiza la ejecución en paralelo)
└── Books_rating.csv       (descargado de Kaggle, ~2,86 GB)
```

## 4. Datos y diseño de carga del experimento

A diferencia de los experimentos de ingesta, este experimento evalúa la **fase de consumo** sobre datos ya persistidos en el clúster:

1. Se carga el dataset completo `Books_rating.csv` (omitiendo la fila 1 de encabezados) en un tópico configurado con **6 particiones** (`exp3-concurrencia`), totalizando **3.000.000 de registros**.
2. En cada configuración, los 3.000.000 de registros se dividen equitativamente entre los $C$ consumidores activos (`--num-records $((3000000 / C))`):
   - **1 consumidor:** lee 3.000.000 registros (asignación de 6 particiones).
   - **2 consumidores:** lee 1.500.000 registros c/u (asignación de 3 particiones por consumidor).
   - **4 consumidores:** lee 750.000 registros c/u (asignación de 2-2-1-1 particiones).
   - **6 consumidores:** lee 500.000 registros c/u (asignación simétrica 1:1 de 1 partición por consumidor).
3. El script `run_exp3.sh` lanza los procesos de forma simultánea en segundo plano (`&` + `wait`) y mide tanto las estadísticas internas de Kafka como el tiempo real de pared (*wall-clock time*) desde que arranca el grupo hasta que termina el último consumidor.

---

## 5. Paso a paso

> Los pasos 1 a 2 se ejecutan en el **computador**. Los pasos 3 a 6 se ejecutan **dentro de la consola de Kafka**.

### Paso 1. Levantar el contenedor de Kafka

Asegúrate de que `docker-compose.yml` esté en la carpeta.

```bash
docker compose up -d
docker ps
```

### Paso 2. Copiar el dataset y el script al contenedor

```bash
docker cp Books_rating.csv kafka:/opt/kafka/bin/
docker cp run_exp3.sh kafka:/opt/kafka/bin/
```

### Paso 3. Entrar a la consola de Kafka y preparar el tópico

```bash
docker exec -it kafka bash
```

Dentro de la consola:

```bash
cd /opt/kafka/bin
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic exp3-concurrencia --partitions 6 --replication-factor 1
```

### Paso 4. Cargar los 3.000.000 de registros al tópico

```bash
tail -n +2 Books_rating.csv | ./kafka-console-producer.sh --topic exp3-concurrencia --bootstrap-server localhost:9092
```

- Espera a que finalice la carga y vuelva a aparecer el prompt de la terminal.

### Paso 5. Ejecutar las mediciones de concurrencia

Otorga permisos de ejecución al script `run_exp3.sh` y ejecútalo:

```bash
chmod +x run_exp3.sh
./run_exp3.sh
```

Internamente, el script ejecuta el siguiente comando en paralelo para cada configuración (`C = 1, 2, 4, 6`), usando un `--group` distinto por prueba para leer siempre desde el `offset 0`:

```bash
./kafka-consumer-perf-test.sh --bootstrap-server localhost:9092 --topic exp3-concurrencia --num-records $((3000000 / C)) --group "grupo-v2-${C}c" --hide-header
```

**Orden de las configuraciones ejecutadas:**

| Configuración | Grupo de consumidores | Consumidores en paralelo | Mensajes por consumidor | Reparto de las 6 particiones |
|---|---|---|---|---|
| 1 | `grupo-v2-1c` | 1 | 3.000.000 | 6 |
| 2 | `grupo-v2-2c` | 2 | 1.500.000 | 3 - 3 |
| 3 | `grupo-v2-4c` | 4 | 750.000 | 2 - 2 - 1 - 1 |
| 4 | `grupo-v2-6c` | 6 | 500.000 | 1 - 1 - 1 - 1 - 1 - 1 |

### Paso 6. Borrar el tópico y limpiar archivos temporales

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic exp3-concurrencia
rm -f Books_rating.csv run_exp3.sh
```

Para salir de la consola: `exit`.

---

## 6. Resultados obtenidos

**Detalle por instancia de consumidor:**

| Config. | Consumidor | Grupo | Mensajes leídos | records/s | MB/s | Rebalanceo (ms) | Fetch time (ms) |
|---|---|---|---|---|---|---|---|
| 1 | C1 | `grupo-v2-1c` | 3.000.126 | 182.024,39 | 165,09 | 4.594 | 11.888 |
| 2 | C1 | `grupo-v2-2c` | 1.500.133 | 46.082,79 | 41,84 | 6.511 | 26.042 |
| 2 | C2 | `grupo-v2-2c` | 1.500.227 | 46.051,72 | 41,72 | 6.495 | 26.082 |
| 3 | C1 | `grupo-v2-4c` | 750.067 | 32.442,47 | 29,38 | 6.938 | 16.180 |
| 3 | C2 | `grupo-v2-4c` | 750.005 | 32.175,15 | 29,24 | 6.944 | 16.368 |
| 3 | C3 | `grupo-v2-4c` | 750.406 | 24.459,92 | 22,17 | 7.166 | 23.513 |
| 3 | C4 | `grupo-v2-4c` | 750.486 | 24.376,72 | 22,17 | 7.180 | 23.607 |
| 4 | C1 | `grupo-v2-6c` | 500.490 | 18.137,64 | 16,42 | 6.953 | 20.641 |
| 4 | C2 | `grupo-v2-6c` | 500.242 | 18.034,54 | 16,42 | 6.951 | 20.787 |
| 4 | C3 | `grupo-v2-6c` | 500.347 | 17.875,28 | 16,24 | 6.953 | 21.038 |
| 4 | C4 | `grupo-v2-6c` | 500.217 | 17.762,76 | 16,10 | 6.957 | 21.204 |
| 4 | C5 | `grupo-v2-6c` | 500.004 | 17.432,07 | 15,81 | 6.935 | 21.748 |
| 4 | C6 | `grupo-v2-6c` | 500.357 | 17.410,38 | 15,74 | 6.945 | 21.794 |

**Resumen agregado por configuración de concurrencia:**

| Consumidores | Throughput agregado (records/s) | Flujo total (MB/s) | Rebalanceo prom. (ms) | Tiempo total real (s) | Variación vel. vs. 1 consumidor |
|---|---|---|---|---|---|
| **1** | **182.024,39** | 165,09 | 4.594 | **19,63** | Base (0%) |
| **2** | **92.134,51** | 83,56 | 6.503 | **35,41** | -49,38% |
| **4** | **113.454,26** | 102,96 | 7.057 | **34,04** | -37,67% |
| **6** | **106.652,67** | 96,74 | 6.949 | **32,93** | -41,41% |

---

## 7. Gráfico de los resultados

<img width="1024" height="572" alt="636c9cd9-fd4f-422f-91c9-8f73c57d149d" src="https://github.com/user-attachments/assets/fb556609-8baa-47f9-b782-02ef3ff09ec6" />
---

## 8. Conclusión

Los resultados refutan parcialmente la hipótesis inicial en un entorno local de un único nodo y evidencian cómo opera el modelo de *Consumer Groups* de Apache Kafka:

1. **Lectura secuencial directa vs. sobrecarga de coordinación (*Rebalance*):** Con **1 consumidor**, no existe negociación de asignación de particiones ni competencia por levantar múltiples máquinas virtuales Java (JVM) simultáneas en el mismo equipo, permitiendo leer secuencialmente las 6 particiones desde la *page cache* del sistema operativo a `182.024 reg/s` en `19,63 s` (con un rebalanceo inicial de `4.594 ms`). Al introducir múltiples consumidores concurrentes, el protocolo de rebalanceo eleva la coordinación inicial a ~`6.950–7.057 ms` y los procesos compiten por la misma CPU y disco.
2. **Reparto asimétrico vs. simétrico de particiones (2, 4 y 6 consumidores):** En la configuración de **4 consumidores**, se observa claramente en la tabla de detalle cómo Kafka reparte las 6 particiones de forma desigual (`2-2-1-1`): dos consumidores leyeron 1 partición cada uno a ~`32.300 reg/s` (`~16,2 s` de fetch), mientras que los otros dos asumieron 2 particiones cada uno a ~`24.400 reg/s` (`~23,5 s` de fetch), condicionando el tiempo total del grupo (`34,04 s`). Al pasar a **6 consumidores**, todos alcanzan una carga homogénea de 1 partición por consumidor (~`17.775 reg/s` c/u) y logran el **menor tiempo total entre los escenarios concurrentes (`32,93 s`)**, además de incrementar el *throughput* agregado un **23,1%** al pasar de 2 a 4 consumidores.
3. **Proyección a entornos productivos:** En un único contenedor Docker donde los consumidores solo descartan mensajes sin procesarlos, un solo cliente satura la lectura en memoria; sin embargo, en arquitecturas distribuidas donde cada consumidor corre en un nodo independiente y realiza transformaciones pesadas sobre cada evento, escalar consumidores hasta igualar el número de particiones (relación 1:1) permite paralelizar el trabajo sin cuellos de botella.
