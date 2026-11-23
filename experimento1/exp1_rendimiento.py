import time
from confluent_kafka import Producer

CSV_PATH = "Books_rating.csv"
# Empezamos con 300.000 registros para probar que todo funciona rápido.
# Luego podemos subirlo a 1_000_000 o None (los 3 millones completos).
MAX_RECORDS = None

TOPICOS = [
    ("exp-p1", 1),
    ("exp-p3", 3),
    ("exp-p6", 6),
]

def probar_escritura(topic_name, num_particiones):
    conf = {
        "bootstrap.servers": "localhost:9092",
        "acks": 1,
        "linger.ms": 5,
        "batch.size": 65536,
    }
    producer = Producer(conf)
    enviados = 0
    inicio = time.perf_counter()

    with open(CSV_PATH, "rb") as f:
        next(f)  # Saltar encabezados
        for linea in f:
            while True:
                try:
                    producer.produce(topic_name, value=linea)
                    break
                except BufferError:
                    producer.poll(0.1)

            enviados += 1
            if enviados % 50_000 == 0:
                producer.poll(0)
            if MAX_RECORDS and enviados >= MAX_RECORDS:
                break

    producer.flush()
    duracion = time.perf_counter() - inicio
    throughput = enviados / duracion

    print(f"Particiones: {num_particiones} | Registros: {enviados:,} | Tiempo: {duracion:.2f} s | Velocidad: {throughput:,.2f} reg/s")

if __name__ == "__main__":
    for topic, p in TOPICOS:
        probar_escritura(topic, p)
