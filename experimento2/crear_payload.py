import csv
import sys

ENTRADA = "Books_rating.csv"
SALIDA = "payload.txt"
CANTIDAD = 2000000   # cantidad de mensajes
TAMANO = 500        # bytes por mensaje

csv.field_size_limit(2**31 - 1)

n = 0
with open(ENTRADA, newline="", encoding="utf-8") as f, open(SALIDA, "wb") as out:
    for fila in csv.DictReader(f):
        texto = (fila.get("review/text") or "").replace("\r", " ").replace("\n", " ").strip()
        datos = texto.encode("utf-8")
        if len(datos) < TAMANO:
            continue  # se descartan reseñas cortas para que todas midan lo mismo
        datos = datos[:TAMANO].decode("utf-8", errors="ignore").encode("utf-8")
        datos += b" " * (TAMANO - len(datos))  # completa si se cortó un carácter con tilde
        out.write(datos + b"\n")
        n += 1
        if n >= CANTIDAD:
            break

print(f"Payload listo: {n} mensajes de {TAMANO} bytes en {SALIDA}")
