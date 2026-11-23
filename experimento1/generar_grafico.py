import matplotlib.pyplot as plt

particiones = ["1 partición", "3 particiones", "6 particiones"]
velocidades = [249148.88, 220552.01, 215148.92]
colores = ["#1b4f5c", "#2a7b88", "#e85d04"]

plt.figure(figsize=(7, 4.2))
barras = plt.bar(particiones, velocidades, color=colores, width=0.5)

plt.title("Rendimiento de escritura en Kafka según número de particiones\n(3.000.000 registros | 1 broker Docker | acks=1)", fontsize=11, pad=12)
plt.ylabel("Velocidad (registros / segundo)", fontsize=10)
plt.ylim(0, 290000)
plt.grid(axis="y", linestyle="--", alpha=0.5)

for barra, valor in zip(barras, velocidades):
    plt.text(barra.get_x() + barra.get_width()/2, valor + 6000, f"{valor:,.0f} reg/s", ha="center", va="bottom", fontweight="bold", fontsize=9.5)

plt.tight_layout()
plt.savefig("grafico_exp1.png", dpi=300)
print("Gráfico guardado como grafico_exp1.png")
