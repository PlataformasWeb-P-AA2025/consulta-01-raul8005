import os
import pandas as pd
from pymongo import MongoClient

#                                   ACTIVIDAD DE LA CONSULTA 1

# ----- 1. CONEXION A MONGODB -----
#Credenciales locales, nombre de base de datos y coleccion
cliente = MongoClient("mongodb://localhost:27017/")
db = cliente["tennis_excel"]
coleccion = db["partidos"]


# ----- 2. LEER LOS ARCHIVOS EXCEL -----
# Carpeta que contiene los archivos, y los nombres de los archivos
carpeta_data = "data"
archivos_excel = ["2022.xlsx", "2023.xlsx"]

# Limpiar colección para evitar duplicados
coleccion.delete_many({})

# ----- 3. EXTRAER LA INFORMACION DE LOS ARCHIVOS Y GUARDAR EN LA COLECCION DE BASE DE DATOS -----
# Ciclo para recorrer cada archivo 
for archivo in archivos_excel:
    ruta = os.path.join(carpeta_data, archivo)

    if os.path.exists(ruta):
        print(f"Leyendo archivo: {archivo}")
        df = pd.read_excel(ruta, engine="openpyxl")
        
        # Reemplazar comas decimales por puntos y convertir columnas numéricas
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.replace(",", ".", regex=False)

        # Intentar convertir cada columna a numérica sin lanzar advertencias.
        # Si no se puede convertir (por ejemplo, contiene texto), se deja como está.
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                pass

        # Convierte Dataframe en colecciones en lista de diccionarios (JSON)
        registros = df.to_dict(orient="records")

        # Inserta los registros en la base de datos MongoDB
        coleccion.insert_many(registros)
        print(f"Numero de elementos insertados: {len(registros)} registros obtenidos del archivo: {archivo}")
        print("----------------------------------------------------")
    else:
        print(f"No se encontró el archivo: {archivo}")

print("----------------------------------------------------------------------------------------------------------------------------")
print("----------------------------------------------------------------------------------------------------------------------------")


# ------------------------ MONGODB ---> CONSULTAS

# CONSULTA 1
print("\nConsulta 1: Promedio de puntos (WPts) obtenidos por los ganadores:")

# Realiza una agregación en la colección para calcular un promedio
avg_pts = coleccion.aggregate([
    {"$match": {"WPts": {"$gt": 0}}}, # Filtra documentos donde WPts (puntos del ganador) sea mayor a 0
    {"$group": {"_id": None, "promedio_puntos": {"$avg": "$WPts"}}} # Se calcula el promedio de los puntos de los ganadores usando "$avg" y los agrupa
])

# Recorre el resultado de la agregación, aunque solo es un resultado 
for agregacion in avg_pts:
    #Muestra el promedio calculado, limitado a 2 decimales
    print(f"Puntos promedio de ganadores: {round(agregacion['promedio_puntos'], 2)}")

print("----------------------------------------------------")

# CONSULTA 2agregacion
print("\nConsulta 2: Partidos con retiro de un jugador:")

# Realiza una búsqueda en la colección de MongoDB
retiros = coleccion.find({"Comment": {"$regex": "Retired", "$options": "i"}}).limit(3)
# Itera sobre cada partido encontrado que cumple con el criterio
for r in retiros:
    # Imprime el nombre del ganador y del perdedor del partido, junto con el comentario que indica el retiro
    print(f"{r['Winner']} vs {r['Loser']} — Comentario: {r['Comment']}")


