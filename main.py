from fastapi import FastAPI, HTTPException
import pandas as pd
import numpy as np


app = FastAPI ()
# http://127.0.0.1:8000


# Definir el endpoint para el mensaje introductorio
@app.get("/")
async def root():
    return {"message by Jorge Cabuya": "¡Bienvenido a la API de Servicios de Steam! En esta API, encontrarás una variedad de endpoints que ofrecen diferentes funciones . Desde obtener información sobre desarrolladores y usuarios hasta estadísticas de uso, ¡aquí es donde encontrarás todo!"}



#Primera funcion 

# Cargar el DataFrame
df_ep1 = pd.read_csv("df_endp1.csv")
    

# Definir el endpoint para obtener estadísticas por desarrollador y año
@app.get("/developer_stats")
def developer_stats(developer: str):
    # Filtrar el DataFrame por el desarrollador especificado
    developer_data = df_ep1[df_ep1['Developer'] == developer]
    
    # Verificar si se encontraron datos para el desarrollador especificado
    if developer_data.empty:
        raise HTTPException(status_code=404, detail="Desarrollador no encontrado")
    
    # Agrupar los datos por año y contar el número de elementos en cada grupo
    grouped_data = developer_data.groupby('Año').agg({'Cantidad de Items': 'count', 'Contenido Free': 'sum'})
    
    # Convertir los datos a formato JSON y devolverlos
    return grouped_data.to_dict(orient="index")

#Segunda Funcion

df_ep2 = pd.read_csv("df_endp2.csv")


# Definir el endpoint para obtener información del usuario
# Definir el endpoint para obtener información del usuario
@app.get("/userdata")
def userdata(user_id: str):
    # Agrupar por user_id y realizar las operaciones necesarias
    user_grouped = df_ep2[df_ep2['user_id'] == user_id].groupby('user_id').agg({
        'Dinero Gastado': 'sum',
        'Cantidad de Items': 'sum'
    })
    
    # Verificar si se encontraron datos para el user_id especificado
    if user_grouped.empty:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    # Obtener los datos relevantes del usuario
    dinero_gastado_total = user_grouped['Dinero Gastado'].iloc[0]
    cantidad_items_total = user_grouped['Cantidad de Items'].iloc[0]
    
    
    # Crear el diccionario con la información del usuario
    user_info = {
        "Usuario": user_id,
        "Dinero gastado": dinero_gastado_total,
        "Cantidad total de items": cantidad_items_total
    }
    
    return user_info


#Tercera Funcion

df_ep3 = pd.read_csv("df_endp3.csv")


# Definir el endpoint para obtener el user con mas horas
@app.get("/user_for_genre")
def UserForGenre(genero: str):
    # Filtrar el DataFrame por el género especificado
    genre_data = df_ep3[df_ep3['genres'] == genero]
    
    # Verificar si se encontraron datos para el género especificado
    if genre_data.empty:
        raise HTTPException(status_code=404, detail=f"No se encontraron datos para el género {genero}")
    
    # Encontrar el usuario que acumula más horas jugadas para el género dado
    max_playtime_user = genre_data.groupby('user_id')['playtime_forever'].sum().idxmax()
    
    # Calcular la acumulación de horas jugadas por año de lanzamiento
    playtime_by_year = genre_data.groupby('release_date')['playtime_forever'].sum().to_dict()
    
    # Crear el diccionario con la información del usuario para el género dado
    user_info = {
        "Género": genero,
        "Usuario con más horas jugadas": max_playtime_user,
        "Acumulación de horas jugadas por año de lanzamiento": playtime_by_year
    }
    
    return user_info


#Cuarta Funcion

df_ep4 = pd.read_csv("df_endp4.csv")


# Definir el endpoint para obtener los mejores desarrolladores para un año dado
@app.get("/best_developer_year")
def best_developer_year(año: int):
    # Filtrar el DataFrame por el año especificado y las condiciones de recomendación
    year_data = df_ep4[(df_ep4['Año'] == año) & (df_ep4['recommend'] == True) & (df_ep4['sentiment_analysis'] == 2)]
    
    # Verificar si se encontraron datos para el año especificado y las condiciones dadas
    if year_data.empty:
        raise HTTPException(status_code=404, detail=f"No se encontraron datos para el año {año} con las condiciones dadas")
    
    # Agrupar por desarrollador y contar el número de juegos recomendados para cada uno
    developer_recommendations = year_data.groupby('Desarrollador').size().reset_index(name='Num_Recomendados')
    
    # Ordenar los desarrolladores por el número de juegos recomendados en orden descendente
    top_developers = developer_recommendations.sort_values(by='Num_Recomendados', ascending=False)
    
    # Obtener el top 3 de desarrolladores con más juegos recomendados
    top_3_developers = top_developers.head(3).to_dict(orient='records')
    
    return top_3_developers


#Quinta Funcion

df_ep5 = pd.read_csv("df_endp5.csv")


# Definir el endpoint para el análisis de reseñas del desarrollador
@app.get("/developer_reviews_analysis")
def developer_reviews_analysis(desarrolladora: str):
    # Filtrar el DataFrame por el desarrollador especificado
    developer_data = df_ep5[df_ep5['Desarrollador'] == desarrolladora]
    
    # Verificar si se encontraron datos para el desarrollador especificado
    if developer_data.empty:
        raise HTTPException(status_code=404, detail=f"No se encontraron datos para el desarrollador {desarrolladora}")
    
    # Agrupar por desarrollador y contar la cantidad de reseñas positivas y negativas
    grouped_reviews = developer_data.groupby('Desarrollador')['sentiment_analysis'].value_counts().unstack().fillna(0)
    
    # Obtener la cantidad de reseñas positivas y negativas
    positive_reviews = int(grouped_reviews.loc[desarrolladora, 2])
    negative_reviews = int(grouped_reviews.loc[desarrolladora, 0])
    
    # Crear el diccionario con los resultados
    developer_analysis = {
        desarrolladora: {
            "Positivas": positive_reviews,
            "Negativas": negative_reviews
        }
    }
    
    return developer_analysis 
