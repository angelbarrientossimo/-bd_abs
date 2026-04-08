# SISTEMAS DE BIG DATA - Examen 2ª Evaluación

### **Nombre**:

**INSTRUCCIONES**:

- Si realizas este examen desde tu ordenador debes **grabar la pantalla** con OBS Studio en formato MKV y entregar el vídeo junto con el examen. Para entregarlo, debes subirlo a OneDrive y adjuntar fichero de texto con la URL del recurso compartido.
- Si lo haces en un equipo del centro, grabaré yo la pantalla desde el ordenador del profesor.
- Debes contestar cada pregunta del examen en la celda del cuaderno Jupyter que hay después de cada pregunta. Si necesitas más celdas, puedes agregarlas a continuación de la que hay.
- Todo tu código **tiene que estár ejecutado**.
- La **entrega** del examen práctico se realizará en el canal de Teams habilitado a tal efecto y consistirá en:
  - Notebook de Jupyter (`.ipynb`).
  - Notebook exportado en formato Markdown (`.md`)
  - Fichero de texto (`.txt.`) con la URL al vídeo compartido en caso de haberlo hecho en tu ordenador.
  - Estos tres ficheros deberán entregarse en un único fichero comprimido en formato ZIP (`.zip`) con el nombre `{apellidos}, {nombre} - SBD Ev2`

Ejecuta la siguiente celda para generar el archivo `flota_rebelde.txt` con el que trabajás en este examen


```python
%%writefile flota_rebelde.txt
name~!~base_asignada~!~naves_disponibles
CR90 corvette~!~Base Yavin 4~!~15
X-wing~!~Base Echo (Hoth)~!~120
Y-wing~!~Base Echo (Hoth)~!~45 naves
Millennium Falcon~!~Flota Nómada~!~1
A-wing~!~Base Endor~!~Error de sensor
Rebel transport~!~Punto de encuentro~!~Ocho
B-wing~!~Astillero Sullust~!~30
EF76 Nebulon-B escort frigate~!~Flota Nómada~!~4
Calamari Cruiser~!~Órbita Mon Cala~!~Desconocido
Star Destroyer~!~Hangar Secreto (Capturado)~!~2
```

    Overwriting flota_rebelde.txt


## EXAMEN PRÁCTICO: Logística de la flota rebelde

Trabajas en el equipo de logística y suministros de la Alianza Rebelde. Recientemente, habéis recibido un archivo de texto con el inventario actual de naves espaciales disponibles en vuestras bases secretas. Sin embargo, este archivo fue generado por un sistema antiguo y contiene errores de formato.

Tu misión es extraer este inventario, limpiarlo y cruzarlo con el catálogo oficial de naves de la API de Star Wars (SWAPI) para calcular la capacidad de carga total de la flota.



### Apartado A: Ingesta y limpieza de la fuente estática (2.5 puntos)

El sistema legado ha exportado el inventario en el archivo `flota_rebelde.txt`. Al inspeccionarlo, notas que el separador de columnas es una secuencia extraña de caracteres (`~!~`) y que la columna numérica tiene datos corruptos.

1.  Carga el archivo `flota_rebelde.txt` en un DataFrame de Pandas. 
2.  La columna `naves_disponibles` contiene basura textual en algunas filas (ej. "5 naves", "Error"). Lee inicialmente esta columna como texto (cadena).
3.  Limpia la columna `naves_disponibles` forzando su conversión a tipo numérico. Asegúrate de transformar los textos irreconocibles en valores nulos (`NaN`).
4.  Elimina las filas que hayan quedado con valor nulo en dicha columna.


```python
def limpiezaNaves(valor_col):
    valor_col = valor_col.replace("naves","")
    valor_col = valor_col.replace("Ocho","8")
    return valor_col
```


```python
import pandas as pd
df_ejer1 = pd.read_csv("./flota_rebelde.txt", sep="~!~", engine="python",dtype={"naves_disponibles": "string"}, na_values =["Error de sensor","Desconocido"],converters={"naves_disponibles":limpiezaNaves})
df_ejer1 = df_ejer1.dropna(subset=["naves_disponibles"])
```

    /tmp/ipykernel_242/3329166864.py:2: ParserWarning: Both a converter and dtype were specified for column naves_disponibles - only the converter will be used.
      df_ejer1 = pd.read_csv("./flota_rebelde.txt", sep="~!~", engine="python",dtype={"naves_disponibles": "string"}, na_values =["Error de sensor","Desconocido"],converters={"naves_disponibles":limpiezaNaves})



```python
df_ejer1.head(12)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>base_asignada</th>
      <th>naves_disponibles</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CR90 corvette</td>
      <td>Base Yavin 4</td>
      <td>15</td>
    </tr>
    <tr>
      <th>1</th>
      <td>X-wing</td>
      <td>Base Echo (Hoth)</td>
      <td>120</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Y-wing</td>
      <td>Base Echo (Hoth)</td>
      <td>45</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Millennium Falcon</td>
      <td>Flota Nómada</td>
      <td>1</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Rebel transport</td>
      <td>Punto de encuentro</td>
      <td>8</td>
    </tr>
    <tr>
      <th>6</th>
      <td>B-wing</td>
      <td>Astillero Sullust</td>
      <td>30</td>
    </tr>
    <tr>
      <th>7</th>
      <td>EF76 Nebulon-B escort frigate</td>
      <td>Flota Nómada</td>
      <td>4</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Star Destroyer</td>
      <td>Hangar Secreto (Capturado)</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>




### Apartado B: Extracción de datos desde API REST (2.5 puntos)

Necesitamos obtener las especificaciones técnicas oficiales de todas las naves del universo Star Wars.

1.  Utilizando la librería `requests`, realiza peticiones `GET` al endpoint oficial de naves: `https://swapi.dev/api/starships/`.
2.  Carga la lista completa de naves en un único DataFrame de Pandas llamado `df_catalogo`.

*(Nota: Si no consigues hacer funcionar la petición a la API o la paginación, carga el archivo `swapi_starships_simulado.csv` en `df_catalogo` y continúa con el siguiente apartado).*


```python
import requests

response = requests.get("https://swapi.dev/api/starships/")
if response.status_code == 200:
    datos = response.json()
    print("Conexion_Establecida")
else:
     print(f"Error. {response.status_code}")

```

    Conexion_Establecida



```python
datos["next"]
```




    'https://swapi.dev/api/starships/?page=2'




```python
import pandas as pd

df_catalogo = pd.json_normalize(datos["results"])
while datos.get("next"):
    url = datos["next"]
    response = requests.get(url)
    if response.status_code == 200:
        datos = response.json()
        df_1 = pd.json_normalize(datos["results"])
        df_catalogo = pd.concat([df_catalogo, df1], ignore_index=True)

```


```python
df_catalogo.head(1)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>model</th>
      <th>manufacturer</th>
      <th>cost_in_credits</th>
      <th>length</th>
      <th>max_atmosphering_speed</th>
      <th>crew</th>
      <th>passengers</th>
      <th>cargo_capacity</th>
      <th>consumables</th>
      <th>hyperdrive_rating</th>
      <th>MGLT</th>
      <th>starship_class</th>
      <th>pilots</th>
      <th>films</th>
      <th>created</th>
      <th>edited</th>
      <th>url</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CR90 corvette</td>
      <td>CR90 corvette</td>
      <td>Corellian Engineering Corporation</td>
      <td>3500000</td>
      <td>150</td>
      <td>950</td>
      <td>30-165</td>
      <td>600</td>
      <td>3000000</td>
      <td>1 year</td>
      <td>2.0</td>
      <td>60</td>
      <td>corvette</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>2014-12-10T14:20:33.369000Z</td>
      <td>2014-12-20T21:23:49.867000Z</td>
      <td>https://swapi.dev/api/starships/2/</td>
    </tr>
  </tbody>
</table>
</div>



### Apartado C: Transformación y cruce de datos (2.5 puntos)

Ahora debes unificar la información local con la oficial y hacer los cálculos logísticos.

1.  Realiza un cruce entre tu DataFrame del inventario limpio (Apartado A) y el DataFrame del catálogo oficial (Apartado B), utilizando el nombre de la nave como clave de unión.
2.  Crea una nueva columna calculada llamada `capacidad_total_flota`. Esta debe ser el resultado de multiplicar las `naves_disponibles` (de tu inventario) por la carga especificada en la API oficial.


```python
df_union = df_catalogo.merge(df_ejer1, on="name",how="left")
```


```python
df_union.head(10)

df_union = df_union.fillna(0)
```


```python
df_union.head(3)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>model</th>
      <th>manufacturer</th>
      <th>cost_in_credits</th>
      <th>length</th>
      <th>max_atmosphering_speed</th>
      <th>crew</th>
      <th>passengers</th>
      <th>cargo_capacity</th>
      <th>consumables</th>
      <th>hyperdrive_rating</th>
      <th>MGLT</th>
      <th>starship_class</th>
      <th>pilots</th>
      <th>films</th>
      <th>created</th>
      <th>edited</th>
      <th>url</th>
      <th>base_asignada</th>
      <th>naves_disponibles</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CR90 corvette</td>
      <td>CR90 corvette</td>
      <td>Corellian Engineering Corporation</td>
      <td>3500000</td>
      <td>150</td>
      <td>950</td>
      <td>30-165</td>
      <td>600</td>
      <td>3000000</td>
      <td>1 year</td>
      <td>2.0</td>
      <td>60</td>
      <td>corvette</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>2014-12-10T14:20:33.369000Z</td>
      <td>2014-12-20T21:23:49.867000Z</td>
      <td>https://swapi.dev/api/starships/2/</td>
      <td>Base Yavin 4</td>
      <td>15</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Star Destroyer</td>
      <td>Imperial I-class Star Destroyer</td>
      <td>Kuat Drive Yards</td>
      <td>150000000</td>
      <td>1,600</td>
      <td>975</td>
      <td>47,060</td>
      <td>n/a</td>
      <td>36000000</td>
      <td>2 years</td>
      <td>2.0</td>
      <td>60</td>
      <td>Star Destroyer</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>2014-12-10T15:08:19.848000Z</td>
      <td>2014-12-20T21:23:49.870000Z</td>
      <td>https://swapi.dev/api/starships/3/</td>
      <td>Hangar Secreto (Capturado)</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Sentinel-class landing craft</td>
      <td>Sentinel-class landing craft</td>
      <td>Sienar Fleet Systems, Cyngus Spaceworks</td>
      <td>240000</td>
      <td>38</td>
      <td>1000</td>
      <td>5</td>
      <td>75</td>
      <td>180000</td>
      <td>1 month</td>
      <td>1.0</td>
      <td>70</td>
      <td>landing craft</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/films/1/]</td>
      <td>2014-12-10T15:48:00.586000Z</td>
      <td>2014-12-20T21:23:49.873000Z</td>
      <td>https://swapi.dev/api/starships/5/</td>
      <td>0</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>




```python
def limpieza2(valor):
    valor = valor.replace("unknown","0")
    return valor

```


```python
df_union["cargo_capacity"] = df_union["cargo_capacity"].apply(limpieza2)
```


```python
df_union["capacidad_total_flota"] = df_union["naves_disponibles"].astype("int32") * df_union["cargo_capacity"].astype("int32")
```


```python
df_union.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>model</th>
      <th>manufacturer</th>
      <th>cost_in_credits</th>
      <th>length</th>
      <th>max_atmosphering_speed</th>
      <th>crew</th>
      <th>passengers</th>
      <th>cargo_capacity</th>
      <th>consumables</th>
      <th>...</th>
      <th>MGLT</th>
      <th>starship_class</th>
      <th>pilots</th>
      <th>films</th>
      <th>created</th>
      <th>edited</th>
      <th>url</th>
      <th>base_asignada</th>
      <th>naves_disponibles</th>
      <th>capacidad_total_flota</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CR90 corvette</td>
      <td>CR90 corvette</td>
      <td>Corellian Engineering Corporation</td>
      <td>3500000</td>
      <td>150</td>
      <td>950</td>
      <td>30-165</td>
      <td>600</td>
      <td>3000000</td>
      <td>1 year</td>
      <td>...</td>
      <td>60</td>
      <td>corvette</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>2014-12-10T14:20:33.369000Z</td>
      <td>2014-12-20T21:23:49.867000Z</td>
      <td>https://swapi.dev/api/starships/2/</td>
      <td>Base Yavin 4</td>
      <td>15</td>
      <td>45000000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Star Destroyer</td>
      <td>Imperial I-class Star Destroyer</td>
      <td>Kuat Drive Yards</td>
      <td>150000000</td>
      <td>1,600</td>
      <td>975</td>
      <td>47,060</td>
      <td>n/a</td>
      <td>36000000</td>
      <td>2 years</td>
      <td>...</td>
      <td>60</td>
      <td>Star Destroyer</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>2014-12-10T15:08:19.848000Z</td>
      <td>2014-12-20T21:23:49.870000Z</td>
      <td>https://swapi.dev/api/starships/3/</td>
      <td>Hangar Secreto (Capturado)</td>
      <td>2</td>
      <td>72000000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Sentinel-class landing craft</td>
      <td>Sentinel-class landing craft</td>
      <td>Sienar Fleet Systems, Cyngus Spaceworks</td>
      <td>240000</td>
      <td>38</td>
      <td>1000</td>
      <td>5</td>
      <td>75</td>
      <td>180000</td>
      <td>1 month</td>
      <td>...</td>
      <td>70</td>
      <td>landing craft</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/films/1/]</td>
      <td>2014-12-10T15:48:00.586000Z</td>
      <td>2014-12-20T21:23:49.873000Z</td>
      <td>https://swapi.dev/api/starships/5/</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Death Star</td>
      <td>DS-1 Orbital Battle Station</td>
      <td>Imperial Department of Military Research, Sien...</td>
      <td>1000000000000</td>
      <td>120000</td>
      <td>n/a</td>
      <td>342,953</td>
      <td>843,342</td>
      <td>1000000000000</td>
      <td>3 years</td>
      <td>...</td>
      <td>10</td>
      <td>Deep Space Mobile Battlestation</td>
      <td>[]</td>
      <td>[https://swapi.dev/api/films/1/]</td>
      <td>2014-12-10T16:36:50.509000Z</td>
      <td>2014-12-20T21:26:24.783000Z</td>
      <td>https://swapi.dev/api/starships/9/</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Millennium Falcon</td>
      <td>YT-1300 light freighter</td>
      <td>Corellian Engineering Corporation</td>
      <td>100000</td>
      <td>34.37</td>
      <td>1050</td>
      <td>4</td>
      <td>6</td>
      <td>100000</td>
      <td>2 months</td>
      <td>...</td>
      <td>75</td>
      <td>Light freighter</td>
      <td>[https://swapi.dev/api/people/13/, https://swa...</td>
      <td>[https://swapi.dev/api/films/1/, https://swapi...</td>
      <td>2014-12-10T16:59:45.094000Z</td>
      <td>2014-12-20T21:23:49.880000Z</td>
      <td>https://swapi.dev/api/starships/10/</td>
      <td>Flota Nómada</td>
      <td>1</td>
      <td>100000</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 21 columns</p>
</div>



### Apartado D: Almacenamiento y salida (2.5 puntos)

El sistema de Inteligencia de Negocio y el Data Lake requieren que exportes los resultados en dos formatos distintos.

1.  **Capa de consumo (analistas):** exporta el DataFrame final resultante del Apartado C a un archivo CSV llamado `reporte_logistico.csv`. Debes asegurarte de utilizar coma (`,`) como separador y excluir explícitamente el índice numérico de Pandas.
2.  **Capa Plata (Data Lake):** para almacenar el histórico de forma eficiente para la CPU y en almacenamiento en frío, exporta el mismo DataFrame a formato **Apache Parquet**. Llama al archivo `historico_flota.parquet` y aplica compresión `gzip`.


```python
df_union.to_csv("reporte_logistico.csv",sep=",",index= False)
```


```python
df_union["base_asignada"] = df_union["base_asignada"].astype("string")
df_union["naves_disponibles"] = df_union["naves_disponibles"].astype("string")
```


```python
df_union.to_parquet("historico_flota.parquet", compression="gzip")
```


```python
df_union ["capacidad_total_flota"] = df_union["capacidad_total_flota"].astype("object")
```
