# PR0202: Cadenas en Redis
### ------------- ESPECIALIZACIÓN EN INTELIGENCIA ARTIFICIAL Y BIG DATA -------------
---------------------------------------------------------------------------------

Módulo:                     SISTEMAS DE BIG DATA
Profesor:                   Víctor J. González
Unidad de Trabajo:          UT02. Almacenamiento de datos
Práctica:                   PR0201: Cadenas en Redis
Resultados de aprendizaje:  RA3


## 1. Trabajo con Redis CLI

# 2. Trabajo con Python
Crea un script en Python que realice las siguientes operaciones con la librería redis. Realiza todos estos pasos en un Notebook de Jupyter y luego descárgalo como Markdown para entregar la práctica.

Inserta la clave app:version con el valor "1.0".

Recupera y muestra el valor de app:version.

Modifica el valor de app:version a "1.1".

Crea la clave contador:descargas con valor 0.

Incrementa en 5 el valor de contador:descargas.

Decrementa en 2 el valor de contador:descargas.

Inserta la clave app:estado con el valor "activo".

Cambia el valor de app:estado a "mantenimiento".

Inserta la clave mensaje:bienvenida con el texto "Hola alumno".

Establece un tiempo de expiración de 30 segundos para la clave app:estado.

Verifica si la clave app:estado todavía existe después de unos segundos.

Elimina la clave app:version y muestra un mensaje confirmando su eliminación.


!pip install redis


```python
import redis
r = redis.Redis(
	host='redis',
	port=6379,
	db=0,
	decode_responses=True
)
```


```python
print("Inserta la clave app:version con el valor 1.0." )
```

    Inserta la clave app:version con el valor 1.0.



```python
r.set("app:version","1.0")
```




    True




```python
print("Recupera y muestra el valor de app:version.")
```

    Recupera y muestra el valor de app:version.



```python
r.get("app:version")
```




    '1.0'




```python
print("Modifica el valor de app:version a 1.1.")
```

    Modifica el valor de app:version a 1.1.



```python
r.set("app:version","1.1")
```




    True




```python
r.get("app:version")
```




    '1.1'




```python
print("Crea la clave contador:descargas con valor 0.")
```

    Crea la clave contador:descargas con valor 0.



```python
r.set("contador:descargas",0)
```




    True




```python
print("Incrementa en 5 el valor de contador:descargas.")
```

    Incrementa en 5 el valor de contador:descargas.



```python
r.incr("contador:descargas")
```




    1




```python
r.incr("contador:descargas")
```




    2




```python
r.incr("contador:descargas")
```




    3




```python
r.incr("contador:descargas")
```




    4




```python
r.incr("contador:descargas")
```




    5




```python
print("Decrementa en 2 el valor de contador:descargas.")

```

    Decrementa en 2 el valor de contador:descargas.



```python
r.decr("contador:descargas")
```




    4




```python
r.decr("contador:descargas")
```




    3




```python
print("Inserta la clave app:estado con el valor activo.")
```

    Inserta la clave app:estado con el valor activo.



```python
r.set("app:estado","activo")
```




    True




```python
r.get("app:estado")
```




    'activo'




```python
print("Cambia el valor de app:estado a mantenimiento.")
```

    Cambia el valor de app:estado a mantenimiento.



```python
r.set("app:estado","mantenimiento")
```




    True




```python
r.get("app:estado")
```




    'mantenimiento'




```python
print("Inserta la clave mensaje:bienvenida con el texto Hola alumno.")
```

    Inserta la clave mensaje:bienvenida con el texto Hola alumno.



```python
r.set("mensaje:bienvenida","Hola alumno")
```




    True




```python
r.get("mensaje:bienvenida")
```




    'Hola alumno'




```python
print("Establece un tiempo de expiración de 30 segundos para la clave app:estado.")
```

    Establece un tiempo de expiración de 30 segundos para la clave app:estado.



```python
r.expire("app:estado",30) 
```




    True




```python
print("Verifica si la clave app:estado todavía existe después de unos segundos.")
```

    Verifica si la clave app:estado todavía existe después de unos segundos.



```python
r.ttl("app:estado")
```




    26




```python
r.ttl("app:estado")
```




    -2




```python
print("Elimina la clave app:version y muestra un mensaje confirmando su eliminación.")
```

    Elimina la clave app:version y muestra un mensaje confirmando su eliminación.



```python
r.delete("app:version")
```




    1

