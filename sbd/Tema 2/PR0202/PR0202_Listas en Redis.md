```python
!pip install redis
```

    Requirement already satisfied: redis in /opt/conda/lib/python3.11/site-packages (7.0.1)



```python
#Importamos Redis
import redis
r = redis.Redis(
	host='redis',
	port=6379,
	db=0,
	decode_responses=True
)
import json
```


```python
#Creamos función agregar_pedido
npedido = 0
def agregar_pedido(cliente,producto):
    cadena = ""
    global npedido
    npedido += 1
    pedido = {
        "id": f"pedido_0{npedido}",
        "cliente": cliente,
        "producto": producto,
        "cantidad": 1,
        "urgente": False
    }
    pedido_json = json.dumps(pedido)
    r.rpush("clave_pedidos",pedido_json)

```


```python
#Agregamos 5 pedidos
agregar_pedido("angel","ventilador")
agregar_pedido("pepe","sata")
agregar_pedido("pedro","usb")
agregar_pedido("maria","telefono")
agregar_pedido("guillermo","portatil")

```


```python
#Creamos la función procesar_pedido
def procesar_pedido():
    pedido_json = r.lpop("clave_pedidos")
    listar_pedidos = json.loads(pedido_json)
    print(listar_pedidos)
procesar_pedido()
```

    {'id': 'pedido_01', 'cliente': 'angel', 'producto': 'ventilador', 'cantidad': 1, 'urgente': False}



```python
#Muestra todos los pedidos actuales en la cola con LRANGE
r.lrange("clave_pedidos",0,3)

```




    ['{"id": "pedido_02", "cliente": "pepe", "producto": "sata", "cantidad": 1, "urgente": false}',
     '{"id": "pedido_03", "cliente": "pedro", "producto": "usb", "cantidad": 1, "urgente": false}',
     '{"id": "pedido_04", "cliente": "maria", "producto": "telefono", "cantidad": 1, "urgente": false}',
     '{"id": "pedido_05", "cliente": "guillermo", "producto": "portatil", "cantidad": 1, "urgente": false}']




```python
#Inserta 2 pedidos adicionales (simulando nuevos clientes)
agregar_pedido("uno","sata")
agregar_pedido("dos","usb")
```


```python
#Procesa todos los pedidos de la cola llamando repetidamente a procesar_pedido() hasta que no queden más pedidos
#Sacamos el número de pedidos que tengo, para borrar todos los datos con un bucle
n_pedidos = r.llen("clave_pedidos")
for i in range(0,n_pedidos):
    procesar_pedido()


```


```python
#Nuevo funcion para tener urgentes
npedido = 0
def agregar_pedido(cliente,producto,urgente):
    cadena = ""
    global npedido
    npedido += 1
    if urgente == 0:
        pedido = {
            "id": f"pedido_0{npedido}",
            "cliente": cliente,
            "producto": producto,
            "cantidad": 1,
            "urgente": False
        }
        pedido_json = json.dumps(pedido)
        r.rpush("clave_pedidos",pedido_json)
    else:
        pedido = {
            "id": f"pedido_0{npedido}",
            "cliente": cliente,
            "producto": producto,
            "cantidad": 1,
            "urgente": True
        }
        pedido_json = json.dumps(pedido)
        r.lpush("clave_pedidos",pedido_json)
    
```


```python
#Agregamos varios elementos primero que no tengan urgencia
agregar_pedido("angel","ventilador",0)
agregar_pedido("miguel","usb",0)

```


```python
#Luego con urgencia para ver como se añaden a la izquierda de la lista
agregar_pedido("prueba","prueba",1)
```


```python

```
