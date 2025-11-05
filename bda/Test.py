diccionario = {
    "nombre": "Carlos",
    "edAd": 28,
    "ciUdad": "Madrid",
    "prOfesión": "Ingeniero",
    "hobbies": ["leer", "viajar", "correr"],
    "activo": True,
    "salario": 3500.50,
    "fecha_registro": "2023-10-05"
}
dict_aux = {}
for keys, values in diccionario.items():
    cadena_aux = ""
    for caracter in keys:
        if caracter == caracter.lower():
            caracter = caracter.upper()
            cadena_aux += caracter
        else:
            caracter = caracter.lower()
            cadena_aux += caracter
    dict_aux[cadena_aux] = values
print(dict_aux)