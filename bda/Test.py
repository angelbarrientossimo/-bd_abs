productos = {
    "Electrónica": ["Smartphone", "Laptop", "Tablet", "Auriculares", "Smartwatch"],
    "Hogar": ["Aspiradora", "Microondas", "Lámpara", "Sofá", "Cafetera"],
    "Ropa": ["Camisa", "Pantalones", "Chaqueta", "Zapatos", "Bufanda"],
    "Deportes": ["Pelota de fútbol", "Raqueta de tenis", "Bicicleta", "Pesas", "Cuerda de saltar"],
    "Juguetes": ["Muñeca", "Bloques de construcción", "Peluche", "Rompecabezas", "Coche de juguete"],
}
contador_seccion = 0
contador_productos = 0
for iterable in productos:
    print("Elementos en la seccion de", productos[iterable]," ",len(productos[iterable]))
    contador_seccion += 1
    for iterable_productos in productos[iterable]:
        contador_productos += 1
print("Numero de secciones",contador_seccion,"Numero de productos en total", contador_productos)

