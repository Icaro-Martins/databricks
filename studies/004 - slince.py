var = "icaro martins"

print(var[0:5])


texto1 = "Olá"
texto2 = ", "
testo3 = "Mundo!"
texto_completo = texto1 + texto2 + testo3
print(texto_completo)

texto_repetido = texto_completo * 3
print(texto_repetido)

print(len(texto_completo))

print(texto_completo[::-1])
print(texto_completo[3:1:-1])

num = "023"
print("1" + num[1:])

print("Olá" in texto_completo)
print("Olá" not in texto_completo)
print("Teste" in texto_completo)
print("Teste" not in texto_completo)

print(True and False or (True and True))