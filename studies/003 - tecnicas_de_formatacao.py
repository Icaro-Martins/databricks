# %s texto
# %d inteiro
# %f real/float


ana = "Ana"
texto_formatado = "O nome dela é %s " % (ana)
print(texto_formatado)

rodrigo = "Rodrigo"
idade = 23
altura = 1.73
text_rodrigo = "Meu nome é %s. eu tenho %d anos e tenho %.2f metros de altura" %(rodrigo,idade,altura)
print(text_rodrigo)

valor = True
print("O valor é 's' %s" %(valor))
print("O valor é 'd' %d" %(valor))

decimal = 23.4566
print("A parte ineira é %d" %(decimal))

texto = "Olá, assim que quebra um linha.\n\tentendeu como quebrar um linha?\n\tFim!"
print(texto)



dia, mes, ano = 4, 12, 1980
data = "Eu nasci em %d/%d/%d" % (dia,mes,ano)
print(data)

horas, minutos = 21,53
print("Agora são %d horas e %d minutos." %(horas,minutos))

pi = 3,14159265359
pi_format = "O PI é normalmente exibido com %.5f" %(pi)
print(pi_format)