pi = 3,14159265359
# pi_format = "O PI é normalmente exibido com %.5f" %(pi)
# print(pi_format)

#Operadores Aritimeticos

print(10 + 20)

numero = 10+20.5
print(numero)

outro_numero = numero + 10
print(outro_numero)

#Operadores Aritimeticos
print(10 + 20)
print(10 - 20)
print(10 * 20)
print(10 / 20)

#Operadores Aritimeticos Diferentes
print(10 // 20)#Divisão Inteira: retorna a 'parte' inteira do resultado da divisao> 3,333 >>> 3
print(10 ** 10)#Esponenciação: elevado ao numero
print(10 % 10)#Modulo: retorna o numero inteiro da divisão

#Aula28:
#Exercicios:001
ano_nascimento = 1996
ano_atual = 2024

print("Idade Atual", ano_atual - ano_nascimento)

#Exercicios:002
num1 = 10
num2 = 15
num3 = 6
media = (num1+num2+num3)/3
print("A média é %f" %(media))

#Exercicios:003
peso = 80.5
altura = 1.72
imc = peso // altura ** 2

print("O IMC é", imc)

#Exercicios:004
ovos = 56
pessoas = 3
print("Tenho inicialmente %d ovos para %d pessoas" %(ovos, pessoas))
ovos_por_pessoa = ovos // pessoas
ovos_restantes = ovos % pessoas
print("Cada uma das %d pessoas ter´ra %d ovo(s)" %(pessoas, ovos_por_pessoa))
print("Restou %d ovos(s) que não puderam ser divididos" %(ovos_restantes))

