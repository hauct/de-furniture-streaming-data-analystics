def return1_2():
    a = 1
    b = 2
    return a, b

dic = {'a':return1_2()[0], 'b':return1_2()[1]}
print(dic)