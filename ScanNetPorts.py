import nmap as nm #ввод библиотеки нмап
import json #ввод библиотеки для обработки джейсона

with open('data1.json', 'w'): #очистим файл после выполнения предыдущей итерации
    pass

with open('data2.json', 'w'): #очистим файл после выполнения предыдущей итерации
    pass

with open('compare.txt', 'w'): #очистим файл после выполнения предыдущей итерации
    pass

begin = int(input("Введите начальный порт сканирования: ")) # с какого порта начинаем сканировать
end = int(input("Введите конечный порт сканирования: ")) # каким портом заканчиваем сканировать

target = '127.0.0.1' # ввод айпи адреса сканирования

scanner = nm.PortScanner() # фиксирование функции сканирования
with open('data1.json', 'a') as op1: # открыть и записать в файл
    for i in range(begin ,end +1): # вводим цикл для сканирования
        res = scanner.scan(target ,str(i)) # сканируем порты
        res = res['scan'][target]['tcp'][i]['state']
        print(json.dumps(f'port {i} is {res}.'), file= op1)
op1.close() # закроем файл с результатами первого сканирования

# если нало сюда вьебём временную задержку

scanner2 = nm.PortScanner() # фиксирование функции сканирования
with open('data2.json', 'a') as op2: # открыть и записать в файл
    for i in range(begin ,end +1): # вводим цикл для сканирования
        res = scanner.scan(target ,str(i)) # сканируем порты
        res = res['scan'][target]['tcp'][i]['state']
        print(json.dumps(f'port {i} is {res}.'), file= op2)
op2.close() # закроем файл с результатами второго сканирования

fin1=open("data1.json","r") # откроем файл с результатами первого сканирования
fin2=open("data2.json","r") # откроем файл с результатами второго сканирования
for line1 in fin1:          # запустим перебор первого файла в цикле
    for line2 in fin2:      # запустим перебор второго файла в цикле
        if line1==line2:    # сравниваем построчно, исходя из логики программы, потому что у нас последовательное сканирование
            ()              # данный вывод нас не интересует, поэтому пустой
        else:               # пишем в алярмный файл, фактуру алярма
            with open('compare.txt', 'a') as op3:
                print("Alarm", file=op3)
            with open('compare.txt', 'a') as op4:
                print("\tОтличия_состояния_порта_первого_сканирования:", line1, file=op4)
            with open('compare.txt', 'a') as op5:
                print("\tОтличия_состояния_порта_второго_сканирования:", line2, file=op5)
        break                # прерывание цикла,чтоб только первая строчка со всеми не сравнивалась
