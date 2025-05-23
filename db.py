from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.orm import Session
from cryptography.fernet import Fernet
import pandas as pd

typedb = "postgresql"
server = "localhost"
port = "5432"
dbname = "mydb"
uname = "myuser"
pword = "123"
engine = create_engine(typedb+"://"+uname+":"+pword+"@"+server+":"+port+"/"+dbname)
#engine = create_engine('postgresql://myuser:123@localhost:5432/mydb') # коннектимся к базе_данных
# вводим имя_дистрибутива, имя пользователя базы данных, пароль пользователя базы данных, хост, порт хоста, имя базы данных
session = Session(engine, future=True)
conn = engine.connect()

#key = Fernet.generate_key() #генерим ключ
key = b'OVfnNEp6wekaNckwGDt_MBdw2mL8f4DS-F3ETBTbQc0='
#print(type(key))
key_file = open("key_file.txt", "w+")
key_file.write(str(key))
key_file.close()
print(key)

with open('key_file.txt', 'r') as f:
    datakey = f.read()
print(datakey)
#print(type(datakey))

cipher_suite = Fernet(key) # шифратор

stmt = text("SELECT * FROM Account")  #пишем запрос
r = conn.execute(stmt) # выполняем запрос

#шифруем записи
for row in r:
    for row2 in row: # итерируем значения
        #print("исходное", row2)
        message = row2.encode("utf8") # кодируем в ютф-8
        #print(message)
        cipher_text = cipher_suite.encrypt(message) # шифруем
        #print("шифрованное сообщение" ,cipher_text)
        plain_text = cipher_suite.decrypt(cipher_text) # расшифруем
        plain_text = plain_text.decode("utf8")
        #print("расшифрованное сообщение" ,plain_text)

df = pd.read_sql("SELECT * FROM Account", conn)

df1 = str(df) #делаем строки
df1 = bytes(df1, encoding='utf8') # делаем тип данных - байты
df1 = cipher_suite.encrypt(df1) # шифруем
df4 = cipher_suite.decrypt(df1) # расшифруем
df4 = df4.decode("utf-8") # возвращаем обратно исходный формат
print(df4)
