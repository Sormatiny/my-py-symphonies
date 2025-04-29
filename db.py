from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.orm import Session
from cryptography.fernet import Fernet


engine = create_engine('postgresql://myuser:123@localhost:5432/mydb') # коннектимся к базе_данных
# вводим имя_дистрибутива, имя пользователя базы данных, пароль пользователя базы данных, хост, порт хоста, имя базы данных
session = Session(engine, future=True)
conn = engine.connect()

key = Fernet.generate_key() #генерим ключ
cipher_suite = Fernet(key) # шифратор

stmt = text("SELECT * FROM Account")  #пишем запрос
r = conn.execute(stmt) # выполняем запрос
#print(r.fetchmany())
for row in r:
    for row2 in row: # итерируем значения
        print("исходное", row2)
        message = row2.encode("utf8") # кодируем в ютф-8
        print(message)
        cipher_text = cipher_suite.encrypt(message) # шифруем
        print(cipher_text)
        plain_text = cipher_suite.decrypt(cipher_text) # расшифруем
        print(plain_text)
