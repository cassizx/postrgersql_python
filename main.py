import psycopg2
import os
from datetime import datetime #

#Функция делает запрос к базе для 
def exist_now_table():
    cur.execute("SELECT table_name FROM information_schema.tables  WHERE table_schema='public' ORDER BY table_name")
    resp = cur.fetchall()
    print("Exist now table:")
    for row in resp:
        print (str(row[0]).split(', ') )
    start()
pass

#Функция создании базы, при вызове в качестве аргументы передаётся введённое в start название, сейчас создаёт таблицу с 2 полями 
def new_table(new_tab):
    try:
        query=(f"CREATE TABLE public.{new_tab} ( id int NOT NULL , testcomn varchar NULL)")
        cur.execute(query) #выполнение запроса
        con.commit() # отправка изменений
        print(f'Table {new_tab} created.') 
        to_log_resp = exist_now_table()
        log(to_log_resp, query) #Exist table now:

        start()
    except psycopg2.errors.DuplicateTable:
        table_exist = (f"Table {new_tab} is alredy exist.")
        print(table_exist)
        log(table_exist, query)
        start()
        pass    
pass 

def select_table(query_table):
    con.commit()
    query = (f'select * from {query_table}')
    query_column_name=(f" SELECT column_name FROM information_schema.columns WHERE table_name='{query_table}' " ) #запрос для получения названий колонок в таблице
    try:
        cur.execute(query)
        response = cur.fetchall()
        cur.execute(query_column_name) 
        response_column_name= cur.fetchall() #название колонок в таблице
        column_text_print = (f"Column name:\n{response_column_name}\n=======================================\n")
        print(column_text_print)
        log(response, query)
        for row in response:
            print ( str(row).split(',') )    
        con.commit()
        start()
    except psycopg2.errors.UndefinedTable:
        print('Wrong table name, repeate')
        start()
pass
# Функция выполнения произвольного запроса, принимает параметр введённый в start при вызове, отправляет запрос базе, получает и выводит ответ. 
def his(text):
    query = (f'{text}')
    try:
        cur.execute(query)
        resp = cur.fetchall()
        log(resp, query)
        print (resp)
        #cur.close()
        con.commit()
        start()
    except psycopg2.InterfaceError:
        print('OK')
    #finally:
        print(f'oshibka {cur}')
        start()
pass
#Функция записи лога, вызывается из других функций. Принимает 2 параметра resp - ответ на sql запрос из функции, sql_script -  сам sql запрос 
def log(resp, sql_script='what_do'):
    date = datetime.date(datetime.now())
    file_with_log = (f"F:/python/postgressql/log{date}.txt") # создание названия файла 
    
    write_to_file = open( file_with_log , 'a') # открытие файла лога в режиме a - добавления записи в конец
    write_to_file.write(f"-----Start new query.-----\n")
    write_to_file.write( f"Time: {datetime.now()}  \nQeury: {sql_script} \nRespone:\n")
    for twtr in resp: # Построчно записывает ответ в файл
        write_to_file.write( str(twtr) + '\n')
    write_to_file.write( '-----End of qeury.-----' + '\n')
    write_to_file.close()
pass
#Функция запуска, запрашивает действие у пользователя    
def start():
    print(f"What do you want to do? \n Options: \n select - select * from <Input table name> \n creat - Create new table \n his - Your query \n exist - Show exist tables \n exit - Exit." )
    what_do = str(input())
    if  what_do == 'creat':
        print("New table name? " )
        new_table_name=str(input())
        new_table(new_table_name)
    elif what_do == 'select':
        print('I can do select all, from which table?:')
        select_table_name=str(input())
        select_table(select_table_name)
    elif what_do == 'his':
        print('Input your query:')
        text = str(input())
        his(text)
    elif what_do == 'exit':
        try:
            con.commit()
            con.close()
        except:
            pass
        print('Bye!') 
        exit()
    elif what_do == 'exist':
        exist_now_table()       
    else:
        print('Dont know, repeat input')
        start()
pass
#
if __name__ == "__main__":
    try:
        #Для выполнения запроса к базе, необходимо с ней соединиться и получить курсор.
        con = psycopg2.connect(
        database="dvdrental", 
        user="postgres", 
        password="password@74784", 
        host="109.68.213.220", 
        port="5432"
        )
        #Через курсор происходит дальнейшее общение в базой.
        cur = con.cursor()
        print("Database opened successfully, exist tables:") 
        exist_now_table()
        start()
        pass
    except psycopg2.OperationalError:
        print('Connection eror, check all.')
        pass



   

