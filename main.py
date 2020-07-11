import psycopg2
import os
from datetime import datetime #

#Для выполнения запроса к базе, необходимо с ней соединиться и получить курсор Через курсор происходит дальнейшее общение в базой.
con = psycopg2.connect(
  database="dvdrental", 
  user="postgres", 
  password="password@74784", 
  host="109.68.213.220", 
  port="5432"
)
print("Database opened successfully, exist tables:") 
cur = con.cursor()
cur.execute("SELECT table_name FROM information_schema.tables  WHERE table_schema='public' ORDER BY table_name")
resp = cur.fetchall()
con.commit()
for row in resp:
    print (str(row[0]).split(', ') )
pass
#file_with_ans = open("F:\/python\/postgressql\/1.txt", 'a') #режим дозаписи - a, w- открытие для перезаписи
#
#Функция, при вызове создаёт таблицу в базе с переданным названием
def new_table(new_tab):
    try:
        query=(f"CREATE TABLE public.{new_tab} ( id int NOT NULL , testcomn varchar NULL)")
        cur.execute(query) #выполнение запроса
        con.commit() # отправка изменений
        print(f'Table {new_tab} created. Exist table now:')
        cur.execute("SELECT table_name FROM information_schema.tables  WHERE table_schema='public' ORDER BY table_name")
        exist_now_tables = cur.fetchall()
        log(exist_now_tables, query)

        for row in exist_now_tables:
            print (str(row[0]).split(', ') )     

        start()
    except psycopg2.errors.DuplicateTable:
        table_exist = (f"Table {new_tab} is alredy exist.")
        print(table_exist)
        log(table_exist, query)
        start()
        pass    
pass    
#Вызов функции. при вызове в качестве аргументы передаётся введённое название
#new_table(new_table_name)
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
        log(response, query_table)
        for row in response:
            print ( str(row).split(',') )    
        con.commit()
        start()
    except psycopg2.errors.UndefinedTable:
        print('Wrong table name, repeate')
        start()
pass

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

def log(resp, sql_script='what_do'):
    date = datetime.date(datetime.now())
    #print( (f"Test:  F:/python/postgressql/log{date}.txt") )
    file_with_log = (f"F:/python/postgressql/log{date}.txt")
    #print(file_with_log)
    write_to_file = open( file_with_log , 'a') 
    write_to_file.write(f"-----Start new query.-----")
    write_to_file.write( f"Time: {datetime.now()}  \n Qeury: {sql_script} \n Respone: \n")
    for twtr in resp:
        write_to_file.write( str(twtr) + '\n')
    write_to_file.write( '-----End of qeury.-----' + '\n')
    write_to_file.close()
pass
    
def start():
    print(f"What do you want to do? \n Options: \n select - select * from <Input table name> \n creat - Create new table \n his - Your query \n exit - Exit." )
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
    else:
        print('Dont know, repeat input')
        start()
pass

if __name__ == "__main__":
    start()

