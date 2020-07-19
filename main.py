from datetime import datetime 
import os
import psycopg2
from prettytable import PrettyTable 
from prettytable import from_db_cursor


# Возвращает существующи таблицы и  вызывает start()
def exist_now_table():
    qeury=("SELECT table_name FROM information_schema.tables  WHERE table_schema='public' ORDER BY table_name")
    cur.execute(qeury)
    print("Exist now table:")
    resp = from_db_cursor(cur)
    print(resp)
    con.commit()
    log(resp, qeury)
    return start()
pass


#Функция для подсчёта времени выполнения запроса, принимает на вход 2 параметра, time_start - время начала выполнения запроса 
# time_end - время получения ответа, возвращет вывод разницы, то есть примерное аремя выполнения.
def calculation_execution_time(time_start, time_end):
    execution_time = time_end - time_start
    return print(f"Запрос выполнен за {execution_time}")


#Функция создании базы, при вызове в качестве аргументы передаётся введённое в start название, сейчас создаёт таблицу с 2 полями 
def new_table(new_tab):
    query=(f"CREATE TABLE public.{new_tab} ( id serial NOT NULL , testcomn varchar(50) NULL)")
    try:
        time_start_qeury = datetime.now() 
        cur.execute(query)                                                                                          # Выполнение запроса
        con.commit()                                                                                                # Отправка изменений
        time_end_qeury = datetime.now()
    except psycopg2.errors.DuplicateTable as err:
        print(f"Table {new_tab} is already exist.")
        log(err, query)
        con.commit()
        start()
    else:
        to_log_resp = (f'Table {new_tab} created.')
        calculation_execution_time(time_start_qeury, time_end_qeury)
        log(to_log_resp, query)                                                                                    
        print(to_log_resp)
        start()
    pass    
pass 


# Возвращает все колонки из введённой таблицы
def select_table(query_table):
    con.commit()
    query = (f'select * from {query_table}')
    time_start_qeury = datetime.now()                                                                           # Время перед началом выполнения запроса
    try:
        cur.execute(query)
        reqested_table = from_db_cursor(cur) 
        time_end_qeury = datetime.now()                                                                             # Время получения ответа                                                   
    except psycopg2.errors.UndefinedTable as err:
        con.commit()
        print('Wrong table name, try again')
        log(err , query)
        exist_now_table()
    else:
        calculation_execution_time(time_start_qeury, time_end_qeury)            # Вычесление времени выполнения запроса
        log(reqested_table, query)
        print(reqested_table)    
        con.commit()
        start()


#Функция для удаления таблицы
def drop_input_table (table_name):
    try:
        query = (f"drop table {table_name}")
        time_start_qeury = datetime.now()
        cur.execute(query)
        con.commit()     
        resp = (f"Done.")
        time_end_qeury = datetime.now()    
    except psycopg2.errors.UndefinedTable as err:
        log(err, query)         
        print('Wrong table name, try again.')
        con.commit()
        return exist_now_table()
    else:
        calculation_execution_time(time_start_qeury, time_end_qeury)
        print(resp)
        log(resp, query)
        exist_now_table()
pass


# Функция выполнения произвольного запроса, принимает параметр введённый в start при вызове, отправляет запрос базе, получает и выводит ответ. 
def his(reqest):
    query = (f'{reqest}')
    try:
        time_start_qeury = datetime.now()
        cur.execute(query)
        resp = from_db_cursor(cur)
        time_end_qeury = datetime.now()
    except psycopg2.InterfaceError as err :
        print(f'Exception {err}')
        log(err, query)
        con.commit()
        start()
    except psycopg2.ProgrammingError as err: 
        print(f'Exception {err} , try again.')
        if err == 'psycopg2.ProgrammingError: no results to fetch':
            print('Done.')
        log(err, query)
        con.commit()
        start()
    else:
        if resp == None:
            print ('Done.')
            calculation_execution_time(time_start_qeury, time_end_qeury)
        else:
            try:
                calculation_execution_time(time_start_qeury, time_end_qeury)
                print(resp)
            except:
                pass
        log(resp, query)
        con.commit()
        start()   
pass


#Функция записи лога, вызывается из других функций. Принимает 2 параметра resp - ответ на sql запрос из функции, reqest -  сам sql запрос 
def log(resp, reqest='what_do'):
    date = datetime.date(datetime.now())
    file_with_log = (f"log{date}.log")                                                       # Создание названия файла   
    write_to_file = open(file_with_log, 'a')                                                 # Открытие файла лога в режиме a - добавления записи в конец
    write_to_file.write(f"-----Start new query.-----\n")
    write_to_file.write( f"Time: {datetime.now()} \nQeury: {reqest} \nRespone:\n")
    write_to_file.write(str(resp))
    write_to_file.write(f"\n-----End of qeury.----- \n")
    write_to_file.close()
pass


#Функция запуска, запрашивает действие у пользователя    
def start():
    print('What do you want to do? \n Options: \n 1. select - select * from <Input table name> \n 2. create - Create new table') 
    print(' 3. his - Your query \n 4. exist - Show exist tables \n 5. drop - Drop exist table \n 6. q - Exit.')
    print('Enter name or enter number:')
    what_do = str(input())
    if what_do == 'select' or what_do == '1':
        print('I can do select all, from which table?:')
        select_table_name=str(input())
        select_table(select_table_name)
    if  what_do == 'create' or what_do == '2':
        print("Input new table name:" )
        new_table_name=str(input())
        new_table(new_table_name)    
    elif what_do == 'his' or what_do == '3':
        print('Input your query:')
        reqest = str(input())
        his(reqest)
    elif what_do == 'exist' or what_do == '4':
        exist_now_table()     
    elif what_do == 'drop' or what_do == '5':                                     #Удаление таблицы
        print('Which table you want to drop?')
        input_drop_table_name= str(input())                     # Запрос на ввод названия таблицы
        drop_input_table(input_drop_table_name)                 # Вызов функции удаляющей таблицу с введённым названием
    elif what_do == 'q' or what_do == '6': 
        try:
            con.commit()
            con.close()
        except:
            pass
        log(what_do, what_do)
        print('Bye!') 
        exit()
    else:
        print('Dont know, repeat input')
        start()
pass


if __name__ == "__main__":
    try:
        file_with_data = open('connection-data.txt', 'rt')                                       #Открытие файла с реквизитами для подключения.
        read_connection_data = file_with_data.read()
        
        read_connection_data = read_connection_data.split('\n')
        con = psycopg2.connect(                                                           #Для выполнения запроса к базе, необходимо с ней соединиться и получить курсор.
            database = read_connection_data[0], user = read_connection_data[1], password = read_connection_data[2], 
            host = read_connection_data[3], port = read_connection_data[4]
            )        
        cur = con.cursor()                                                      #Через курсор происходит дальнейшее общение в базой.
        resp = ("Database opened successfully.")
        file_with_data.close()
        print(resp)
        log(resp, con)
        print(exist_now_table())                                                # Покажет сущуствующие таблицы и вызовит функцию start, при большом количестве таблиц
        #start() # Вызывается из exist_now_table()                              # закомментировать и расскомментировать start()
        #pass
    except psycopg2.OperationalError as err:
        log(err, read_connection_data)
        print('Connection eror, check data to connect.')
        print(read_connection_data)
        pass
pass


   

