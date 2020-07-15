import os
from datetime import datetime #
import psycopg2


# Возвращает существующи таблицы и  вызывает start()
def exist_now_table():
    qeury=("SELECT table_name FROM information_schema.tables  WHERE table_schema='public' ORDER BY table_name")
    cur.execute(qeury)
    resp = cur.fetchall()
    print("Exist now table:")
    for row in resp:
       print (str(row).strip('()').strip(',').join('||'))
    con.commit()
    log(resp, qeury)
    return start()
pass


#Функция создании базы, при вызове в качестве аргументы передаётся введённое в start название, сейчас создаёт таблицу с 2 полями 
def new_table(new_tab):
    try:
        query=(f"CREATE TABLE public.{new_tab} ( id serial NOT NULL , testcomn varchar(50) NULL)")
        cur.execute(query)                                                                                          # Выполнение запроса
        con.commit()                                                                                                # Отправка изменений
        to_log_resp = (f'Table {new_tab} created.') 
        log([to_log_resp], query)                                                                                    
        print(to_log_resp)
        start()
    except psycopg2.errors.DuplicateTable as err:
        table_exist = (f"Table {new_tab} is alredy exist.")
        print(table_exist)
        log([table_exist,err], query)
        start()
        pass    
pass 


# Возвращает все колонки из введённой таблицы
def select_table(query_table):
    con.commit()
    query = (f'select * from {query_table}')
    query_column_name=(f" SELECT column_name FROM information_schema.columns WHERE table_name='{query_table}' " )   #запрос для получения названий колонок в таблице
    try:
        time_start_qeury = datetime.now()                                                                           # Время перед началом выполнения запроса
        cur.execute(query)
        response = cur.fetchall()
        cur.execute(query_column_name) 
        response_column_name= cur.fetchall()                                                                        # Название колонок в таблице
        time_end_qeury = datetime.now()                                                                             # Время получения ответа
        time_execution_qeury = (time_end_qeury - time_start_qeury)                                                  # Вычесление времени выполнения запроса
        print(f"Запрос выполнен за {time_execution_qeury}")
        column_text_print = (f"Column name:\n{str(response_column_name).strip('[]').strip('').join('||')}\n{ ( '==========' * len(response_column_name) ) }")
        print(column_text_print)
        log(response, query)
        for row in response:
            print ( str(row).strip('()').strip(',').join('||') )    #.strip('()') / .split(',')
        con.commit()
        start()
    except psycopg2.errors.UndefinedTable as err:
        print('Wrong table name, repeate')
        err_to_log = [err]                                                                                            # Костыль, что бы в логе не "размазывало" побуквенно
        log(err_to_log, query_column_name)
        start()
pass


#Функция для удаления таблицы
def drop_input_table(table_name):
    try:
        query = (f"drop table {table_name}")
        cur.execute(query)
        con.commit()     
        resp =  f"Done."    
        log([resp], query)
        print(resp)
        start()       
    except psycopg2.errors.UndefinedTable:
        log(["psycopg2.errors.UndefinedTable"], query)          # Костыль, что бы цикл в функции log не размазывал слово побуквенно
        print('Wrong table name, repeate')
        con.commit()
        return exist_now_table()
pass


# Функция выполнения произвольного запроса, принимает параметр введённый в start при вызове, отправляет запрос базе, получает и выводит ответ. 
def his(text):
    query = (f'{text}')
    try:
        cur.execute(query)
        resp = cur.fetchall()
        log(resp, query)
        print (resp)
        con.commit()
        start()
    except psycopg2.InterfaceError as err :
        print('OK')
    #finally:
        print(f'oshibka {err}')
        start()
    except psycopg2.ProgrammingError as err: 
        print(f'oshibka {err}')
        if err == 'psycopg2.ProgrammingError: no results to fetch':
            print('Done')
        log([err], query)
        start()   
pass


#Функция записи лога, вызывается из других функций. Принимает 2 параметра resp - ответ на sql запрос из функции, sql_script -  сам sql запрос 
def log(resp, sql_script='what_do'):
    date = datetime.date(datetime.now())
    file_with_log = (f"log{date}.log")                                                       # Создание названия файла   
    write_to_file = open(file_with_log, 'a')                                                 # Открытие файла лога в режиме a - добавления записи в конец
    write_to_file.write(f"-----Start new query.-----\n")
    write_to_file.write( f"Time: {datetime.now()}  \nQeury: {sql_script} \nRespone:\n")
    write_to_file.write(f"test {len(resp)}\n")
    if len(resp) > 1 or len(resp) == 0 :                                                     # Проверка на размер, если одна строка, то запись не в цикле
        for log_pesp_text in resp:                                                           # Построчно записывает ответ в файл
            write_to_file.write( str(log_pesp_text).strip('()').strip(',').join('||') + '\n') 
    else:
        write_to_file.write( str(resp[0]).strip('()').strip(',').join('||') + '\n')                                            # Костыль, что бы цикл не размазывал слово побуквенно
    write_to_file.write( '-----End of qeury.-----' + '\n')
    write_to_file.close()
pass


#Функция запуска, запрашивает действие у пользователя    
def start():
    print(f"What do you want to do? \n Options: \n select - select * from <Input table name> \n creat - Create new table \n his - Your query \n exist - Show exist tables \n drop - Drop exist table \n exit - Exit." )
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
    elif what_do == 'exist':
        exist_now_table()     
    elif what_do == 'drop':                                     #Удаление таблицы
        print('Which table you want to drop?')
        input_drop_table_name= str(input())                     # Запрос на ввод названия таблицы
        drop_input_table(input_drop_table_name)                 # Вызов функции удаляющей таблицу с введённым названием
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
    try:                                                                        #Для выполнения запроса к базе, необходимо с ней соединиться и получить курсор.
        con = psycopg2.connect(     
            database="dvdrental", user="postgres", password="password@74784", 
            host="109.68.213.220", port="5432"
            )        
        cur = con.cursor()                                                      #Через курсор происходит дальнейшее общение в базой.
        print("Database opened successfully.") 
        print(exist_now_table())                                                # Покажет сущуствующие таблицы и вызовит функцию start, при большом количестве таблиц
        #start() # Вызывается из exist_now_table()                              # закомментировать и расскомментировать start()
        pass
    except psycopg2.OperationalError:
        print('Connection eror, check all.')
        pass
pass


   

