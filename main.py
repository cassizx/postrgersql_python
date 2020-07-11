import psycopg2
import os

#Для выполнения запроса к базе, необходимо с ней соединиться и получить курсор Через курсор происходит дальнейшее общение в базой.
con = psycopg2.connect(
  database="", 
  user="", 
  password="", 
  host="", 
  port="5432"
)
print("Database opened successfully, exist tables:") 
cur = con.cursor()
cur.execute("SELECT table_name FROM information_schema.tables  WHERE table_schema='public' ORDER BY table_name")
resp = cur.fetchall()
con.commit()
for row in resp:
    print (str(row[0]).split(', ') )


file_with_ans = open("F:\/python\/postgressql\/1.txt", 'a') #режим дозаписи - a, w- открытие для перезаписи


#Функция, при вызове создаёт таблицу в базе с переданным названием
def new_table(new_tab):
    query=(f"CREATE TABLE public.{new_tab} ( id int NOT NULL , testcomn varchar NULL)")
    cur.execute(query) #выполнение запроса
    con.commit() # отправка изменений
    print(f'Table {new_tab} created. Exist table now:')
    cur.execute("SELECT table_name FROM information_schema.tables  WHERE table_schema='public' ORDER BY table_name")
    exist_now_tables = cur.fetchall()
    for row in exist_now_tables:
        print (str(row[0]).split(', ') )
       
    start()
#Вызов функции. при вызове в качестве аргументы передаётся введённое название
#new_table(new_table_name)

def select_table(query_table):
    con.commit()
    query = (f'select * from {query_table}')
    query1=(f" SELECT column_name FROM information_schema.columns WHERE table_name='{query_table}' " )
    try:
        cur.execute(query)
        reuqest1= cur.fetchall()
        cur.execute(query1) 
        qw1= cur.fetchall()
        
        qw1_text = (f"Column name:\n{qw1}\n ==============+=============+============")
        #print(f"Column name:\n{qw1}\nContent:" )  #, '\n'
        file_with_ans.write(qw1_text)
        print( qw1_text)

        for row in reuqest1:
            print ( str(row).split(',') )
            text = (f"{ str(row).split(', ') }" )
            file_with_ans.write(text + '\n')
        file_with_ans.write( '-----End of qeury.-----' + '\n')
        #con.commit()
        start()
    except psycopg2.errors.UndefinedTable:
        print('Wrong table name, repeate')
        start()
    #resp = cur.fetchall()
    #print(resp)
    #return con.close()


def his(text):
    query = (f'{text}')
    try:
        cur.execute(query)
        resp = cur.fetchall()
        print (resp)
        #cur.close()
        con.commit()
        start()
    except:
        print('OK')
    finally:
        print(f'oshibka {cur}')
        start()
pass

def start():
    
    print('What do you want to do?' '\n' 'Options:' '\n'  'select - select * from <Input table name>' '\n'  'creat - Create new table' '\n' 'his - Your query' '\n' 'exit - Exit.'  )
    what_do=str(input())

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
        con.commit()
        con.close()
        print('Bye!') 
        exit()   
    else:
        print('Dont know, repeat input')
        start()


if __name__ == "__main__":
    start()

