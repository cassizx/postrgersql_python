# postrgerssql_python

Postrgerssql + python psycopg2

Скрипт для подключения и работы с БД postgresssql, для запуска потребуется указать параметры подключения:

database="", 
user="", 
password="", 
host="", 
port="5432"

После подключения покажет существующие таблицы в базе и предложит ввести команду, доступные команды:

1) select - select * from <Input table name>
Обычный select запрос, возвращает все поля в таблице.  

2)creat - Create new table
Создаюёт новую таблицу, названием вводится после выбора этой функции.

3)his - Your query
Введите ваш запрос.

4)exit - Exit.
Выход.
