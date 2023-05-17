import mysql.connector
from mysql.connector import Error
from configs import connect_config
from faker import Faker

fake = Faker()

def read_table(table_name):
    sql_select_Query = "select * from " + table_name
    cursor.execute(sql_select_Query)
    record = cursor.fetchall()
    print("Total number of rows in table: ", len(record))
    i = 0
    while i < len(record):
        print(record[i], '\r')
        i+=1

def create_table():
    try:
        create_table_query = """CREATE TABLE Love_players (
                            Id int(11) NOT NULL,
                             Name varchar(250) NOT NULL,
                             Club varchar(250),
                             PRIMARY KEY (Id))"""
        cursor.execute(create_table_query)

    except Error as error:
        print("Failed to create table in MySQL: {}".format(error))

def insert_row(table_name, id, player_name, club):
    try:
        insert_query = "insert into " + table_name + " values (%s, %s, %s)"
        param = (id, player_name, club)
        cursor.execute(insert_query, param)
        connection.commit()


    except Error as e:
        print("Failed to create table in MySQL: {}".format(e))

def fill_table_with_fake_data():
    i = 3
    number_rows = 5
    number_columns = 3
    insert_query = create_query(number_columns, 'love_players')

    while i < number_rows:
        parameters = []
        parameters.insert(0, i)
        parameters.insert(1, fake.name())
        parameters.insert(2, fake.company())
        cursor.execute(insert_query, parameters)
        i += 1

    #connection.commit()



def create_query(number, table_name):
    new_query = "insert into " + table_name + " values (%s"
    i = 1
    while i < number:
        new_query += ", %s"
        i += 1

    new_query += ");"
    return new_query

def delete_row():
    sql_delete_query = "delete from love_players where id = 3"
    cursor.execute(sql_delete_query)


try:
    connection = mysql.connector.connect(**connect_config)
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        #cursor.execute("select database();")
        db = connect_config['database']
        cursor.execute("use " + db + ";")

        fill_table_with_fake_data()
        delete_row()
        read_table('love_players')

        #create_table()

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
