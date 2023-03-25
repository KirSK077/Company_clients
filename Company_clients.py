import psycopg2
from psycopg2 import Error


def drope_db(conn):
    # Функция удаления базы данных
    cursor = conn.cursor()
    cursor.execute("""
        DROP TABLE IF EXISTS phone_numbers;
        DROP TABLE IF EXISTS clients;
        """)
    conn.commit()
    return 'База данных удалена'

def create_db(conn):
    # Функция создания базы данных
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE IF EXISTS phone_numbers;
        DROP TABLE IF EXISTS clients;
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS clients(
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(30) NOT NULL,
            last_name VARCHAR(30) NOT NULL,
            email VARCHAR(45) UNIQUE
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS phone_numbers(
            id SERIAL PRIMARY KEY,
            phone_number INTEGER UNIQUE,
            client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE
        );
        """)
        conn.commit()
    return 'База данных создана'

def add_client(conn, id, first_name, last_name, email, phone_number=None):
    # Функция добавления нового клиента
    try:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO clients(id, first_name, last_name, email) VALUES(%s, %s, %s, %s)
            """, (id, first_name, last_name, email))
            cur.execute("""
            INSERT INTO phone_numbers(phone_number, client_id) VALUES(%s, %s)
            """, (phone_number, id))
            conn.commit()
    except psycopg2.errors.UniqueViolation:
        print(f'Почта {email} уже существует в БД, укажите другую')
    else:
        print(f"Данные внесены: {first_name} {last_name}, {email}, {phone_number}")

def add_phone_number(conn, id, phone_number):
    # Функция добавления телефона для существующего клиента
    try:
        with conn.cursor() as cur:
            cur.execute("""
            DO $$
            BEGIN
            if exists (SELECT * FROM phone_numbers WHERE phone_number is NULL AND client_id = %s) THEN 
            UPDATE phone_numbers SET phone_number = %s WHERE client_id = %s;
            ELSE
            INSERT INTO phone_numbers (phone_number, client_id) VALUES (%s, %s);
            END if;
            END $$;
            """, (id, phone_number, id, phone_number, id))
            conn.commit()
    except psycopg2.errors.UniqueViolation:
        print(f'Номер телефона {phone_number} уже существует в БД, укажите другой')
    else:
        print(f"Данные внесены: {phone_number}")

def change_client(conn, id, first_name=None, last_name=None, email=None, phone_number=None):
    # Функция, позволяющая изменить данные о клиенте.
    if first_name is not None:
        with conn.cursor() as cur:
            cur.execute("""
            UPDATE clients SET first_name = %s
            WHERE id = %s;
            """, (first_name, id))
            conn.commit()
        print("Имя клиента измененено")
    if last_name is not None:
        with conn.cursor() as cur:
            cur.execute("""
            UPDATE clients SET last_name = %s
            WHERE id = %s;
            """, (last_name, id))
            conn.commit()
        print("Фамилия клиента измененена")
    if email is not None:
        with conn.cursor() as cur:
            cur.execute("""
            UPDATE clients SET email = %s
            WHERE id = %s;
            """, (email, id))
            conn.commit()
        print("Электронная почта клиента измененена")
    if phone_number is not None:
        with conn.cursor() as cur:
            cur.execute("""
            UPDATE phone_numbers SET phone_number = %s
            WHERE id IN (SELECT id FROM phone_numbers WHERE client_id = %s LIMIT 1);
            """, (phone_number, id))
            conn.commit()
        print("Номер телефона изменен")

def delete_phone_number(conn, phone_number):
    # Функция удаления телефона для существующего клиента (полное удаление строчки из БД)
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phone_numbers WHERE phone_number=%s
        """, (phone_number,))
        conn.commit()
    print(f"Данные удалены: {phone_number}")

def delete_client(conn, id):
    # Функция удаления данных клиента
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM clients WHERE id=%s
        """, (id,))
        conn.commit()
    print("Данные удалены") 

def find_client(conn, first_name=None, last_name=None, email=None, phone_number=None):
    # Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.
    if phone_number is not None:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT first_name, last_name, email, phone_number 
              FROM clients c  
              JOIN phone_numbers pn 
                ON pn.client_id = c.id  
            WHERE phone_number=%s
            """, (phone_number,))
            conn.commit()
            print(*cur.fetchall())
    else:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT first_name, last_name, email, phone_number 
              FROM clients c  
              JOIN phone_numbers pn 
                ON pn.client_id = c.id  
                WHERE (first_name=%s or last_name=%s or email=%s)
            """, (first_name, last_name, email))
            conn.commit()
            print(*cur.fetchall())


password = 'mynWck2ssG'
with psycopg2.connect(database="Company_clients", user="postgres", password=password) as conn:
    # print(drope_db(conn))
    print(create_db(conn))
    add_client(conn, 1, "Alex", "Ivanov", "AlexIvanov@gmail.com", 123456)
    add_client(conn, 2, "Pavel", "Petrov", "PPetrov@mail.ru")
    add_client(conn, 3, "Pavel", "Kuznetsov", "kuznetc999@yandex.ru")
    add_phone_number(conn, 1, 11111)
    add_phone_number(conn, 2, 22222)
    # delete_phone_number(conn, 987654)
    # delete_client(conn, 1)
    change_client(conn, 1, first_name='Mikhail', last_name='Potapov', email='Medved123@gmail.com', phone_number=999)
    find_client(conn, first_name='Pavel')

