import psycopg2
from sys import argv

def check_date(conn):
    cursor = conn.cursor()
    sql = """update  cyclones_history 
        set end_date = to_date('29991231', 'yyyymmdd')
        where end_date = (select max(date) - INTERVAL '1 DAY' from cyclones_history_tmp)
        """
    cursor.execute(sql)
    sql = """
    delete from cyclones_history where from_date =  (select max(date) from cyclones_history_tmp)
    """
    cursor.execute(sql)

    conn.commit()


def insert_line(conn, line):
    cursor = conn.cursor()
    tmp = line.split("\t")
    insert = f"insert into cyclones_history_tmp (id, date, status) values ({tmp})".replace("[", "").replace("]", "")
    cursor.execute(insert)
    conn.commit()


def merge_table(conn):
    cursor = conn.cursor()
    sql = """
        update cyclones_history trg
        set end_date = (select max(date) - INTERVAL '1 DAY' from cyclones_history_tmp)
        where (trg.id ,trg.status)  not in (select id, status  from cyclones_history_tmp) 
                and trg.end_date > (select max(date) from cyclones_history_tmp)
        """
    cursor.execute(sql)
    conn.commit()

    sql = """
        merge into cyclones_history trg
        using cyclones_history_tmp as ch
        on trg.id = ch.id and trg.status = ch.status
        when  not matched  then 
            insert values(ch.date, to_date('29991231', 'yyyymmdd'), ch.id, ch.status)
    """
    cursor.execute(sql)
    conn.commit()
    sql = """truncate table public.cyclones_history_tmp"""
    cursor.execute(sql)
    conn.commit()
def insert_to_psg(path_file):
    try:
        conn = psycopg2.connect(
            host="0.0.0.0",
            port="5432",
            database="postgres",
            user="postgres",
            password="root")

        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE if NOT EXISTS PUBLIC.cyclones_history_tmp(
        id varchar(10),
        date date,
        status varchar(2));
        """)
        conn.commit()
        with open(path_file, "r") as reader:
            temp = reader.read().split("\n")
            for line in temp:
                if line:
                    insert_line(conn, line)
            conn.commit()
        check_date(conn)
        merge_table(conn)
        cursor.execute("drop table public.cyclones_history_tmp")
        conn.commit()
    finally:
        conn.close()


if __name__ == '__main__':
    try:
        name, path_file = argv
    except ValueError:
        path_file = "file_csv/cyclones_20130605.csv"
    insert_to_psg(path_file)