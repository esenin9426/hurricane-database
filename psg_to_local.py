from sys import argv
import psycopg2

conn = psycopg2.connect(
    host="0.0.0.0",
    port="5432",
    database="postgres",
    user="postgres",
    password="root")

def download_to_local(sql_where):
    try:
        cursor = conn.cursor()

        sql = f"""
        with 
        count_status as(
        select c.id, to_char(c.date, 'yyyy-mm-dd') date, c.status,c.time,
        row_number() over (partition by c.id, c.date order by c.date, c.time desc) rn
        from cyclones c
        where {sql_where})
        
        select c.id, c.date, c.status from count_status c
        where rn = 1"""

        cursor.execute(sql)
        while True:
            tmp = cursor.fetchone()
            if tmp:
                file = open(f"file_csv/cyclones_{tmp[1].replace('-', '')}.csv", 'a')
                file.write("\t".join(tmp) + "\n")
            else:
                file.close()
                break
    finally:
        conn.close()

if __name__ == '__main__':
    try:
        name, sql_where = argv
    except ValueError:
        sql_where = "date >= '2013-01-01'"
    download_to_local(sql_where)
