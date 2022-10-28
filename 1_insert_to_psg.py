import psycopg2

conn = psycopg2.connect(
    host="0.0.0.0",
    port="5432",
    database="postgres",
    user="postgres",
    password="root")
try:
    cursor = conn.cursor()
    sql = """
        CREATE TABLE if NOT EXISTS PUBLIC.cyclones (
                                 ID varchar(10),
                                 Name varchar(10),
                                 Date date,
                                 Time integer,
                                 Event varchar(5),
                                 Status varchar(2),
                                 Latitude varchar(10),
                                 Longitude varchar(10),
                                 MaximumWind integer,
                                 MinimumPressure integer,
                                 LowWindNE integer,
                                 LowWindSE integer,
                                 LowWindSW integer,
                                 LowWindNW integer,
                                 ModerateWindNE integer,
                                 ModerateWindSE integer,
                                 ModerateWindSW integer,
                                 ModerateWindNW integer,
                                 HighWindNE integer,
                                 HighWindSE integer,
                                 HighWindSW integer,
                                 HighWindNW integer);
        """
    cursor.execute(sql)
    conn.commit()
    file = "atlantic.csv"
    file_open = open(file, "r")
    column_names = ','.join(file_open.readline().replace(" ", "").split(',')).replace("\n", "")
    while True:
        temp = file_open.readline().replace(" ", "").replace("\n", "")
        if temp:
            temp = temp.split(",")
            sql = f"""insert into cyclones 
            ({column_names})
            values ({temp})""".replace("[", "").replace("]", "")
            try:
                cursor.execute(sql)
            except:
                print(sql)

        else:
            break
    conn.commit()
finally:
    file_open.close()
    conn.close()