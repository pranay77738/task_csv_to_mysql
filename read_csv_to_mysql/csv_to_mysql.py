import csv
import os
import mysql.connector as msql
from mysql.connector import Error
import logging

try:
    # connecting to MySQL server
    conn = msql.connect(host='localhost', user=os.environ.get('user'),
                        password=os.environ.get('pass'), database='mydb')
    if conn.is_connected():
        cursor = conn.cursor()
        # drops the table if already exists in database tables
        cursor.execute('DROP TABLE IF EXISTS info')
        # creating table
        cursor.execute('''CREATE TABLE info(
           Date CHAR(10) NOT NULL,
           Ad_Unit_Name VARCHAR(100) NOT NULL,
           Ad_Unit_ID INT NOT NULL,
           Typetag INT NOT NULL,
           Revenue_Source VARCHAR(100) NOT NULL,
           Market VARCHAR(100) NOT NULL,
           Queries TEXT,
           Clicks SMALLINT,
           Impressions SMALLINT,
           Page_Rpm FLOAT,
           Impression_Rpm FLOAT,
           True_Revenue VARCHAR(10),
           Coverage VARCHAR(10),
           Ctr DOUBLE
        )''')
        try:
            ''' opens the csv file and read through csv lines using the
            delimiter to separate the columns and by using the next it skips
            headers of csv file. loop through the rows using for loop and inserting 
            values into respected columns.
            '''
            with open('code_challenge.csv') as csv_file:
                csvfile = csv.reader(csv_file, delimiter=';')
                header = next(csvfile)
                for row in csvfile:
                    if not ''.join(row).strip():
                        continue
                    else:
                        try:
                            cursor.execute("INSERT INTO info values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                           [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                                            row[9], row[10], row[11], row[12], row[13]])
                            conn.commit()
                        except Error as e:
                            print(e)
        except IOError:
            logging.exception('')
        conn.commit()

except Error as e:
    print("Error while connecting to MySQL", e)

finally:
    if conn.is_connected():
        print("Successfully inserted data into table")
        conn.close()
