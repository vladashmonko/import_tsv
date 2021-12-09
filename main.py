import csv
import mysql.connector

database = mysql.connector.connect(host="localhost", user="user123", password="qwerty", database="mydb", port=3306)
cursor = database.cursor()


def save_data_from_file(schema, table, columns_count):
    data_file_path = schema + '\\' + table + '.tsv'
    params_format = '(' + ('%s, ' * columns_count)[:-2] + ')'
    with open('D:\\Genesis\\sandbox.KNU\\' + data_file_path) as file:
        tsv_file = csv.reader(file, delimiter="\t")
        for line in tsv_file:
            sql_query = 'INSERT INTO ' + table + ' VALUES' + params_format
            cursor.executemany(sql_query, line)


save_data_from_file('dictionary', 'activity_type', 2)
save_data_from_file('dictionary', 'context', 2)
save_data_from_file('dictionary', 'service_type', 2)

save_data_from_file('marketing', 'orders', 7)
save_data_from_file('marketing', 'subchannels_cost', 3)
save_data_from_file('marketing', 'subscription_status', 3)

save_data_from_file('marketing', 'orders', 7)
save_data_from_file('marketing', 'subchannels_cost', 3)
save_data_from_file('marketing', 'subscription_status', 3)

database.commit()
cursor.close()
database.close()
