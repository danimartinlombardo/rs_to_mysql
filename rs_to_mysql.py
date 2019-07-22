import psycopg2
import pymysql
from credentials import *

# Get data from Redshift
try:	
	redshift_table_name = 'datawarehouse.ops_dim_agency'
	redshift_conn = psycopg2.connect(dbname= 'dwh', host='cabify-datawarehouse.cxdpjwjwbg9i.eu-west-1.redshift.amazonaws.com', port= '5439', user= rs_user, password= rs_pass)
	redshift_cur = redshift_conn.cursor()
	redshift_cur.execute('''
		SELECT
			sk_agency, id_agency, cd_code, ds_slug
		FROM %s
		WHERE
			tm_updated_at > date_trunc('day', DATEADD(day, -90, GETDATE()));'''
		% redshift_table_name)
	description = redshift_cur.description
	rows = redshift_cur.fetchall()
	for row in rows: print (row)
except psycopg2.Error as e:
	print(str(e))

# Insert data to Mysql
try:
	mysql_table_name = 'rs_ops_dim_agency'
	mysql_conn = pymysql.connect(host='35.195.80.162', port=3306, user=mysql_user, password=mysql_pass, database='GRW_drivers')
	mysql_cur = mysql_conn.cursor()
	insert_template = 'INSERT INTO %s (%s) VALUES %s;'
	column_names = ', '.join([x[0] for x in description])
	print(column_names)
	values = ', '.join(["""('""" + """','""".join(map(str, x)) + """')""" for x in rows])
	print (values)
	mysql_cur.execute(insert_template % (mysql_table_name, column_names, values))
	results = mysql_cur.fetchall()
	for result in results: print (result)
except pymysql.Error as e:
	print(str(e))
