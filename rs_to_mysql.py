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
	print("Successfully loaded "+str(len(rows))+" regions")
except psycopg2.Error as e:
	print("ERROR loading regions: "+str(e))

# Insert data to Mysql
try:
	mysql_table_name = 'rs_ops_dim_agency'
	mysql_conn = pymysql.connect(host='35.195.80.162', port=3306, user=mysql_user, password=mysql_pass, database='GRW_drivers')
	mysql_cur = mysql_conn.cursor()
	column_names = ', '.join([x[0] for x in description])
	values = ', '.join(["""('""" + """','""".join(map(str, x)) + """')""" for x in rows])
	mysql_cur.execute('''
		DELETE FROM %s;
		INSERT INTO %s (%s)
		VALUES %s;'''
		% (mysql_table_name, mysql_table_name, column_names, values))
	mysql_conn.commit()
	results = mysql_cur.fetchall()
	print("Successfully inserted "+str(len(values))+" regions")
except pymysql.Error as e:
	print("ERROR inserting regions: "+str(e))
