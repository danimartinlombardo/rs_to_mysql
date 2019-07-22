import psycopg2
import pymysql
from credentials import *

# Get data from Redshift
try:	
	redshift_table_name = 'datawarehouse.ops_dim_agency'
	redshift_conn = psycopg2.connect(dbname= 'dwh', host='cabify-datawarehouse.cxdpjwjwbg9i.eu-west-1.redshift.amazonaws.com', port= '5439', user= rs_user, password= rs_pass)
	redshift_cur = redshift_conn.cursor()
	print ("Check 1")
	redshift_cur.execute('select * from %s;' % redshift_table_name)
	description = redshift_cur.description
	rows = redshift_cur.fetchall()
	print (len(rows))
except psycopg2.Error as e:
	print(str(e))

print ("Check 3")
# Insert data to Mysql
try:
	mysql_table_name = 'ops_dim_agency'
	mysql_conn = pymysql.connect(host='35.195.80.162', port=3306, user=mysql_user, password=mysql_pass, database='GRW_drivers')
	mysql_cur = mysql_conn.cursor()
	print ("Check 4")
	insert_template = 'insert into %s (%s) values %s;'
	column_names = ', '.join([x[0] for x in description])
	values = ', '.join(['(' + ','.join(map(str, x)) + ')' for x in rows])
	print (len(values))
except pymysql.Error as e:
	print(str(e))
print ("Check 6")



