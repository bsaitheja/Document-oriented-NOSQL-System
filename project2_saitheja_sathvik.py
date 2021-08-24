import mysql.connector
hname="localhost"
uname="root"
pwd="12345678"
store_db = mysql.connector.connect(host=hname,user=uname,password=pwd)

root = store_db.cursor()
search_list=["CREATE DATABASE  IF NOT EXISTS project2_saitheja_sathvik","SHOW DATABASES"]
root.execute(search_list[0])
print("project2_saitheja_sathvik - database created.")

root.execute(search_list[1])
