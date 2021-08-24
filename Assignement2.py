import xml.etree.cElementTree as data_var_str
from pymongo import MongoClient
import mysql.connector
import csv as fetchcsv
import json
from tabulate import tabulate
from pprint import pprint as align
import xml.dom.minidom

print("1. Localhost")
print("2. 127.0.0.1")
hname=input("Please select the hostname : ")
if hname==1:
  store_db = mysql.connector.connect(host="localhost",user="root",password="12345678",database="project2_saitheja_sathvik")
else:
  store_db = mysql.connector.connect(host="127.0.0.1",user="root",password="12345678",database="project2_saitheja_sathvik")

root = store_db.cursor()
def drop_all_tables():
  droplist=["DROP TABLE IF EXISTS DEPARTMENT","DROP TABLE IF EXISTS EMPLOYEE","DROP TABLE IF EXISTS PROJECT","DROP TABLE IF EXISTS WORKS_ON"]
  for i in range(len(droplist)):
    root.execute(droplist[i])

def create_all_tables():
  root.execute("CREATE TABLE DEPARTMENT (Branch_title VARCHAR(123), Branch_nmr INT(50) PRIMARY KEY, M_g_social_n VARCHAR(123), Mgr_start_date VARCHAR(123))")
  with open('process_txt_files/DEPARTMENT.txt') as capture:
      store_process_file = fetchcsv.reader(capture)
      for data_in_S_P_F in store_process_file:
        root.execute(f"INSERT INTO DEPARTMENT (Branch_title, Branch_nmr, M_g_social_n, Mgr_start_date) VALUES ({data_in_S_P_F[0]},{data_in_S_P_F[1]}, {data_in_S_P_F[2]},{data_in_S_P_F[3]})")
  store_db.commit()

  root.execute("CREATE TABLE EMPLOYEE (F_S_T_nme VARCHAR(123), M_I_T VARCHAR(123), L_n_e VARCHAR(123), Social_S_n VARCHAR(123) PRIMARY KEY, D_O_B VARCHAR(123),STAY_ADD VARCHAR(123), Gen_dr VARCHAR(123), Package_of_the_employess INT(50), S_E_R_Social_N VARCHAR(123),D_num_er INT(50))")
  with open('process_txt_files/EMPLOYEE.txt', "rt") as capture:
      store_process_file = fetchcsv.reader(capture)
      for data_in_S_P_F in store_process_file:
        root.execute(f"INSERT INTO EMPLOYEE (F_S_T_nme, M_I_T, L_n_e, Social_S_n, D_O_B, STAY_ADD, Gen_dr, Package_of_the_employess, S_E_R_Social_N, D_num_er) VALUES ({','.join(data_in_S_P_F)})")     
  store_db.commit()

  root.execute("CREATE TABLE PROJECT (pro_j_nme VARCHAR(123), pro_j_nbr VARCHAR(123) PRIMARY KEY, pro_j_lcn VARCHAR(123), D_nbr VARCHAR(123))")
  with open('process_txt_files/PROJECT.txt') as capture:
      store_process_file = fetchcsv.reader(capture)
      for data_in_S_P_F in store_process_file:
        root.execute(f"INSERT INTO PROJECT (pro_j_nme, pro_j_nbr, pro_j_lcn, D_nbr) VALUES ({','.join(data_in_S_P_F)})")
  store_db.commit()

  root.execute("CREATE TABLE WORKS_ON (Emp_yee_Social_sn VARCHAR(123), Proj_nbrs INT(50) , nbr_of_hrs FLOAT, PRIMARY KEY (Emp_yee_Social_sn, Proj_nbrs))")
  with open('process_txt_files/WORKS_ON.txt') as capture:
      store_process_file = fetchcsv.reader(capture)
      for data_in_S_P_F in store_process_file:
        root.execute(f"INSERT INTO WORKS_ON (Emp_yee_Social_sn, Proj_nbrs, nbr_of_hrs) VALUES ({','.join(data_in_S_P_F)})")
  store_db.commit()

def to_display_in_terminal():
  root.execute("SELECT projt.pro_j_nme, projt.pro_j_nbr, deprt.Branch_title, data_var_str.L_n_e, data_var_str.F_S_T_nme, w.nbr_of_hrs FROM PROJECT projt ,DEPARTMENT deprt ,EMPLOYEE data_var_str,WORKS_ON w WHERE projt.D_nbr = deprt.Branch_nmr AND data_var_str.D_num_er = deprt.Branch_nmr AND data_var_str.Social_S_n = w.Emp_yee_Social_sn AND projt.pro_j_nbr = w.Proj_nbrs ORDER BY projt.pro_j_nme")
  sql_output = root.fetchall()
  f1 = open("Tables/Project", "w+")
  f1.write(tabulate(sql_output, headers=['Project_Name', 'Project_No','Name_of_the_Branch','Last_name_of_Employee','First_name_of_Employee','nbr_of_hrs'], tablefmt='psql'))
  root.execute("SELECT JSON_OBJECT('Project_Name', projt.pro_j_nme,'Project_No', projt.pro_j_nbr,'Name_of_the_Branch', deprt.Branch_title,'Employees', JSON_ARRAYAGG( JSON_OBJECT('Last_name_of_Employee', data_var_str.L_n_e,'First_name_of_Employee',data_var_str.F_S_T_nme,'nbr_of_hrs', w.nbr_of_hrs))) as projects FROM PROJECT projt ,DEPARTMENT deprt ,EMPLOYEE data_var_str,WORKS_ON w WHERE projt.D_nbr = deprt.Branch_nmr AND data_var_str.D_num_er = deprt.Branch_nmr AND data_var_str.Social_S_n = w.Emp_yee_Social_sn AND projt.pro_j_nbr = w.Proj_nbrs GROUP BY projt.pro_j_nme, projt.pro_j_nbr, deprt.Branch_title ORDER BY projt.pro_j_nme")
  sql_output = root.fetchall()
  list_proj_j_son_formate = [json.loads(value_in_proj_dct[0]) for value_in_proj_dct in sql_output]
  with open('json/Projects.json', 'w') as Open_file_as:
    Open_file_as.write(json.dumps(list_proj_j_son_formate, sort_keys=True, indent=4))
  

  pjt_x_m_l_fomt = bytes()
  for data_in_j_son in list_proj_j_son_formate: 
    var_store = data_var_str.Element("Project")
    data_var_str.SubElement(var_store,"Project_Name").text = data_in_j_son["Project_Name"]
    data_var_str.SubElement(var_store,"Project_No").text = data_in_j_son["Project_No"]
    data_var_str.SubElement(var_store,"Name_of_the_Branch").text = data_in_j_son["Name_of_the_Branch"]
    project = data_var_str.SubElement(var_store,"Employees")
    for value_in_json in data_in_j_son["Employees"]:
      data_var_str.SubElement(project,"First_name_of_Employee").text = value_in_json["First_name_of_Employee"]
      data_var_str.SubElement(project,"Last_name_of_Employee").text = str(value_in_json["Last_name_of_Employee"])
      data_var_str.SubElement(project,"nbr_of_hrs").text = str(value_in_json["nbr_of_hrs"])
    pjt_x_m_l_fomt = pjt_x_m_l_fomt + data_var_str.tostring(var_store)

  zero_one_fle = open("xml/xml_file_pro.xml", "wb")
  zero_one_fle.write(b"<Projects>")
  zero_one_fle.write(pjt_x_m_l_fomt)
  zero_one_fle.write(b"</Projects>")
  zero_one_fle.close()

  root.execute("SELECT data_var_str.L_n_e, data_var_str.F_S_T_nme,deprt.Branch_title,projt.pro_j_nme, projt.pro_j_nbr,w.nbr_of_hrs FROM EMPLOYEE data_var_str, DEPARTMENT deprt ,PROJECT projt , WORKS_ON w WHERE data_var_str.D_num_er = deprt.Branch_nmr AND projt.D_nbr = deprt.Branch_nmr AND w.Emp_yee_Social_sn = data_var_str.Social_S_n AND projt.pro_j_nbr = w.Proj_nbrs ORDER BY data_var_str.F_S_T_nme AND data_var_str.L_n_e")
  sql_output = root.fetchall()
  f1 = open("Tables/Employee", "w+")
  f1.write(tabulate(sql_output, headers=['Last_name_of_Employee', 'First_name_of_Employee','Name_of_the_Branch','Project_Name','Project_No','nbr_of_hrs'], tablefmt='psql'))
  root.execute("SELECT JSON_OBJECT('Last_name_of_Employee', data_var_str.L_n_e,'First_name_of_Employee', data_var_str.F_S_T_nme,'Name_of_the_Branch', deprt.Branch_title,'Projects', JSON_ARRAYAGG( JSON_OBJECT('Project_Name', projt.pro_j_nme,'Project_No', projt.pro_j_nbr ,'nbr_of_hrs', w.nbr_of_hrs ))) as employees FROM EMPLOYEE data_var_str, DEPARTMENT deprt ,PROJECT projt , WORKS_ON w WHERE data_var_str.D_num_er = deprt.Branch_nmr AND projt.D_nbr = deprt.Branch_nmr AND w.Emp_yee_Social_sn = data_var_str.Social_S_n AND projt.pro_j_nbr = w.Proj_nbrs GROUP BY data_var_str.L_n_e, data_var_str.F_S_T_nme, deprt.Branch_title ORDER BY data_var_str.L_n_e AND data_var_str.F_S_T_nme")
  sql_output = root.fetchall()
  list_emp_j_son_formate =[json.loads(value_in_eoe_dct[0]) for value_in_eoe_dct in sql_output]

  with open('json/Employees.json', 'w') as Open_file_as:
    Open_file_as.write(json.dumps(list_emp_j_son_formate, sort_keys=True, indent=4))
  

  emp_x_m_l_fomt = bytes()
  for data_in_j_sor in list_emp_j_son_formate: 
    var_store = data_var_str.Element("Employee")
    data_var_str.SubElement(var_store,"Last_name_of_Employee").text = data_in_j_sor["Last_name_of_Employee"]
    data_var_str.SubElement(var_store,"First_name_of_Employee").text = data_in_j_sor["First_name_of_Employee"]
    data_var_str.SubElement(var_store,"Name_of_the_Branch").text = data_in_j_sor["Name_of_the_Branch"]
    project = data_var_str.SubElement(var_store,"Projects")
    for value_in_json in data_in_j_sor["Projects"]:
      data_var_str.SubElement(project,"Project_Name").text = value_in_json["Project_Name"]
      data_var_str.SubElement(project,"Project_No").text = str(value_in_json["Project_No"])
      data_var_str.SubElement(project,"nbr_of_hrs").text = str(value_in_json["nbr_of_hrs"])
    emp_x_m_l_fomt = emp_x_m_l_fomt + data_var_str.tostring(var_store)

  zero_one_fle = open("xml/xml_file_emp.xml", "wb")
  zero_one_fle.write(b"<Employees>")
  zero_one_fle.write(emp_x_m_l_fomt)
  zero_one_fle.write(b"</Employees>")
  zero_one_fle.close()

  connect_clt = MongoClient('mongodb://localhost:27017/')
  link_Mo_go = connect_clt.db2Assignment2_saitheja_sathvik
  print("DATABASE CREATED")
  Mo_go_coll = link_Mo_go["projects"]
  Mo_go_coll.delete_many({})
  [Mo_go_coll.insert_one(data_in_list) for data_in_list in list_proj_j_son_formate]

  
  eoe_coll = link_Mo_go["employees"]
  eoe_coll.delete_many({})
  [eoe_coll.insert_one(data_in_list) for data_in_list in list_emp_j_son_formate]

  root.execute("SELECT deprt.Branch_title, deprt.Branch_nmr, m.L_n_e, m.F_S_T_nme,data_var_str.L_n_e, data_var_str.F_S_T_nme, data_var_str.Package_of_the_employess FROM DEPARTMENT deprt INNER JOIN EMPLOYEE m ON deprt.M_g_social_n = m.Social_S_n INNER JOIN EMPLOYEE data_var_str ON data_var_str.D_num_er = deprt.Branch_nmr ORDER BY deprt.Branch_title, m.L_n_e, m.F_S_T_nme, data_var_str.L_n_e, data_var_str.F_S_T_nme")
  sql_output = root.fetchall()
  f1 = open("Tables/Department", "w+")
  f1.write(tabulate(sql_output, headers=['Name_of_the_Branch', 'Branch_nbr','Last_name_of_manager','First_name_of_manager','Last_name_of_Employee','First_name_of_Employee','Package_of_the_employess'], tablefmt='psql'))
  root.execute("SELECT JSON_OBJECT('Name_of_the_Branch', deprt.Branch_title,'Dpt_nmr', deprt.Branch_nmr,'Mag_begin_period_d',deprt.Mgr_start_date,'Last_name_of_manager',m.L_n_e,'First_name_of_manager', m.F_S_T_nme,'Employees', JSON_ARRAYAGG(JSON_OBJECT('Last_name_of_Employee',data_var_str.L_n_e,'First_name_of_Employee', data_var_str.F_S_T_nme,'Package_of_the_employess', data_var_str.Package_of_the_employess))) as departments FROM DEPARTMENT deprt INNER JOIN EMPLOYEE m ON deprt.M_g_social_n = m.Social_S_n INNER JOIN EMPLOYEE data_var_str ON data_var_str.D_num_er = deprt.Branch_nmr GROUP BY deprt.Branch_title, deprt.Branch_nmr, m.L_n_e, m.F_S_T_nme")
  sql_output = root.fetchall()
  d_p_t_j_on =[json.loads(value_in_dpt_dct[0]) for value_in_dpt_dct in sql_output]
  with open('json/Departments.json', 'w') as Open_file_as:
    Open_file_as.write(json.dumps(d_p_t_j_on, sort_keys=True, indent=4))

  dpt_coll = link_Mo_go["departments"]
  dpt_coll.delete_many({})
  [dpt_coll.insert_one(data_in_list) for data_in_list in d_p_t_j_on]

  # 1
  print("1: Fetch all the employee first name  from the employe document ORDER BY the first name ")
  documents = link_Mo_go.employees.find( {},{'First_name_of_Employee':1}  ).sort("First_name_of_Employee",1)
  for d in documents:
    align(d)
 
  #2
  print("2: Number of projects in each Department")
  documents = link_Mo_go.projects.aggregate([
        {"$group" : {'_id':"$Name_of_the_Branch",'Total':{'$sum':1}}}
    ])
  for d in documents:
    align(d)


  #3
  print("3: Get all the employess working in dept 4")
  documents = link_Mo_go.departments.find( { 'Dpt_nmr':4} ) 
  for d in documents:
    align(d)
  

  #4
  print("4: Fetch all project from EMPLOYEES where employee 'Joyce' is working on and dept 'Research'")
  documents = link_Mo_go.employees.find( { 'First_name_of_Employee': 'Joyce','Name_of_the_Branch':'Research'},{'First_name_of_Employee':1,'Name_of_the_Branch':1,'Projects':1})
  for d in documents:
    align(d)

  #5
  print("5: Fetch employee names under admin department from department collection")
  documents = link_Mo_go.departments.find( { 'Name_of_the_Branch':'Software'},{'Employees':1}) 
  for d in documents:
    align(d)

  #6
  print("6: Get first,project number, working for  EMPLOYEES collection where department name is Software")
  documents = link_Mo_go.employees.find( { 'Name_of_the_Branch': 'Software'},{'First_name_of_Employee':1,'Projects.nbr_of_hrs':1,'Projects.Project_No':1} )
  for d in documents:
    align(d)

  dom = xml.dom.minidom.parse("xml/xml_file_emp.xml")
  pretty_xml_as_string = dom.toprettyxml()
  f2=open("formated_xml/employess_xml",'w+')
  for i in pretty_xml_as_string:
    f2.write(i)

  


  store_db.close()
  connect_clt.close()

def pretty_xml():
  dom = xml.dom.minidom.parse("xml/xml_file_emp.xml")
  pretty_xml_as_string = dom.toprettyxml()
  f2=open("formated_xml/employess_xml",'w+')
  for i in pretty_xml_as_string:
    f2.write(i)

  dom2=xml.dom.minidom.parse("xml/xml_file_pro.xml")
  pretty_xml_as_string2 = dom2.toprettyxml()
  f3=open("formated_xml/xml_file_pro",'w+')
  for j in pretty_xml_as_string2:
    f3.write(j)


if __name__ == '__main__':
  drop_all_tables()
  create_all_tables()
  to_display_in_terminal()
  pretty_xml()


