#Primeros pasos Creando entorno e instalando librerias Requeridas
# virtualenv -p python3 env
#.\env\Scripts\activate   
#py -m pip install --upgrade pip
#pip list
#pip install pyodbc  
#pip install pandas
#pip install matplotlib
#pip install SQLAlchemy

import pyodbc
import pandas as pd
from sqlalchemy import create_engine, event
from sqlalchemy.engine.url import URL
import time

#Conexión Database Origen - SQL server
conexion_prd =("Driver={SQL Server};"
               "Server=(local);"
               "Database=SQLBDSIPA;"
               "UID=user_sql;"
               "PWD=user_sql;")
#--Caso de Windows Authentication, no colocar UID y PWD.
#Trusted_Connection=yes

#Cursor conexión Origen
try:
    conexion_prd=pyodbc.connect(conexion_prd)
    cursor_prd=conexion_prd.cursor()
    cursor_prd.execute("select @@version;")
    row=cursor_prd.fetchone()
    print(row)
    #cursor_prd.execute("Select id, cCodigoTrabajador, cNombres, cApPaterno, cApMaterno, cDireccion, sexo FROM [SQLBDSIPA].[dbo].[GL_TRABAJADOR];")
    #rows=cursor_prd.fetchall()
    #print(rows)

except Exception as ex:
    print(ex)
    quit()

#Conexion Database Destino - SQL Server
conexion_dw =("Driver={SQL Server Native Client 11.0};"
               "Server=(local);"
               "Database=DemoSQL;"
               "UID=user_sql;"
               "PWD=user_sql;")

#Cursor conexión Destino
try:
    conexion_dw=pyodbc.connect(conexion_dw)
    cursor_dw=conexion_dw.cursor()
except Exception as ex:
    print(ex)
    quit()

#Lectura de tabla
connection_string = "Driver={SQL Server};Server=(local);Database=SQLBDSIPA;UID=user_sql;PWD=user_sql;"
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
#from sqlalchemy import create_engine
engine = create_engine(connection_url)

ahora = time.strftime("%c")
print('Ini Lectura:',ahora)

df=pd.read_sql("Select id, cCodigoTrabajador, cNombres, replace(cApPaterno,'''','''''') cApPaterno, cApMaterno, replace(isnull(cDireccion,''),'''','\''') cDireccion, sexo FROM [SQLBDSIPA].[dbo].[GL_TRABAJADOR2]", engine)

ahora = time.strftime("%c")
print('Fin Lectura:',ahora)

#Limpiando Registro tabla Destino
cursor_dw.execute('Truncate table trabajadores')
cursor_dw.commit()

def booleano(bool):
  if bool:
    return 1
  else :
    return 0

ahora = time.strftime("%c")
print('Ini:',ahora)
#Iniciando Estructura e replicación para insertar cada line en tabla destino
for i, id in enumerate(df['id']):
    codigo=df.loc[i,'cCodigoTrabajador']
    nombre=df.loc[i,'cNombres']
    apellidos=df.loc[i,'cApPaterno']+' ' #+ df.loc[i,'cApMaterno']
    direccion=df.loc[i, 'cDireccion']
    sexo=df.loc[i,'sexo']

    script ='Insert into trabajadores ([idTrabajador], [codigo], [nombres], [apellidos], [direccion], [sexo]) '
    datos ='values (' + str(id) + ',\'' + codigo.strip() + '\',\'' + nombre.strip() + '\',\'' + apellidos.strip() + '\',\'' + direccion + '\','+ str(booleano(sexo)) +')'

    query= script+datos
    #print(query)
    cursor_dw.execute(query)
    cursor_dw.commit()

ahora = time.strftime("%c")
print('Fin:',ahora)

#Cerramos las conexiones
conexion_prd.close()
conexion_dw.close()

