

import dotenv
import os
import requests
from Components.Webreq import Webreq
from Components.Database import get_engine,get_engine_from_settings,get_session
import time
import numpy as np
import pandas as pd
import logging
import unidecode
pd.options.mode.chained_assignment = None  # default='warn'



def replace_columns(df,dict_repl):
    """
    Esta funcion reemplaza los caracteres de string tipo español i.e: á, ñ, ó, etc.
    Y renombra las columnas segun un diccionario.

    Parametros
    ----------
    df : Pandas DataFrame
    
    dict_repl : dictionary
    
    Returns
    -------
    int
        1 or 0
    """
    for i in df.columns:
        df.columns=df.columns.str.replace(i,unidecode.unidecode(i))
    for old, new in dict_repl.items():
        df.columns=df.columns.str.replace(old,new)

def sql_upload_table(df,eng,tablename : str):
    """
    Esta funcion sube a una dataframe conexion sql una tabla y le asigna el nombre a la tabla y una columna de la fecha de actualización

    Parametros
    ----------
    df : Pandas DataFrame
    
    eng : Engine de conexion a sql server
    
    tablename : str

    """
    foo=str(pd.to_datetime('today').strftime("%d/%m/%Y"))
    df.loc['fecha_actualizacion']=foo
    df.to_sql(name=tablename,con=eng,index=False,if_exists='replace')
    

def settings_config(valname:str,value:str):
    """
    Esta funcion modifica los valores de settings del archivo de variables de entorno dado un keyname de entrada

    Parametros
    ----------
    valname : str
    
    value : str
    
    """
    try:
        foo = os.getenv("%s"%valname)

        if foo:
            logging.info(f"Se procede a Modificar Valor de variable de entorno %s {foo=}")
            pass
        else:
            logging.info(f"No Se procede a Modificar Valor de variable de entorno %s porque no existe {foo=}")
            pass
        
        
        dotenv_file = dotenv.find_dotenv()
        dotenv.load_dotenv(dotenv_file)
        os.environ[valname] = value
        dotenv.set_key(dotenv_file, valname, os.environ[valname])
    except:
        raise exception("")

def pregunta_inicio(question:str,positiva:str):
    """
    Esta funcion Crear pregunta modificable y de respuesta YES/NO

    Parametros
    ----------
    question : str
    
    positiva : str
    
    Returns
    -------
    int
        1 or 0
    """
    i = 0
    while i < 2:
        answer = input("%s(Y/N)"%(question))
        if any(answer.lower() == f for f in ["yes", 'y', '1', 'ye']):
            print("%s"%(positiva))
            time.sleep(2.5)
            return 1
            break
        elif any(answer.lower() == f for f in ['no', 'n', '0']):
            print("No")
            time.sleep(2)
            return 0
            break
        else:
            i += 1
            if i < 2:
                print('Por favor introduce YES ó No')
                time.sleep(1)
            else:
                print("Nada se ha hecho")
                time.sleep(1)

                
def rev_url(url:str):
    """
    Esta funcion revisar si URL ingresada existe
    
    Empleando la libreria requests verifica si existe conexion con el input ingresado.

    Parametros
    ----------
    url : str
    
    Returns
    -------
    int
        1 or 0

    """
    try:
        response = requests.get(url)
        foo=np.arange(200,300)
        if response.status_code == 200:
            print('La Pagina Web Existe')
            return 1
        else:
            print('La Pagina Web No Existe')
            return 0
    except:
        pass
def loop_input_url(tipo:str):
    while True:
            url_tipo=str(input("Introduce la url de %s"%tipo))
            foo=rev_url(url_tipo)
            time.sleep(2.5)
            if foo==1:
                break
    return url_tipo
    
def a():
    """
    
    """
    
    if pregunta_inicio("¿Desea Actualizar URL Predeterminada?","Entendido!")==1:
        
        url_museos=loop_input_url("Museos")
        url_cines=loop_input_url("Cines")
        url_biblio=loop_input_url("Bibliotecas")
        
                
    else:
        url_museos='https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_4207def0-2ff7-41d5-9095-d42ae8207a5d'

        url_cines='https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_392ce1a8-ef11-4776-b280-6f1c7fae16ae'

        url_biblio='https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_01c6c048-dbeb-44e0-8efa-6944f73715d7'
        
    if pregunta_inicio("¿Desea Modificar el Archivo de Variables de Entorno para ServidorSQL?","Entendido!")==1:
        keys=['USER_NAME','PASSWORD','HOST_NAME','PORT_ID','DATABASE_NAME']
        for i in keys:
            settings_value=str(input("Introduce el valor deseado para %s"%i))
            settings_config(i,settings_value)
    


    w1=Webreq(url_museos,"museos")
    w2=Webreq(url_cines,"cines")
    w3=Webreq(url_biblio,"bibiotecas")

    df_museos,fm1,fm2,fm3=w1.requests_downloadurl()
    w1.make_dirs(df_museos,fm1,fm2,fm3)

    df_cines,fc1,fc2,fc3=w2.requests_downloadurl()
    w2.make_dirs(df_cines,fc1,fc2,fc3)

    df_biblio,fb1,fb2,fb3=w3.requests_downloadurl()
    w3.make_dirs(df_biblio,fb1,fb2,fb3)



    dict_columns={'cod_loc':'cod_localidad',
                  'idprovincia':'id_provincia',
                  'iddepartamento':'id_departamento',
                  'direccion':'domicilio',
                  'cp':'codigo postal',
                  'telefono':'numero de telefono'}        

    df_museos.columns=df_museos.columns.str.lower()
    df_cines.columns=df_cines.columns.str.lower()
    df_biblio.columns=df_biblio.columns.str.lower()

    replace_columns(df_cines,dict_columns)
    replace_columns(df_museos,dict_columns)
    replace_columns(df_biblio,dict_columns)

    Column_selection=pd.Series(['cod_localidad','id_provincia','id_departamento','categoria','provincia','localidad','nombre','domicilio','codigo postal','numero de telefono','mail','web'])
    df_final=pd.concat((df_museos,df_cines,df_biblio),ignore_index=True)
    df_final=df_final.replace('Nan',np.nan)

    df_normalizada=df_final[Column_selection]

    df_conjuntos_reg=df_final[['provincia','categoria','fuente']].value_counts()

    tabla_cines=df_cines[['provincia','pantallas','butacas','espacio_incaa']].replace('Nan',np.nan)
    
    engine=get_engine_from_settings()
    
    sql_upload_table(df_normalizada,engine,'Normalizada')
    
    sql_upload_table(df_conjuntos_reg,engine,'Conjuntos')
    sql_upload_table(tabla_cines,engine,'Tabla_Cines')
    engine.dispose()
    

if __name__ == '__main__':
    a()