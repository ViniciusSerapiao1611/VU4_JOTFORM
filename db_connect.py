import pyodbc
import time
import logging
from configuracoes import Servidor,UID,PWD


def db_coletaotimizada():
    max_retries = 3
    retry_count = 0
    timeout = 30 
    
    while retry_count < max_retries:
        try:
            conn = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};'
                f'SERVER={Servidor};'
                'DATABASE=ENORSIG_JOTFORMV1;'
                f'UID={UID};'
                f'PWD={PWD};'
                'TIMEOUT=' +str(timeout) +';'
                'CONNECTION TIMEOUT=' +str(timeout) + ';'
            )
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            logging.info("Conexão com o banco de dados estabelecida.")
            return conn
        except pyodbc.Error as e:
            retry_count += 1
            logging.error(f"Tentativa {retry_count} de {max_retries} falhou: {e}")
            if retry_count >= max_retries:
                raise Exception("Erro ao conectar com o servidor após várias tentativas. Tente novamente mais tarde.")
            time.sleep(5)  
    
def db_enorfrota():
    max_retries = 3
    retry_count = 0
    timeout = 30 
    
    while retry_count < max_retries:
        try:
            conn = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};'
                f'SERVER={Servidor};'
                'DATABASE=BD_ENORFROTA;'
                f'UID={UID};'
                f'PWD={PWD};'
                'TIMEOUT=' + str(timeout) + ';'
                'CONNECTION TIMEOUT=' + str(timeout) + ';'
            )
            
            # Testar a conexão
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            
            logging.info("Conexão com o banco de dados estabelecida.")
            return conn
        except pyodbc.Error as e:
            retry_count += 1
            logging.error(f"Tentativa {retry_count} de {max_retries} falhou: {e}")
            if retry_count >= max_retries:
                raise Exception("Erro ao conectar com o servidor após várias tentativas. Tente novamente mais tarde.")
            time.sleep(5)  

def db_vu4():
    max_retries = 3
    retry_count = 0
    timeout = 30 
    
    while retry_count < max_retries:
        try:
            conn = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};'
                f'SERVER={Servidor};'
                'DATABASE=APP_GUARULHOS4;'
                f'UID={UID};'
                f'PWD={PWD};'
                'TIMEOUT=' +str(timeout) +';'
                'CONNECTION TIMEOUT=' +str(timeout) + ';'
            )
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            print("\nConexão com o banco de dados estabelecida.")
            return conn
        except pyodbc.Error as e:
            retry_count += 1
            print(f"Tentativa {retry_count} de {max_retries} falhou: {e}")
            if retry_count >= max_retries:
                raise Exception("Erro ao conectar com o servidor após várias tentativas. Tente novamente mais tarde.")
            time.sleep(5)  
 