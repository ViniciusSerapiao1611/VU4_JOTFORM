import pandas as pd
import time
import os
import datetime
import pyodbc
import threading
import logging
from driver import configurar_driver
from db_connect import db_vu4
from configuracoes import c_download_dir
from renomear_colunas import renomear_colunas
import math

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)

lock = threading.Lock()

def infer_sql_type(col):
    col = col.upper()
    if "DATA" in col or "DATE" in col:
        return "DATE"
    if "NUMERO" in col or "QT" in col or "METRO" in col or "LATA" in col:
        return "INT"
    if "VALOR" in col or "RENDA" in col or "LEITURA" in col:
        return "NUMERIC(10,2)"
    return "VARCHAR(255)"

def limpar_valor_numeric(valor):
    try:
        if valor is None:
            return None
        v = float(str(valor).strip().replace(',', '.'))
        if math.isnan(v) or math.isinf(v):
            return None
        v = round(v, 2)
        return v
    except:
        return None

def limpar_valores_antes_insert(dado, colunas_ordenadas):
    for col in colunas_ordenadas:
        v = dado.get(col)
        if isinstance(v, str) and v.strip() == "":
            dado[col] = None
        tipo = infer_sql_type(col)
        if tipo == "INT" and dado[col] is not None:
            try:
                dado[col] = int(float(dado[col]))
            except:
                dado[col] = None
        elif tipo.startswith("NUMERIC") and dado[col] is not None:
            dado[col] = limpar_valor_numeric(dado[col])
        elif tipo == "DATE" and dado[col] is not None:
            try:
                data = pd.to_datetime(dado[col], errors='coerce', dayfirst=True)
                if pd.isna(data) or data.year < 1900:
                    dado[col] = None
                else:
                    dado[col] = data
            except:
                dado[col] = None

def validar_valor(valor, tipo):
    try:
        if valor is None or str(valor).strip() == "":
            return True
        if tipo == "INT":
            valor = float(str(valor).strip().replace(",", "."))
            return valor.is_integer()
        elif tipo.startswith("NUMERIC"):
            valor = limpar_valor_numeric(valor)
            return valor is not None
        elif tipo == "DATE":
            val = pd.to_datetime(valor, errors='coerce', dayfirst=True)
            return pd.notnull(val) and val.year >= 1900
        return True
    except:
        return False

def worker(dados_chunk, colunas_ordenadas, thread_id, lista_erros, lista_imports, inserts_counter, ja_existem_counter):
    conn = db_vu4()
    cursor = conn.cursor()
    hoje = datetime.datetime.now()

    for dado in dados_chunk:
        try:
            cursor.execute("SELECT 1 FROM TAB_VU4 WHERE ID_JOTFORM = ?", (dado.get("ID_JOTFORM"),))
            if cursor.fetchone():
                with lock:
                    ja_existem_counter[0] += 1
                continue

            limpar_valores_antes_insert(dado, colunas_ordenadas)

            valores = []
            for col in colunas_ordenadas:
                valor = dado.get(col)
                tipo = infer_sql_type(col)

                if valor is None or (isinstance(valor, float) and (pd.isna(valor) or str(valor).lower() in ['nan', 'inf'])):
                    valores.append(None)
                else:
                    valor_str = str(valor).strip()
                    if tipo == "VARCHAR(255)" and len(valor_str) > 255:
                        print(f"[Thread {thread_id}] ⚠️ Valor muito longo em '{col}', cortando: {valor_str[:50]}...")
                        valor_str = valor_str[:255]
                    valores.append(valor_str)

            colunas_sql = ", ".join(f"[{col}]" for col in colunas_ordenadas)
            placeholders = ", ".join(["?"] * len(colunas_ordenadas))

            sql = f"""
                INSERT INTO TAB_VU4 ({colunas_sql}, [DATA_IMPORTACAO], [PDF_URL])
                VALUES ({placeholders}, ?, ?)
            """
            valores += [hoje.strftime('%Y-%m-%d %H:%M:%S'), ""]

            try:
                cursor.execute(sql, valores)
                conn.commit()
                print(f"[Thread {thread_id}] ✅ Inserido: {dado.get('ID_JOTFORM')}")
                with lock:
                    inserts_counter[0] += 1
                    lista_imports.append({
                        'ID': dado.get('ID_JOTFORM'),
                        'Data_Importacao': hoje,
                        'Status': 'Importacao com sucesso',
                        'Thread': thread_id
                    })
                cursor.execute("SELECT COUNT(*) FROM TAB_VU4")
                print("Total atual na tabela:", cursor.fetchone()[0])


            except Exception as e:
                conn.rollback()
                print(f"[Thread {thread_id}] ❌ ERRO ao inserir {dado.get('ID_JOTFORM')}: {e}")
                with lock:
                    lista_erros.append({
                        'ID': dado.get('ID_JOTFORM'),
                        'Erro': str(e),
                        'Thread': thread_id
                    })

        except Exception as e:
            print(f"[Thread {thread_id}] ❌ ERRO ao processar {dado.get('ID_JOTFORM')}: {e}")
            with lock:
                lista_erros.append({
                    'ID': dado.get('ID_JOTFORM'),
                    'Erro': str(e),
                    'Thread': thread_id
                })
            conn.rollback()

    conn.close()

def vu4():
    url = "https://www.jotform.com/excel/251754253573056"
    driver = configurar_driver()
    driver.get(url)
    time.sleep(10)
    download_file = None
    while download_file is None:
        time.sleep(5)
        files = os.listdir(c_download_dir)
        for file in files:
            if file.startswith("APP VU IV") and file.endswith(".xlsx"):
                download_file = os.path.join(c_download_dir, file)
                driver.quit()

    logging.info(f"Arquivo baixado: {download_file}")
    df = pd.read_excel(download_file, sheet_name="Submissions")
    df = renomear_colunas(df)
    # Verifica colunas que possuem valores maiores que 255 caracteres
    colunas_longas = {}
    for col in df.columns:
        if df[col].dtype == object:
            grandes = df[df[col].astype(str).str.len() > 255]
            if not grandes.empty:
                colunas_longas[col] = grandes[[col]].copy()

    if colunas_longas:
        print("⚠️ Colunas com dados maiores que 255 caracteres:")
        for col, valores in colunas_longas.items():
            print(f"➡️ {col}: {len(valores)} registros excedendo")
            print(valores.head(1))  # Mostra os 3 primeiros só pra espiar
    else:
        print("✅ Nenhuma coluna com valor acima de 255 caracteres.")

    dados = df.to_dict(orient="records")
    colunas_ordenadas = list(df.columns)

    n_threads = 10
    chunk_size = len(dados) // n_threads + 1

    lista_erros = []
    lista_imports = []
    inserts_counter = [0]
    ja_existem_counter = [0]

    threads = []
    for i in range(n_threads):
        chunk = dados[i*chunk_size:(i+1)*chunk_size]
        t = threading.Thread(target=worker, args=(chunk, colunas_ordenadas, i, lista_erros, lista_imports, inserts_counter, ja_existem_counter))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    logging.info(f"Importacao finalizada. Inseridos: {inserts_counter[0]}, Ja existem: {ja_existem_counter[0]}, Erros: {len(lista_erros)}")

    if lista_erros:
        df_erros = pd.DataFrame(lista_erros)
        nome_arquivo_erros = f"erros_vu4_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        df_erros.to_excel(nome_arquivo_erros, index=False)
        logging.info(f"Arquivo de erros gerado: {nome_arquivo_erros}")

    if lista_imports:
        df_imports = pd.DataFrame(lista_imports)
        nome_arquivo_imports = f"imports_vu4_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        df_imports.to_excel(nome_arquivo_imports, index=False)
        logging.info(f"Arquivo de imports gerado: {nome_arquivo_imports}")

    os.remove(download_file)

# if __name__ == "__main__":
#     vu4()
