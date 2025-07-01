import os
import re
import requests
import subprocess
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium import webdriver
import time
from configuracoes import *
import logging
chrome_driver_path = "chromedriver.exe"

def obter_versao_chrome():
    """Obtém a versão do Google Chrome instalada no sistema."""
    try:
        result = subprocess.run(
            ['reg', 'query', r'HKEY_CURRENT_USER\Software\Google\Chrome\BLBeacon', '/v', 'version'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        match = re.search(r'version\s+REG_SZ\s+([\d.]+)', result.stdout)
        if match:
            return match.group(1)
    except Exception as e:
        logging.error(f"Erro ao obter a versão do Chrome: {e}")
    return None

def obter_versao_chromedriver():
    """Obtém a versão do ChromeDriver atual."""
    try:
        result = subprocess.run(
            [chrome_driver_path, '--version'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        match = re.search(r'ChromeDriver\s+([\d.]+)', result.stdout)
        if match:
            return match.group(1)
    except Exception as e:
        logging.error(f"Erro ao obter a versão do ChromeDriver: {e}")
    return None

def baixar_chromedriver(versao_chrome):
    """Baixa a versão correta do ChromeDriver com base na versão do Chrome."""
    try:
        major_version = versao_chrome.split('.')[0]

        url = f"https://storage.googleapis.com/chrome-for-testing-public/{versao_chrome}/win64/chromedriver-win64.zip"
        response = requests.get(url)

        if response.status_code == 200:
            latest_version = response.text.strip()
            download_url = f"https://chromedriver.storage.googleapis.com/{latest_version}/chromedriver_win32.zip"
            zip_path = os.path.join(os.getcwd(), "chromedriver.zip")
            
            # Baixar o arquivo zip
            with requests.get(url, stream=True) as r:
                with open(zip_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

            # Remover o ChromeDriver antigo se existir
            if os.path.exists(chrome_driver_path):
                try:
                    os.remove(chrome_driver_path)
                except Exception as e:
                    logging.error(f"Erro ao remover ChromeDriver antigo: {e}")
                    return

            # Extrair o arquivo zip
            time.sleep(2)  # Reduzido o tempo de espera
            import zipfile
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Extrair para um diretório temporário
                temp_dir = os.path.join(os.getcwd(), "temp_chromedriver")
                zip_ref.extractall(temp_dir)
                
                # Encontrar a pasta que começa com "chromedriver"
                pasta_chrome = None
                for item in os.listdir(temp_dir):
                    if os.path.isdir(os.path.join(temp_dir, item)) and item.startswith("chromedriver"):
                        pasta_chrome = os.path.join(temp_dir, item)
                        break

                if pasta_chrome:
                    chromedriver_exe = os.path.join(pasta_chrome, "chromedriver.exe")
                    if os.path.exists(chromedriver_exe):
                        import shutil
                        shutil.move(chromedriver_exe, chrome_driver_path)
                        # Remover a pasta chromedriver após mover o arquivo
                        shutil.rmtree(pasta_chrome)
                    else:
                        logging.info("chromedriver.exe não encontrado na pasta")
                else:
                    logging.info("Pasta chromedriver não encontrada")
                
                # Limpar o diretório temporário
                shutil.rmtree(temp_dir)
            
            # Remover o arquivo zip
            os.remove(zip_path)

            logging.info("ChromeDriver atualizado com sucesso!")
        else:
            logging.error(f"Erro ao obter a versão mais recente do ChromeDriver: {response.status_code}")
    except Exception as e:
        logging.error(f"Erro ao baixar o ChromeDriver: {e}")

def verificar_e_atualizar_chromedriver():
    """Verifica se as versões do Chrome e ChromeDriver são compatíveis e atualiza se necessário."""
    versao_chrome = obter_versao_chrome()
    versao_chromedriver = obter_versao_chromedriver()
    
    if not versao_chrome:
        logging.error("Não foi possível determinar a versão do Chrome.")
        return
    
    if not versao_chromedriver or not versao_chromedriver.startswith(versao_chrome.split('.')[0]):
        logging.info("Versão do ChromeDriver incompatível. Atualizando...")
        baixar_chromedriver(versao_chrome)
    else:
        logging.info("ChromeDriver já está atualizado.")

def configurar_driver():
    verificar_e_atualizar_chromedriver()
    options = webdriver.ChromeOptions()
    options.add_argument('--start-maximized')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument("window-size=1920x1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    service = ChromeService(chrome_driver_path)
    return webdriver.Chrome(service=service, options=options)