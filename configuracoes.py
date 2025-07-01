"""Configurações do servidor de e-mail:"""
c_smtp_server = "email-ssl.com.br"
c_smtp_port = 587

"""Configurações de usuário e senha:"""
c_email_remetente = 'Enorsig@enorsul.com.br'
c_senha = 'Enorsig@2025'

"""Configurações do destinatário"""
c_destinatario = 'marcos.serapiao@enorsul.com.br'

"""Se usar gmail, comentar a as configurações acima e descomentar as abaixo"""
#c_smtp_server = "smtp.gmail.com"
#c_smtp_port = 587
#c_email_remetente = 'automacaoenorsul@gmail.com'
#c_senha = 'plny qyii ugdb lwib'

"""Conexão ao SQL"""
Servidor = '200.98.80.97'
UID = 'mserapiao'
PWD = '00!@serapiao'

"""Configuração de diretório de download dinâmico"""
import os
c_download_dir = os.path.join(os.path.expanduser("~"), "Downloads")
