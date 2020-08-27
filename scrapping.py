# import numpy as np
import pandas as pd
# import seaborn as sns
# import string
import requests
import warnings
warnings.filterwarnings('ignore')
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

header = {
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
  "X-Requested-With": "XMLHttpRequest"
}

url = 'http://fundamentus.com.br/resultado.php'

print('Realizando requisição...')
r = requests.get(url, headers=header)

print('Lendo a página...')
df = pd.read_html(r.text, thousands='.', decimal=',')[0]

for coluna in ['Div.Yield', 'Mrg Ebit', 'Mrg. Líq.', 'ROIC', 'ROE', 'Cresc. Rec.5a']:
  df[coluna] = df[coluna].str.replace('.', '')
  df[coluna] = df[coluna].str.replace(',', '.')
  df[coluna] = df[coluna].str.rstrip('%').astype('float') / 100

# valor_liq_2m = float(input(print('Retornar empresas com lucro líquido dos últimos dois meses superior a: R$', end=' ', flush=False)))
# define o valor de corte para as empresas a aparecerem
df = df[df['Liq.2meses'] > 1000000]

print('Realizando cálculo para o ranking...')
ranking = pd.DataFrame()
ranking['Position'] = range(1,151)
ranking['EV/EBIT'] = df[df['EV/EBIT'] > 0].sort_values(by=['EV/EBIT'])['Papel'][:150].values
ranking['ROIC'] = df.sort_values(by=['ROIC'], ascending=False)['Papel'][:150].values

auxA = ranking.pivot_table(columns='EV/EBIT', values='Position')
auxB = ranking.pivot_table(columns='ROIC', values='Position')
t = pd.concat([auxA, auxB])

rank = t.dropna(axis=1).sum().astype(int).sort_values()

rank.to_csv('C:/Users/Jandrei/Desktop/Apps/AnaliseFundamentalista/ranking.csv', index=True, sep=';', header=False)

df = df.loc[:,['Papel']]

def get(url, timeout):
    return requests.get(url, headers=header, timeout = timeout)

def requestUrls(urls, timeout = 5):
    with ThreadPoolExecutor(max_workers = 3) as executor:
        agenda = { executor.submit(get, url, timeout): url for url in urls }

        for tarefa in as_completed(agenda):     
            try:
                conteudo = tarefa.result()
            except Exception as e:
                print ("\nNão foi possível fazer a requisição! \n{}".format(e))
            else:
                yield conteudo
                
urls = 'https://www.fundamentus.com.br/detalhes.php?papel=' + df['Papel'].values

print('\nPROCESSAMENTO DE REQUISIÇÕES\n')
requisicoes = requestUrls(urls, timeout=60)

lista_empresas = []

total_ticket = len(df)

for requisicao in tqdm(requisicoes, unit=' requisições', desc='Requisitando dados: ', disable=False, total=total_ticket, ncols=100):
    # codigo = requisicao.status_code
    # url = requisicao.url
    conteudo = requisicao.content
    lista_empresas += [conteudo]
    # print (f'{codigo}: {url}')
    
colunas = [
    'Papel',
    'Empresa',
    'Setor',
    'Subsetor',
    'Cotação',
    'Valor da Firma',
    'Valor de Mercado',
    'Número de Ações',
    'P/L',
    'Div. Yield',
    'EV/EBITDA',
    'EV/EBIT',
    'ROE',
    'ROIC',
    'Lucro Líquido (ano)'
]

tabela_base = pd.DataFrame(columns=colunas)

total_lista = len(lista_empresas)

for key in tqdm(range(total_lista), unit=' dados', desc='Tratando dados: ', disable=False, total=total_lista, ncols=100):
    dados_base = pd.read_html(lista_empresas[key])
    papel = dados_base[0][1][0]
    empresa = dados_base[0][1][2]
    setor = dados_base[0][1][3]
    subsetor = dados_base[0][1][4]
    cotacao = dados_base[0][3][0]
    valor_da_firma = dados_base[1][1][1]
    valor_de_mercado = dados_base[1][1][0]
    n_acoes = dados_base[1][3][1]
    p_l = dados_base[2][3][1]
    div_yield = dados_base[2][3][8]
    ev_ebitda = dados_base[2][3][9]
    ev_ebit = dados_base[2][3][10]
    roe = dados_base[2][5][8]
    roic = dados_base[2][5][7]
    lucro_liq_ano = dados_base[4][1][4]
    to_append = [papel, empresa, setor, subsetor, cotacao, valor_da_firma, valor_de_mercado, n_acoes, p_l, div_yield, ev_ebitda, ev_ebit, roe, roic, lucro_liq_ano]
    tabela_base_length = len(tabela_base)
    tabela_base.loc[tabela_base_length] = to_append
    
for coluna in ['ROE', 'Div. Yield', 'ROIC']:
    tabela_base[coluna] = tabela_base[coluna].str.rstrip('%')
    
tabela_base.to_csv('C:/Users/Jandrei/Desktop/Apps/AnaliseFundamentalista/tabela_dados.csv', index=False, sep=';')

print('\nDados atualizados')