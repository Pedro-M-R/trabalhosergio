import pandas as pd

valor = pd.read_csv("baixados_sia/SIA_Valor_aprovado_certo.csv", sep=',')
print(valor.head())

quantidade = pd.read_csv("baixados_sia/SIA_QTD.aprovada_certo.csv", sep=',')
print(quantidade.head())

