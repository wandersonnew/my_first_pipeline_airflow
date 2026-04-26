import pandas as pd

def marge_por_produto(produtos):
    margens = {}

    for produto in produtos:
        if "Gasolina Aditivada" in produto:
            margens[produto] = 0.17
        elif "Gasolina" in produto:
            margens[produto] = 0.15
        elif "Etanol" in produto:
            margens[produto] = 0.12
        elif "Diesel" in produto:
            margens[produto] = 0.10
        elif "Gnv" in produto:
            margens[produto] = 0.18
        elif "Glp" in produto:
            margens[produto] = 0.20
        else:
            margens[produto] = 0.15

    return margens

def calcula_lucro(compra, venda, produto, margens):
    if pd.notna(compra) and pd.notna(venda):
        return venda - compra

    if pd.isna(compra) and pd.notna(venda):
        return venda * margens.get(produto, 0.15)
    
    return 0