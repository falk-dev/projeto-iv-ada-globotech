import sys
from conexao_sqlalchemy import criar_engine_mysql, consultas
from relatorios import executar_relatorio_sql
import pandas as pd

def mostrar_menu():
   print("\n=== Sistema de Relatórios Globo Tech ===\n")
   print("Selecione o relatório que deseja gerar:")
   for idx, nome_rel in enumerate(consultas.keys(), 1):
      print(f"{idx}. {nome_rel.replace('_', ' ').capitalize()}")
   print("0. Sair\n")

def obter_escolha():
   while True:
      try:
         escolha = int(input("Digite o número do relatório (0 para sair): "))
         if 0 <= escolha <= len(consultas):
               return escolha
         else:
               print("Número inválido. Tente novamente.")
      except ValueError:
         print("Entrada inválida. Digite um número.")

def main():
   engine_mysql = criar_engine_mysql()
   while True:
      mostrar_menu()
      escolha = obter_escolha()
      if escolha == 0:
         print("Encerrando o sistema. Até logo!")
         sys.exit(0)

      nome_relatorio = list(consultas.keys())[escolha - 1]
      print(f"\nExecutando relatório: {nome_relatorio.replace('_', ' ').capitalize()}...")

      df = executar_relatorio_sql(consultas[nome_relatorio], engine_mysql, nome_csv=f"{nome_relatorio}.csv")

      print(f"Relatório '{nome_relatorio}' salvo como '{nome_relatorio}.csv'\n")
      print("Visualização rápida dos dados:\n")
      print(df.head(10).to_string(index=False))  # mostra as 10 primeiras linhas formatadas
      print("\n" + "-"*50 + "\n")

if __name__ == "__main__":
   main()
