import sys
import os
import pandas as pd
from conexao_sqlalchemy import criar_engine_mysql, consultas
from relatorios import executar_relatorio_sql

def limpar_tela():
   """Limpa a tela do console"""
   os.system('cls' if os.name == 'nt' else 'clear')

def mostrar_menu():
   limpar_tela()
   print("\n=== SISTEMA DE RELATÓRIOS ===")
   print("\nEscolha um relatório:\n")
   
   # Lista ordenada alfabeticamente
   relatorios = sorted(consultas.keys())
   
   for idx, relatorio in enumerate(relatorios, 1):
      print(f"{idx}. {relatorio.replace('_', ' ').title()}")
   
   print("\n0. Sair")

def obter_escolha():
   while True:
      escolha = input("\nDigite o número do relatório: ")
      
      if escolha == '0':
         return 0
         
      if escolha.isdigit() and 1 <= int(escolha) <= len(consultas):
         return int(escolha)
         
      print("❌ Opção inválida. Tente novamente.")

def main():
   try:
      engine = criar_engine_mysql()
      
      while True:
         mostrar_menu()
         escolha = obter_escolha()
         
         if escolha == 0:
               print("\n✅ Programa encerrado com sucesso!")
               sys.exit(0)
               
         nome_relatorio = sorted(consultas.keys())[escolha-1]
         limpar_tela()
         print(f"\n📊 RELATÓRIO: {nome_relatorio.replace('_', ' ').title()}\n")
         
         # Executa o relatório
         df = executar_relatorio_sql(consultas[nome_relatorio], engine)
         
         if not df.empty:
               # Pergunta sobre salvamento
               salvar = input("\nDeseja salvar o relatório? (s/n): ").lower()
               if salvar == 's':
                  os.makedirs("relatorios", exist_ok=True)
                  caminho = f"relatorios/{nome_relatorio}.csv"
                  df.to_csv(caminho, index=False)
                  print(f"\n✅ Relatório salvo em: {caminho}")
         
         input("\nPressione Enter para continuar...")
         
   except Exception as e:
      print(f"\n❌ Erro: {str(e)}")
      sys.exit(1)

if __name__ == "__main__":
   main()