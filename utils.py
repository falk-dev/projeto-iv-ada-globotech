# utils.py
import pandas as pd

def validar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
   df = df.dropna(subset=["id_usuario", "id_conteudo", "plataforma"])
   df = df[df["watch_duration_seconds"] >= 0]
   return df