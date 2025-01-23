import pandas as pd

fileNames =["Coffee_domestic_consumption.csv", "Coffee_production.csv"]

df = pd.read_csv(fileNames[0])

print(df["Coffee type"][1])