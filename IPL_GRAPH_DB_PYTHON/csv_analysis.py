import pandas as pd


df= pd.read_csv(r"C:\Users\basup\OneDrive\Desktop\IPL\IPL_Matches_2008_2022.csv")  

print(df.isna().sum())
