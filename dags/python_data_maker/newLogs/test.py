import pandas as pd

df = pd.read_csv("./itemsdata.csv")

id_value = 100462
value =  df[df['ID']==id_value]['Category'].values[0]
print(value)

