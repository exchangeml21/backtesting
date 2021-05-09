import pandas as pd
import zipfile
zipref = zipfile.ZipFile("09122020.csv.zip", "r")
filename = zipref.namelist()[0]
f=zipref.open(filename)
df = pd.read_csv(f)
