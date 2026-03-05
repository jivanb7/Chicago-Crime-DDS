from pymongo import MongoClient
import pandas as pd

uri = "mongodb+srv://dbuser:2IpbYCFEWj5nij77@cluster0.xsanuz.mongodb.net/?appName=Cluster0"

client = MongoClient(uri)

db = client["crime"]
collection = db["chicago_crime_agg_context"]

data = list(collection.find())

df = pd.DataFrame(data)

df.drop(columns=["_id"], inplace=True)

df.to_csv("crime_aggregated.csv", index=False)

print("Saved crime_aggregated.csv")