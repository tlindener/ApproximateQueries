import pandas as pd

df = pd.read_csv('data\\ratings.csv', names=['product', 'user', 'rating', 'timestamp'])
df['product'].plot(kind='hist', bins=100)
