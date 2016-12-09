#import pandas for data cleansing process
import pandas as pd

# Read file
df = pd.read_csv("/Users/karthikeyanc/Documents/Documents/CK/Learning/Lab/fallProject/SFPD.csv")

#Data cleaning and transformation 
df['Resolution'] = df['Resolution'].replace(['NONE'], 'NO RESOLUTION')
df['Address']    = df['Address'].str.replace('/', '-')
df['Category']   = df['Category'].str.replace('/', '-')
df['Date']       = pd.to_datetime(df['Date'])
df['year'], df['month'],df['day'] = df['Date'].dt.year, df['Date'].dt.month,df['Date'].dt.day

#writing it an csv file
df.to_csv('SFPD_TRANSFORMED.csv', sep=',')

