import pandas as pd
import datetime
from pandas import DataFrame as df
#程序未完成，
num=20191208
filename=str(num)+'换乘旅客分析结果.xlsx'
read_path=r'G:\深圳通公交数据\数据分析结果\常旅客换乘结果_30min换乘间隔'+'\\'+filename
df=pd.read_excel(read_path,encoding='ANSI')
# df['first']=df['换乘类别'].sep('_')[0]
# df['fisrt']=''
for i in range(df.shape[0]-1):
    df.loc[i,'first']=df['换乘类别'][i].split('_')[0]
    if df['换乘类别'][i].split('_')[1] is not None:
        df.loc[i, 'second'] = df['换乘类别'][i].split('_')[1]
railway_railway=(float(df[(df['换乘类别']=='地铁出站_地铁入站')]['样本量'])+float(df[(df['换乘类别']=='地铁入站_地铁入站')]['样本量']))
print('bus_bus:',bus_bus)

print(df)