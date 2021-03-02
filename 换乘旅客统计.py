import sys
import pandas as pd
from pandas import DataFrame as df
import datetime

start_time=datetime.datetime.now()
coln = ['卡号', '交易日期时间', '交易类型', '交易金额', '交易值', '设备编码', '公司名称', '线路站点', '车牌号','联程标记', '结算日期']
file_name=r'\20191201'
read_path=r'G:\深圳通公交数据\深圳通2019-12_数据规整'
read_path_name=read_path+file_name

df_tmp=pd.read_table(read_path_name,sep=',',engine='python',encoding='UTF8',names=coln)        #读数据，并且命名列

def rename_df_transaction_type(dff):                             #根据交易类型重新赋值定义换乘类型列，便于后续处理（必要性不强）
    ys=dff['交易类型'].unique()
    print(ys)
    print('交易类型',len(ys))
    dff['换乘类型']=0
    bg=list(range(1,len(ys)+1))
    print(ys)
    print(bg)
    for i in range(len(ys)):
        dff['换乘类型'][dff['交易类型']==ys[i]]=bg[i]
    return dff

df_tmp=rename_df_transaction_type(df_tmp)
df_tmp.sort_values(['卡号','交易日期时间'],ascending=True,inplace=True)
df_tmp['mm']=((df_tmp['交易日期时间']%1000000)//10000)*60+(df_tmp['交易日期时间']%10000)//100           #根据交易时间字段提取交易的小时分钟
df_tmp['卡号变化']=df_tmp['卡号'].diff()    #计算卡号变化，本行数据-上行数据
df_tmp['时间变化']=df_tmp['mm'].diff()   #计算时间变化，本行数据-上行数据
df_tmp.iloc[0,13]=0  #把NaN替换为0
df_tmp.iloc[0,14]=0  #把NaN替换为0

df_tmp['卡号变化'][df_tmp['卡号变化']!=0]=1     #改变标记，将卡号变化的位置记录为1
df_tmp['换乘偏移']=df_tmp['换乘类型'].shift(1)  #将换乘类型列下一行，用于随后计算是否发生换乘
df_tmp['换乘编号']=df_tmp['换乘偏移']*100+df_tmp['换乘类型']  #换乘编号，用于判断换乘发生的类型，前面的数表示下一次换的交通工具，个位数表示前一次的类型
# df_tmp_idnochange=df_tmp[df_tmp['卡号变化']!=1]          #去掉所有卡号发生变化的点，因为用于换乘判别时，卡号的第一条记录是无效的
# transfer_type=df_tmp_s[['换乘编号','卡号']].groupby('换乘编号').count()    #只提取出两列即能满足后续计数需求，减少内存负担，transfer_type存储不同换乘编号->不同换乘方式的数目
# transfer_type.sort_values('卡号',ascending=False,inplace=True)            #卡号列现在存储的是统计数目，或者 transfer_type.rename(columns={'卡号':'数量'})
df_tmp_s=df_tmp[(df_tmp['交易类型']!='二维码巴士')&(df_tmp['交易类型']!='巴士二维码下车')&(df_tmp['交易类型']!='巴士二维码补扣')]     #剔除二维码相关数据，因为无法计算换乘（只能刷公交、不能刷地铁）
df_tmp_s=df_tmp_s[df_tmp_s['卡号变化']!=1]                      #去掉卡号发生变化的点，用于换乘客流分析
transfer_type=df_tmp_s.loc[:,['换乘编号','时间变化']]

transfer_type=transfer_type[transfer_type['时间变化']<90]      #时间变化小于90用于分析换乘客流
sample_delqrcode_size=transfer_type.shape[0]                   #用于本次分析的、去除二维码相关数据的样本总数
df_tmp_s_rz=df_tmp[(df_tmp['交易类型']=='地铁入站')|(df_tmp['交易类型']=='巴士')|(df_tmp['交易类型']=='巴士上车')|(df_tmp['交易类型']=='巴士分段补扣')]  #用于本次分析的，去除地铁出站、二维码、巴士下车的样本总数（去重）
sample_valid_size=df_tmp_s_rz.shape[0]             #用于本次分析的，去除地铁出站、二维码、巴士下车的样本总数（去重）
print('用于换乘分析的样本总数，不包括地铁出站、二维码、巴士下车:',sample_valid_size)
result=transfer_type.groupby(['换乘编号']).count()
result=result.rename(columns={'时间变化':'样本量'})
write_path=r'G:\深圳通公交数据\数据分析结果'
writefilename=file_name+'换乘旅客分析结果.csv'
write_path_name=write_path+writefilename
result=result.sort_values(['样本量'],ascending=False)
result.to_csv(write_path_name,encoding='ANSI')


end_time=datetime.datetime.now()
print('程序耗时',end_time-start_time)
