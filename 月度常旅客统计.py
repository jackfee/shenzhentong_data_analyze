import sys
import pandas as pd
import datetime
from pandas import DataFrame as df

start_time=datetime.datetime.now()

df_tmp_total_result=pd.DataFrame(columns=['卡号','交易类型'])
for i in range(1,32):
    filename_num=20191200+i
    coln = ['卡号', '交易日期时间', '交易类型', '交易金额', '交易值', '设备编码', '公司名称', '线路站点', '车牌号','联程标记', '结算日期']
    read_path =r'G:\深圳通公交数据\深圳通2019-12_数据规整'+'\\'+str(filename_num)
    df_tmp=pd.read_table(read_path,sep=',',engine='python',encoding='UTF8',names=coln)
    df_tmp_s=df_tmp[(df_tmp['交易类型']!='地铁出站') & (df_tmp['交易类型']!='巴士下车')& (df_tmp['交易类型']!='巴士二维码下车')]     #去掉所有下车的记录
    df_tmp_result=df_tmp_s[['卡号','交易类型']].groupby(['卡号']).count()
    df_tmp_total_result=df_tmp_total_result.append(df_tmp_result,sort=True)
    during_time=datetime.datetime.now()
    print('文件', i,'读取完毕，用时：',during_time-start_time)
df_tmp_total_result=df_tmp_total_result.sort_values(['交易类型'],ascending=False)
df_tmp_total_result=df_tmp_total_result.rename(columns={'交易类型':'数量'})
print(df_tmp_total_result.columns)
df_tmp_total_result.index.name='ID'
df_tmp_total_result=df_tmp_total_result.drop(['卡号'],axis=1)

df_tmp_total_result_count=df_tmp_total_result.groupby(['ID']).count()
dict_tmp={}
dataf=pd.DataFrame(columns=['天数','人数','占比'])
for i in range(1,32):
#     print('每月乘坐',i,'天','共计',df_tmp_total_result_count[df_tmp_total_result_count==i].shape[0],'人次','占比',%.2f %(100*(df_tmp_total_result_count[df_tmp_total_result_count==i].shape[0])/(df_tmp_total_result_count.shape[0])),'%')
    dict_tmp['天数']=i
    dict_tmp['人数']=df_tmp_total_result_count[df_tmp_total_result_count['数量']==i].shape[0]
    dict_tmp['占比']=((df_tmp_total_result_count[df_tmp_total_result_count['数量']==i].shape[0])/(df_tmp_total_result_count.shape[0]))
    dataf=dataf.append(dict_tmp,ignore_index=True)
during_time = datetime.datetime.now()
print('按天数统计耗时：', during_time - start_time)
dataf.to_excel(r'G:\深圳通公交数据\数据分析结果\月度常旅客统计\常旅客_按每月出行天数统计.xlsx',encoding='ANSI')

df_tmp_total_result_sum=df_tmp_total_result.groupby(['ID']).sum()
dict_tmp={}
datah=pd.DataFrame(columns=['次数','人数','占比'])
transit_times=df_tmp_total_result_sum['数量'].unique() #transit_times为一个月出行的天数的类型，如一个月出行6次、10次
for i in transit_times:
#     print('每月乘坐',i,'天','共计',df_tmp_total_result_count[df_tmp_total_result_count==i].shape[0],'人次','占比',%.2f %(100*(df_tmp_total_result_count[df_tmp_total_result_count==i].shape[0])/(df_tmp_total_result_count.shape[0])),'%')
    dict_tmp['次数']=i
    dict_tmp['人数']=df_tmp_total_result_sum['数量'][df_tmp_total_result_sum['数量']==i].sum()
    dict_tmp['占比']=(df_tmp_total_result_sum['数量'][df_tmp_total_result_sum['数量']==i].sum()/(df_tmp_total_result_sum['数量'].sum()))
    datah=datah.append(dict_tmp,ignore_index=True)
during_time = datetime.datetime.now()
print('按次数统计耗时：', during_time - start_time)
datah.to_excel(r'G:\深圳通公交数据\数据分析结果\月度常旅客统计\常旅客_按每月出行次数统计.xlsx',encoding='ANSI')

end_time=datetime.datetime.now()
print('程序耗时:',end_time-start_time)
