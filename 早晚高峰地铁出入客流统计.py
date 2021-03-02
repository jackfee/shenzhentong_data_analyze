import pandas as pd
import datetime

start_time=datetime.datetime.now()
coln = ['卡号', '交易日期时间', '交易类型', '交易金额', '交易值', '设备编码', '公司名称', '线路站点', '车牌号','联程标记', '结算日期']
df_in_morning=pd.DataFrame(columns=[])
df_out_morning=pd.DataFrame(columns=[])
df_in_evening=pd.DataFrame(columns=[])
df_out_evening=pd.DataFrame(columns=[])

for i in range(1,32):
    file_name=20191200+i
    file_name = str(file_name)
    weekday_judge = datetime.datetime.strptime(file_name,'%Y%m%d').weekday()
    if weekday_judge<5:
        read_path = r'G:\深圳通公交数据\深圳通2019-12_数据规整'
        read_path_name = read_path + '\\' + file_name
        df_tmp = pd.read_table(read_path_name, sep=',', engine='python', encoding='UTF8', names=coln)  # 读数据，并且命名列
        df_tmp.sort_values(['卡号', '交易日期时间'], ascending=True, inplace=True)
        df_tmp['mm'] = ((df_tmp['交易日期时间'] % 1000000) // 10000) * 60 + (df_tmp['交易日期时间'] % 10000) // 100  # 根据交易时间字段提取交易的小时分钟
        df_tmp_subway_in = df_tmp[(df_tmp['交易类型'] == '地铁入站')]
        df_tmp_subway_out = df_tmp[(df_tmp['交易类型'] == '地铁出站')]

        df_tmp_subway_out_morning = df_tmp_subway_out[(df_tmp_subway_out['mm'] > 450) & (df_tmp_subway_out['mm'] < 570)]   #早高峰出站
        df_tmp_subway_out_morning_count=df_tmp_subway_out_morning[['线路站点','mm']].groupby(['线路站点']).count()
        df_tmp_subway_out_morning_count=df_tmp_subway_out_morning_count.rename(columns={'mm':file_name})
        df_out_morning=pd.merge(df_out_morning,df_tmp_subway_out_morning_count,left_index=True,right_index=True,how='outer')

        df_tmp_subway_in_morning = df_tmp_subway_in[(df_tmp_subway_in['mm'] > 450) & (df_tmp_subway_in['mm'] < 570)]     #早高峰入站
        df_tmp_subway_in_morning_count = df_tmp_subway_in_morning[['线路站点', 'mm']].groupby(['线路站点']).count()
        df_tmp_subway_in_morning_count=df_tmp_subway_in_morning_count.rename(columns={'mm':file_name})
        df_in_morning=pd.merge(df_in_morning,df_tmp_subway_in_morning_count,left_index=True,right_index=True,how='outer')

        df_tmp_subway_out_evening = df_tmp_subway_out[(df_tmp_subway_out['mm'] > 1050) & (df_tmp_subway_out['mm'] < 1170)]  #晚高峰出站
        df_tmp_subway_out_evening_count=df_tmp_subway_out_evening[['线路站点', 'mm']].groupby(['线路站点']).count()
        df_tmp_subway_out_evening_count=df_tmp_subway_out_evening_count.rename(columns={'mm':file_name})
        df_out_evening=pd.merge(df_out_evening,df_tmp_subway_out_evening_count,left_index=True,right_index=True,how='outer')

        df_tmp_subway_in_evening = df_tmp_subway_in[(df_tmp_subway_in['mm'] > 1050) & (df_tmp_subway_in['mm'] < 1170)]      #晚高峰入站
        df_tmp_subway_in_evening_count = df_tmp_subway_in_evening[['线路站点', 'mm']].groupby(['线路站点']).count()
        df_tmp_subway_in_evening_count=df_tmp_subway_in_evening_count.rename(columns={'mm':file_name})
        df_in_evening=pd.merge(df_in_evening,df_tmp_subway_in_evening_count,left_index=True,right_index=True,how='outer')
    else:
        continue
    during_time=datetime.datetime.now()
    print('文件',file_name,'用时',during_time-start_time)
df_out_morning.to_excel(r'G:\深圳通公交数据\数据分析结果\早晚高峰客流统计\早高峰出站.xlsx',encoding='ANSI')
df_in_morning.to_excel(r'G:\深圳通公交数据\数据分析结果\早晚高峰客流统计\早高峰入站.xlsx',encoding='ANSI')
df_out_evening.to_excel(r'G:\深圳通公交数据\数据分析结果\早晚高峰客流统计\晚高峰出站.xlsx',encoding='ANSI')
df_in_evening.to_excel(r'G:\深圳通公交数据\数据分析结果\早晚高峰客流统计\晚高峰入站.xlsx',encoding='ANSI')
