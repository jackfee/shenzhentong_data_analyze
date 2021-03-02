import pandas as pd
import datetime

start_time=datetime.datetime.now()
coln = ['卡号', '交易日期时间', '交易类型', '交易金额', '交易值', '设备编码', '公司名称', '线路站点', '车牌号','联程标记', '结算日期']
result=pd.DataFrame(columns=['第几天','地铁换乘出行次数','地铁出行总次数','地铁换乘比例'])
for i in range(1,32):
    # file_name=r'\20191201'
    file_name=20191200+i
    file_name='\\'+str(file_name)
    read_path=r'G:\深圳通公交数据\深圳通2019-12_数据规整'
    read_path_name=read_path+file_name

    df_tmp=pd.read_table(read_path_name,sep=',',engine='python',encoding='UTF8',names=coln)        #读数据，并且命名列



    df_tmp.sort_values(['卡号','交易日期时间'],ascending=True,inplace=True)
    df_tmp['mm']=((df_tmp['交易日期时间']%1000000)//10000)*60+(df_tmp['交易日期时间']%10000)//100           #根据交易时间字段提取交易的小时分钟
    df_tmp['卡号变化']=df_tmp['卡号'].diff()    #计算卡号变化，本行数据-上行数据
    df_tmp['时间变化']=df_tmp['mm'].diff()   #计算时间变化，本行数据-上行数据


    df_tmp['卡号变化'][df_tmp['卡号变化']!=0]=1     #改变标记，将卡号变化的位置记录为1
    df_tmp['换乘偏移']=df_tmp['交易类型'].shift(1)  #将换乘类型列下一行，用于随后计算是否发生换乘
    df_tmp['换乘类别']=df_tmp['换乘偏移']+'_'+df_tmp['交易类型']  #换乘类别，用于判断换乘发生的类型，前面的数表示下一次换的交通工具，个位数表示前一次的类型

    df_tmp_subway=df_tmp[(df_tmp['交易类型']=='地铁入站')|(df_tmp['交易类型']=='地铁出站')]
    # df_tmp_subway_all_sample_size=df_tmp_subway.shape[0]
    # print('地铁出入合计样本总量：',df_tmp_subway_all_sample_size)
    df_tmp_subway_rz=df_tmp_subway[df_tmp_subway['交易类型']=='地铁入站']
    df_tmp_subway_rz_sample_size=df_tmp_subway_rz.shape[0]
    print('地铁入站合计样本总量：',df_tmp_subway_rz_sample_size)

    # print(df_tmp_subway.columns.tolist())
    # print(df_tmp_subway['交易类型'].unique())
    def rename_df_lines_type(dff):
        ys=[ '地铁一号线', '地铁二号线','地铁三号线','地铁四号线', '地铁五号线','地铁七号线','地铁九号线','地铁十一号线']
        dff['地铁线路']=0
        bg=[1,2,3,4,5,7,9,11]
        print(ys)
        print(bg)
        for i in range(len(ys)):
            dff['地铁线路'][dff['公司名称']==ys[i]]=bg[i]
        return dff
    df_tmp_subway=rename_df_lines_type(df_tmp_subway)
    df_tmp_subway['线路变化']=df_tmp_subway['地铁线路'].diff()
    df_tmp_subway_s=df_tmp_subway[df_tmp_subway['卡号变化']!=1]
    df_tmp_subway=df_tmp_subway[df_tmp_subway['换乘类别']=='地铁入站_地铁出站']
    df_tmp_subway_xrhc_sample_size=df_tmp_subway.shape[0]
    print('先入后出样本总量：',df_tmp_subway_xrhc_sample_size)
    subway_line_change_times=df_tmp_subway[df_tmp_subway['线路变化']!=0].shape[0]
    subway_line_change_proportions=subway_line_change_times/df_tmp_subway_rz_sample_size
    dict_tmp={'第几天':i,'地铁换乘出行次数':subway_line_change_times,'地铁出行总次数':df_tmp_subway_rz_sample_size,'地铁换乘比例':subway_line_change_proportions}
    result=result.append(dict_tmp,ignore_index=True)
    during_time=datetime.datetime.now()
    print(i,'个文件，耗时：',during_time-start_time)

write_path=r'G:\深圳通公交数据\数据分析结果\地铁地铁换乘计算结果'
write_path_name=write_path+'\地铁换乘计算.xlsx'
result.to_excel(write_path_name,encoding='ANSI')


end_time=datetime.datetime.now()
print('程序耗时',end_time-start_time)