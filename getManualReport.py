import pandas as pd
# from datetime import datetime
from dataLoader import DataLoader

# 实例化 DataLoader
data_loader = DataLoader()


def get_dataframe_Manual_CIC_BMS():
    df = data_loader.get_dataframe("CIC_BMS")
    # 先筛选出“Rpt Key”列值不为空的行数据
    df_non_empty = df[df['Rpt Key'].notna()]
    # 然后筛选出“Rpt Key”列值的起始字母不为CTR的行数据
    # 使用字符串操作 str.startswith() 进行匹配
    filtered_df = df_non_empty[
        ~df_non_empty['Rpt Key'].str.startswith('CTR') & (df_non_empty['Billing Status'].str.lower() == 'successful')]
    return filtered_df


def get_dataframe_Manual_GCG_BMS():
    df = data_loader.get_dataframe("GCG_BMS")
    # 先筛选出“Rpt Key”列值不为空的行数据
    df_non_empty = df[df['Rpt Key'].notna()]
    # 然后筛选出“Rpt Key”列值的起始字母不为CTR的行数据
    # 使用字符串操作 str.startswith() 进行匹配
    filtered_df = df_non_empty[
        ~df_non_empty['Rpt Key'].str.startswith('CTR') & (df_non_empty['Billing Status'].str.lower() == 'successful')]
    return filtered_df


def get_dataframe_Manual_CIC_ISB_Chargeout():
    df = data_loader.get_dataframe("CIC_ChargeOut_ISB")
    # 根据条件1过滤数据：列字段名“Report prepared by :(ID)”下的数据值等于
    # “Business Operations - Billing Administration/China/Contr/IBM” 或者是 “RBA Billings”.
    condition1 = df["Report prepared by :(ID)"].isin(
        ["Business Operations - Billing Administration/China/Contr/IBM", "RBA Billings"])

    # 根据条件2过滤数据：列字段名“Billing Status”下的数据值等于“successful”.
    condition2 = df["Billing Status"].str.lower() == 'successful'

    # 将两个条件结合起来进行过滤
    filtered_df = df[condition1 & condition2]
    return filtered_df


def get_dataframe_Manual_GCG_ISB_Chargeout():
    df = data_loader.get_dataframe("GCG_ChargeOut_ISB")
    # 根据条件1过滤数据：列字段名“Report prepared by :(ID)”下的数据值等于
    # “Business Operations - Billing Administration/China/Contr/IBM” 或者是 “RBA Billings”.
    condition1 = df["Report prepared by :(ID)"].isin(
        ["Business Operations - Billing Administration/China/Contr/IBM", "RBA Billings"])

    # 根据条件2过滤数据：列字段名“Billing Status”下的数据值等于“successful”.
    condition2 = df["Billing Status"].str.lower() == 'successful'

    # 将两个条件结合起来进行过滤
    filtered_df = df[condition1 & condition2]
    return filtered_df


def get_dataframe_Manual_CIC_SAP_Chargeout():
    df = data_loader.get_dataframe("CIC_ChargeOut_SAP")
    # 根据条件1过滤数据：列字段名“Report prepared by :(ID)”下的数据值等于
    # “Business Operations - Billing Administration/China/Contr/IBM” 或者是 “RBA Billings”.
    condition1 = df["Report prepared by :(ID)"].isin(
        ["Business Operations - Billing Administration/China/Contr/IBM", "RBA Billings"])

    # 根据条件2过滤数据：列字段名“Billing Status”下的数据值等于“successful”.
    condition2 = df["Billing Status"].str.lower() == 'successful'

    # 将两个条件结合起来进行过滤
    filtered_df = df[condition1 & condition2]
    return filtered_df


def get_dataframe_Manual_GCG_SAP_Chargeout():
    df = data_loader.get_dataframe("GCG_ChargeOut_SAP")
    # 根据条件1过滤数据：列字段名“Report prepared by :(ID)”下的数据值等于
    # “Business Operations - Billing Administration/China/Contr/IBM” 或者是 “RBA Billings”.
    condition1 = df["Report prepared by :(ID)"].isin(
        ["Business Operations - Billing Administration/China/Contr/IBM", "RBA Billings"])

    # 根据条件2过滤数据：列字段名“Billing Status”下的数据值等于“successful”.
    condition2 = df["Billing Status"].str.lower() == 'successful'

    # 将两个条件结合起来进行过滤
    filtered_df = df[condition1 & condition2]
    return filtered_df


def write_Manual_data_to_excel(time_range_from_billing_type_search, months_filter_for_BMS_CATS,
                               months_filter_for_ChargeOut):

    file_path_Manual = []
    result_list = []

    try:
        # 获取数据
        df_CIC_BMS = get_dataframe_Manual_CIC_BMS()
        df_GCG_BMS = get_dataframe_Manual_GCG_BMS()
        df_CIC_ISB_Chargeout = get_dataframe_Manual_CIC_ISB_Chargeout()
        df_CIC_SAP_Chargeout = get_dataframe_Manual_CIC_SAP_Chargeout()
        df_GCG_ISB_Chargeout = get_dataframe_Manual_GCG_ISB_Chargeout()
        df_GCG_SAP_Chargeout = get_dataframe_Manual_GCG_SAP_Chargeout()

        # 检查是否所有数据都为空
        if df_CIC_BMS.empty and df_GCG_BMS.empty and df_CIC_ISB_Chargeout.empty and df_CIC_SAP_Chargeout.empty and df_GCG_ISB_Chargeout.empty and df_GCG_SAP_Chargeout.empty:
            print("所有数据均为空，程序退出")
            result_list.append("NG")
            return

        # 初始化Dataframe
        combined_df_CIC_BMS = pd.DataFrame()
        combined_df_GCG_BMS = pd.DataFrame()
        combined_df_CIC_ISB_Chargeout = pd.DataFrame()
        combined_df_GCG_ISB_Chargeout = pd.DataFrame()
        combined_df_CIC_SAP_Chargeout = pd.DataFrame()
        combined_df_GCG_SAP_Chargeout = pd.DataFrame()

        # 处理 BMS 数据
        if months_filter_for_BMS_CATS is not None:
            combined_df_CIC_BMS = df_CIC_BMS[df_CIC_BMS['Ledger Month Num'].isin(months_filter_for_BMS_CATS)]
            combined_df_GCG_BMS = df_GCG_BMS[df_GCG_BMS['Ledger Month Num'].isin(months_filter_for_BMS_CATS)]

            # 创建DataFrame副本以避免SettingWithCopyWarning
            combined_df_CIC_BMS = combined_df_CIC_BMS.copy()
            combined_df_GCG_BMS = combined_df_GCG_BMS.copy()
            # 数据去重
            combined_df_CIC_BMS.drop_duplicates(inplace=True)
            combined_df_GCG_BMS.drop_duplicates(inplace=True)

        # 处理 ChargeOut 数据
        if months_filter_for_ChargeOut is not None:
            pattern = '|'.join(months_filter_for_ChargeOut)
            combined_df_CIC_ISB_Chargeout = df_CIC_ISB_Chargeout[
                df_CIC_ISB_Chargeout['Request  ID'].str.contains(pattern)]
            combined_df_GCG_ISB_Chargeout = df_GCG_ISB_Chargeout[
                df_GCG_ISB_Chargeout['Request  ID'].str.contains(pattern)]
            combined_df_CIC_SAP_Chargeout = df_CIC_SAP_Chargeout[
                df_CIC_SAP_Chargeout['Request  ID'].str.contains(pattern)]
            combined_df_GCG_SAP_Chargeout = df_GCG_SAP_Chargeout[
                df_GCG_SAP_Chargeout['Request  ID'].str.contains(pattern)]

            # 创建DataFrame副本以避免SettingWithCopyWarning
            combined_df_CIC_ISB_Chargeout = combined_df_CIC_ISB_Chargeout.copy()
            combined_df_GCG_ISB_Chargeout = combined_df_GCG_ISB_Chargeout.copy()
            combined_df_CIC_SAP_Chargeout = combined_df_CIC_SAP_Chargeout.copy()
            combined_df_GCG_SAP_Chargeout = combined_df_GCG_SAP_Chargeout.copy()
            # 数据去重
            combined_df_CIC_ISB_Chargeout.drop_duplicates(inplace=True)
            combined_df_GCG_ISB_Chargeout.drop_duplicates(inplace=True)
            combined_df_CIC_SAP_Chargeout.drop_duplicates(inplace=True)
            combined_df_GCG_SAP_Chargeout.drop_duplicates(inplace=True)

        if combined_df_CIC_BMS.empty and combined_df_GCG_BMS.empty and combined_df_CIC_ISB_Chargeout.empty and combined_df_GCG_ISB_Chargeout.empty and combined_df_CIC_SAP_Chargeout.empty and combined_df_GCG_SAP_Chargeout.empty:
            result_list.append("NG")
            print("当前输入的月份查找的Manual_Report没有数据")
        else:
            # 设置文件路径，添加时间戳防止覆盖
            # timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_path_Manual = f'report/Manual_Report_{time_range_from_billing_type_search}.xlsx'
                # 写入Excel文件
            with pd.ExcelWriter(file_path_Manual) as writer:
                if not combined_df_CIC_BMS.empty:
                    combined_df_CIC_BMS.to_excel(writer, sheet_name='CIC_BMS', index=False)
                if not combined_df_GCG_BMS.empty:
                    combined_df_GCG_BMS.to_excel(writer, sheet_name='GCG_BMS', index=False)
                if not combined_df_CIC_ISB_Chargeout.empty:
                    combined_df_CIC_ISB_Chargeout.to_excel(writer, sheet_name='CIC_ISB_Chargeout', index=False)
                if not combined_df_GCG_ISB_Chargeout.empty:
                    combined_df_GCG_ISB_Chargeout.to_excel(writer, sheet_name='GCG_ISB_Chargeout', index=False)
                if not combined_df_CIC_SAP_Chargeout.empty:
                    combined_df_CIC_SAP_Chargeout.to_excel(writer, sheet_name='CIC_SAP_Chargeout', index=False)
                if not combined_df_GCG_SAP_Chargeout.empty:
                    combined_df_GCG_SAP_Chargeout.to_excel(writer, sheet_name='GCG_SAP_Chargeout', index=False)
            print("查出的数据生成的Manual_Report已保存在本地")
            result_list.append("OK")
            file_path_Manual = [file_path_Manual]
    except Exception as e:
        print(f"程序出错: {e}")

    return file_path_Manual, result_list


if __name__ == "__main__":
    df1 = get_dataframe_Manual_CIC_BMS()
    df2 = get_dataframe_Manual_GCG_BMS()
    df3 = get_dataframe_Manual_CIC_ISB_Chargeout()
    df4 = get_dataframe_Manual_CIC_SAP_Chargeout()
    df5 = get_dataframe_Manual_GCG_ISB_Chargeout()
    df6 = get_dataframe_Manual_GCG_SAP_Chargeout()
    # print("CIC_BMS: ", df1)
    # print("GCG_BMS: ", df2)
    # print("CIC_ChargeOut_ISB: ", df3)
    # print("CIC_ChargeOut_SAP: ", df4)
    print("GCG_ChargeOut_ISB: ", df5)
    print("GCG_ChargeOut_SAP: ", df6)
