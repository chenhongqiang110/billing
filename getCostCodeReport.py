import pandas as pd
# from datetime import datetime
from dataLoader import DataLoader

# 实例化 DataLoader
data_loader = DataLoader()


def get_dataframe_Cost_Code_CIC_BMS(cost_codes):
    if isinstance(cost_codes, str):
        cost_codes = [cost_codes]
    df = data_loader.get_dataframe("CIC_BMS")
    filtered_df = df[df['Cost Code'].isin(cost_codes)]
    return filtered_df


def get_dataframe_Cost_Code_GCG_BMS(cost_codes):
    if isinstance(cost_codes, str):
        cost_codes = [cost_codes]
    df = data_loader.get_dataframe("GCG_BMS")
    filtered_df = df[df['Cost Code'].isin(cost_codes)]
    return filtered_df


def get_dataframe_Cost_Code_CIC_ISB_Chargeout(cost_codes):
    if isinstance(cost_codes, str):
        cost_codes = [cost_codes]
    df = data_loader.get_dataframe("CIC_ChargeOut_ISB")
    filtered_df = df[df['Project Name'].apply(lambda x: any(pid in x for pid in cost_codes))]
    return filtered_df


def get_dataframe_Cost_Code_GCG_ISB_Chargeout(cost_codes):
    if isinstance(cost_codes, str):
        cost_codes = [cost_codes]
    df = data_loader.get_dataframe("GCG_ChargeOut_ISB")
    filtered_df = df[df['Project Name'].apply(lambda x: any(pid in x for pid in cost_codes))]
    return filtered_df


def get_dataframe_Cost_Code_CIC_SAP_Chargeout(cost_codes):
    if isinstance(cost_codes, str):
        cost_codes = [cost_codes]
    df = data_loader.get_dataframe("CIC_ChargeOut_SAP")
    filtered_df = df[df['Project Name'].apply(lambda x: any(pid in x for pid in cost_codes))]
    return filtered_df


def get_dataframe_Cost_Code_GCG_SAP_Chargeout(cost_codes):
    if isinstance(cost_codes, str):
        cost_codes = [cost_codes]
    df = data_loader.get_dataframe("GCG_ChargeOut_SAP")
    filtered_df = df[df['Project Name'].apply(lambda x: any(pid in x for pid in cost_codes))]
    return filtered_df


def write_Cost_Code_data_to_excel(cost_codes):
    file_path_Cost_Code = []
    result_list = []
    if isinstance(cost_codes, str):
        cost_codes = [cost_codes]

    try:
        # 获取数据
        df_CIC_BMS = get_dataframe_Cost_Code_CIC_BMS(cost_codes)
        df_GCG_BMS = get_dataframe_Cost_Code_GCG_BMS(cost_codes)
        df_CIC_ISB_Chargeout = get_dataframe_Cost_Code_CIC_ISB_Chargeout(cost_codes)
        df_CIC_SAP_Chargeout = get_dataframe_Cost_Code_CIC_SAP_Chargeout(cost_codes)
        df_GCG_ISB_Chargeout = get_dataframe_Cost_Code_GCG_ISB_Chargeout(cost_codes)
        df_GCG_SAP_Chargeout = get_dataframe_Cost_Code_GCG_SAP_Chargeout(cost_codes)

        # 检查是否所有数据都为空
        if df_CIC_BMS.empty and df_GCG_BMS.empty and df_CIC_ISB_Chargeout.empty and df_CIC_SAP_Chargeout.empty and df_GCG_ISB_Chargeout.empty and df_GCG_SAP_Chargeout.empty:
            print("当前输入的cost_codes查找的Cost_Code_Report没有数据")
            result_list.append("NG")
        else:
            # 创建DataFrame副本以避免SettingWithCopyWarning
            df_CIC_BMS = df_CIC_BMS.copy()
            df_GCG_BMS = df_GCG_BMS.copy()
            df_CIC_ISB_Chargeout = df_CIC_ISB_Chargeout.copy()
            df_CIC_SAP_Chargeout = df_CIC_SAP_Chargeout.copy()
            df_GCG_ISB_Chargeout = df_GCG_ISB_Chargeout.copy()
            df_GCG_SAP_Chargeout = df_GCG_SAP_Chargeout.copy()
            # 数据去重
            df_CIC_BMS.drop_duplicates(inplace=True)
            df_GCG_BMS.drop_duplicates(inplace=True)
            df_CIC_ISB_Chargeout.drop_duplicates(inplace=True)
            df_CIC_SAP_Chargeout.drop_duplicates(inplace=True)
            df_GCG_ISB_Chargeout.drop_duplicates(inplace=True)
            df_GCG_SAP_Chargeout.drop_duplicates(inplace=True)

            # 设置文件路径，添加时间戳防止覆盖
            # timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_path_Cost_Code = f'report/Cost_Code_Report.xlsx'
            # 写入Excel文件
            with pd.ExcelWriter(file_path_Cost_Code) as writer:
                if not df_CIC_BMS.empty:
                    df_CIC_BMS.to_excel(writer, sheet_name='CIC_BMS', index=False)
                if not df_GCG_BMS.empty:
                    df_GCG_BMS.to_excel(writer, sheet_name='GCG_BMS', index=False)
                if not df_CIC_ISB_Chargeout.empty:
                    df_CIC_ISB_Chargeout.to_excel(writer, sheet_name='CIC_ISB_Chargeout', index=False)
                if not df_GCG_ISB_Chargeout.empty:
                    df_GCG_ISB_Chargeout.to_excel(writer, sheet_name='GCG_ISB_Chargeout', index=False)
                if not df_CIC_SAP_Chargeout.empty:
                    df_CIC_SAP_Chargeout.to_excel(writer, sheet_name='CIC_SAP_Chargeout', index=False)
                if not df_GCG_SAP_Chargeout.empty:
                    df_GCG_SAP_Chargeout.to_excel(writer, sheet_name='GCG_SAP_Chargeout', index=False)
            print("查出的数据生成的Cost_Code_Report已保存在本地")
            result_list.append("OK")
            file_path_Cost_Code = [file_path_Cost_Code]
    except Exception as e:
        print(f"程序出错: {e}")

    return file_path_Cost_Code, result_list


if __name__ == "__main__":
    ids = ['CTR018144628CIC', 'SHF224030001GCG', '2403641BISX0005P3I', 'TRTA24050009GCG']
    # df1 = get_dataframe_Cost_Code_CIC_BMS(ids)
    # df2 = get_dataframe_Cost_Code_GCG_BMS(ids)

    # df3 = get_dataframe_Cost_Code_CIC_ISB_Chargeout(ids)
    # df4 = get_dataframe_Cost_Code_CIC_SAP_Chargeout(ids)
    # df5 = get_dataframe_Cost_Code_GCG_ISB_Chargeout(ids)
    # df6 = get_dataframe_Cost_Code_GCG_SAP_Chargeout(ids)
    # print("CIC_BMS: ", df1)
    # print("GCG_BMS: ", df2)

    # print("CIC_ChargeOut_ISB: ", df3)
    # print("GCG_ChargeOut_SAP: ", df4)
    # print("CIC_ChargeOut_ISB: ", df5)
    # print("GCG_ChargeOut_SAP: ", df6)

    write_Cost_Code_data_to_excel(ids)
