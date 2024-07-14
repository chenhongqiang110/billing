import pandas as pd
# from datetime import datetime
from dataLoader import DataLoader
from utils.commonFunction import get_month_range

# 实例化 DataLoader
data_loader = DataLoader()


def get_dataframe_AutoLabor_CIC_BMS():
    df = data_loader.get_dataframe("CIC_BMS")
    # 先筛选出“Work Item Id”列值不为空的行数据
    df_non_empty = df[df['Work Item Id'].notna()]
    # “Billing Status”下的数据值等于“successful”
    filtered_df = df_non_empty[df_non_empty['Billing Status'].str.lower() == 'successful']
    return filtered_df


def get_dataframe_AutoLabor_GCG_BMS():
    df = data_loader.get_dataframe("GCG_BMS")
    # 先筛选出“Work Item Id”列值不为空的行数据
    df_non_empty = df[df['Work Item Id'].notna()]
    # “Billing Status”下的数据值等于“successful”
    filtered_df = df_non_empty[df_non_empty['Billing Status'].str.lower() == 'successful']
    return filtered_df


def get_dataframe_AutoLabor_CIC_CATS():
    df = data_loader.get_dataframe("CIC_CATS_Labor")
    # CATS Report无筛选条件， 把CIC和GCG下CATS Report的Labor sheet页面的数据全部抓取出来
    filtered_df = df
    return filtered_df


def get_dataframe_AutoLabor_GCG_CATS():
    df = data_loader.get_dataframe("GCG_CATS_Labor")
    # CATS Report无筛选条件， 把CIC和GCG下CATS Report的Labor sheet页面的数据全部抓取出来
    filtered_df = df
    return filtered_df

def write_AutoLabor_data_to_excel(time_range_from_billing_type_search, months_filter_for_BMS_CATS):

    file_path_AutoLabor = []
    result_list = []

    try:
        # 获取数据
        df_CIC_BMS = get_dataframe_AutoLabor_CIC_BMS()
        df_GCG_BMS = get_dataframe_AutoLabor_GCG_BMS()
        df_CIC_CATS = get_dataframe_AutoLabor_CIC_CATS()
        df_GCG_CATS = get_dataframe_AutoLabor_GCG_CATS()

        # 检查是否所有数据都为空
        if df_CIC_BMS.empty and df_GCG_BMS.empty and df_CIC_CATS.empty and df_CIC_CATS.empty:
            print("所有数据均为空，程序退出")
            result_list.append("NG")
            return

        # 初始化Dataframe
        combined_df_CIC_BMS = pd.DataFrame()
        combined_df_GCG_BMS = pd.DataFrame()
        combined_df_CIC_CATS = pd.DataFrame()
        combined_df_GCG_CATS = pd.DataFrame()

        if months_filter_for_BMS_CATS is not None:
            # 处理 BMS 数据
            combined_df_CIC_BMS = df_CIC_BMS[df_CIC_BMS['Ledger Month Num'].isin(months_filter_for_BMS_CATS)]
            combined_df_GCG_BMS = df_GCG_BMS[df_GCG_BMS['Ledger Month Num'].isin(months_filter_for_BMS_CATS)]

            # 创建DataFrame副本以避免SettingWithCopyWarning
            combined_df_CIC_BMS = combined_df_CIC_BMS.copy()
            combined_df_GCG_BMS = combined_df_GCG_BMS.copy()
            # 数据去重
            combined_df_CIC_BMS.drop_duplicates(inplace=True)
            combined_df_GCG_BMS.drop_duplicates(inplace=True)

            # 处理 CATS 数据
            combined_df_CIC_CATS = df_CIC_CATS[df_CIC_CATS['Ledger Month'].isin(months_filter_for_BMS_CATS)]
            combined_df_GCG_CATS = df_GCG_CATS[df_GCG_CATS['Ledger Month'].isin(months_filter_for_BMS_CATS)]

            # 创建DataFrame副本以避免SettingWithCopyWarning
            combined_df_CIC_CATS = combined_df_CIC_CATS.copy()
            combined_df_GCG_CATS = combined_df_GCG_CATS.copy()
            # 数据去重
            combined_df_CIC_CATS.drop_duplicates(inplace=True)
            combined_df_GCG_CATS.drop_duplicates(inplace=True)

        if combined_df_CIC_BMS.empty and combined_df_GCG_BMS.empty and combined_df_CIC_CATS.empty and combined_df_GCG_CATS.empty:
            result_list.append("NG")
            print("当前输入的月份查找的AutoLabor_Report没有数据")
        else:
            # 设置文件路径，添加时间戳防止覆盖
            # timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_path_AutoLabor = f'report/AutoLabor_Report_{time_range_from_billing_type_search}.xlsx'
                # 写入Excel文件
            with pd.ExcelWriter(file_path_AutoLabor) as writer:
                if not combined_df_CIC_BMS.empty:
                    combined_df_CIC_BMS.to_excel(writer, sheet_name='CIC_BMS', index=False)
                if not combined_df_GCG_BMS.empty:
                    combined_df_GCG_BMS.to_excel(writer, sheet_name='GCG_BMS', index=False)
                if not combined_df_CIC_CATS.empty:
                    combined_df_CIC_CATS.to_excel(writer, sheet_name='CIC_Labor_CATS', index=False)
                if not combined_df_GCG_CATS.empty:
                    combined_df_GCG_CATS.to_excel(writer, sheet_name='GCG_Labor_CATS', index=False)

            print("查出的数据生成的AutoLabor_Report已保存在本地")
            result_list.append("OK")
            file_path_AutoLabor = [file_path_AutoLabor]
    except Exception as e:
        print(f"程序出错: {e}")

    return file_path_AutoLabor, result_list


if __name__ == "__main__":
    # df1 = get_dataframe_AutoLabor_CIC_BMS()
    # df2 = get_dataframe_AutoLabor_GCG_BMS()
    months_filter_for_BMS_CATS = get_month_range('202401-202402')
    write_AutoLabor_data_to_excel(1,months_filter_for_BMS_CATS)

