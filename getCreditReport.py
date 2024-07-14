import pandas as pd
# from datetime import datetime
from dataLoader import DataLoader

# 实例化 DataLoader
data_loader = DataLoader()

def get_dataframe_Manual_Need_Confirmation():
    df = data_loader.get_dataframe("Need_Confirmation")
    print("invoice的值0000000000000000000：", df['Billing Invoice Number'])

    # 使用字符串操作 str.startswith() 进行匹配
    filtered_df = df[(df['Remark5 (Billing Type)'].str.lower() == 'credit')]
    print("invoice的值555555555555555：", filtered_df['Billing Invoice Number'])

    # # 确保 Billing Invoice Number 字段保持原始格式，将其重新转换为字符串
    # filtered_df['Billing Invoice Number'] = filtered_df['Billing Invoice Number'].astype(str)
    # # 确保 Billing Invoice Number 字段保持原始格式
    # # filtered_df.loc[:, 'Billing Invoice Number'] = filtered_df['Billing Invoice Number'].apply(lambda x: f"'{x}")
    return filtered_df


def write_Need_Confirmation_data_to_excel(time_range_from_billing_type_search, months_filter_for_Need_Confirmation):

    file_path_Credit = []
    result_list = []

    try:
        # 获取数据
        df_Need_Confirmation = get_dataframe_Manual_Need_Confirmation()
        # print("invoice的值2：", df_Need_Confirmation['Billing Invoice Number'])

        # 检查是否所有数据都为空
        if df_Need_Confirmation.empty:
            print("所有数据均为空，程序退出")
            result_list.append("NG")
            return

        # 初始化Dataframe
        combined_df_Need_Confirmation = pd.DataFrame()

        # 处理 Need_Confirmation 数据
        if months_filter_for_Need_Confirmation is not None:
            combined_df_Need_Confirmation = df_Need_Confirmation[df_Need_Confirmation['Billing Cycle'].isin(months_filter_for_Need_Confirmation)]
            # 创建DataFrame副本以避免SettingWithCopyWarning
            combined_df_Need_Confirmation = combined_df_Need_Confirmation.copy()
            # 数据去重
            combined_df_Need_Confirmation.drop_duplicates(inplace=True)

        if combined_df_Need_Confirmation.empty:
            result_list.append("NG")
            print("当前输入的月份查找的Credit_Report没有数据")
        else:
            # 设置文件路径，添加时间戳防止覆盖
            # timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_path_Credit = f'report/Credit_Report_{time_range_from_billing_type_search}.xlsx'
            # 写入Excel文件
            with pd.ExcelWriter(file_path_Credit) as writer:
                if not combined_df_Need_Confirmation.empty:
                    combined_df_Need_Confirmation.to_excel(writer, sheet_name='Credit', index=False)
            print("查出的数据生成的Credit_Report已保存在本地")
            result_list.append("OK")
            file_path_Credit = [file_path_Credit]
    except Exception as e:
        print(f"程序出错: {e}")

    return file_path_Credit, result_list



if __name__ == "__main__":
    df1 = get_dataframe_Manual_Need_Confirmation()
    print("Need_Confirmation: ", df1)

