import pandas as pd
# from datetime import datetime
from dataLoader import DataLoader

# 实例化 DataLoader
data_loader = DataLoader()


def get_dataframe_Invoice_Number_CIC_BMS(invoice_number):
    df = data_loader.get_dataframe("CIC_BMS")
    filtered_df = df[df['Cias Invoice Num'].isin(invoice_number)]
    return filtered_df


def get_dataframe_Invoice_Number_GCG_BMS(invoice_number):
    df = data_loader.get_dataframe("GCG_BMS")
    filtered_df = df[df['Cias Invoice Num'].isin(invoice_number)]
    return filtered_df


def get_dataframe_Invoice_Number_CIC_ISB_Chargeout(invoice_number):
    df = data_loader.get_dataframe("CIC_ChargeOut_ISB")
    filtered_df = df[df['Invoice Number'].isin(invoice_number)]
    return filtered_df


def get_dataframe_Invoice_Number_GCG_ISB_Chargeout(invoice_number):
    df = data_loader.get_dataframe("GCG_ChargeOut_ISB")
    filtered_df = df[df['Invoice Number'].isin(invoice_number)]
    return filtered_df


def get_dataframe_Invoice_Number_CIC_SAP_Chargeout(invoice_number):
    df = data_loader.get_dataframe("CIC_ChargeOut_SAP")
    filtered_df = df[df['Invoice Number'].isin(invoice_number)]
    return filtered_df


def get_dataframe_Invoice_Number_GCG_SAP_Chargeout(invoice_number):
    df = data_loader.get_dataframe("GCG_ChargeOut_SAP")
    filtered_df = df[df['Invoice Number'].isin(invoice_number)]
    return filtered_df


def write_Invoice_Number_data_to_excel(invoice_number):
    file_path_Invoice_Number = []
    result_list = []
    if isinstance(invoice_number, (str,int)):
        invoice_number = [invoice_number]

    try:
        # 获取数据
        df_CIC_BMS = get_dataframe_Invoice_Number_CIC_BMS(invoice_number)
        df_GCG_BMS = get_dataframe_Invoice_Number_GCG_BMS(invoice_number)
        df_CIC_ISB_Chargeout = get_dataframe_Invoice_Number_CIC_ISB_Chargeout(invoice_number)
        df_CIC_SAP_Chargeout = get_dataframe_Invoice_Number_CIC_SAP_Chargeout(invoice_number)
        df_GCG_ISB_Chargeout = get_dataframe_Invoice_Number_GCG_ISB_Chargeout(invoice_number)
        df_GCG_SAP_Chargeout = get_dataframe_Invoice_Number_GCG_SAP_Chargeout(invoice_number)

        # 检查是否所有数据都为空
        if df_CIC_BMS.empty and df_GCG_BMS.empty and df_CIC_ISB_Chargeout.empty and df_CIC_SAP_Chargeout.empty and df_GCG_ISB_Chargeout.empty and df_GCG_SAP_Chargeout.empty:
            print("当前输入的invoice_number查找的Invoice_Number_Report没有数据")
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
            file_path_Invoice_Number = f'report/Invoice_Number_Report.xlsx'
            # 写入Excel文件
            with pd.ExcelWriter(file_path_Invoice_Number) as writer:
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
            print("查出的数据生成的Invoice_Number_Report已保存在本地")
            result_list.append("OK")
            file_path_Invoice_Number = [file_path_Invoice_Number]
    except Exception as e:
        print(f"程序出错: {e}")

    return file_path_Invoice_Number, result_list


if __name__ == "__main__":
    ids = ['0600365596', '600362686', '600362715', 'TRTA24050009GCG','600362697','600379301','0600363126']

    # ids = ['a600363051', 'b600363060']
    write_Invoice_Number_data_to_excel(ids)