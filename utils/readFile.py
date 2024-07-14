import pandas as pd
import json
import requests
from dataLoader import DataLoader

data_loader = DataLoader()


def read_csv(file_path):
    """
    读取 CSV 文件并返回数据
    :param file_path: CSV 文件的路径
    :return: DataFrame 数据
    """
    try:
        data = pd.read_csv(file_path, encoding='ISO-8859-1', low_memory=False)
        return data
    except Exception as e:
        print(f"读取 CSV 文件时出错: {e}")
        return None


def read_excel(file_path, sheetName):
    """
    读取 Excel 文件并返回数据
    :param file_path: Excel 文件的路径
    :return: DataFrame 数据
    """
    try:
        # data = pd.read_excel(file_path, encoding='ISO-8859-1', low_memory=False)
        data = pd.read_excel(file_path, sheet_name=sheetName)
        return data
    except Exception as e:
        print(f"读取 Excel 文件时出错: {e}")
        return None


#
# def get_intranet_id(notesid):
#     # 01 url defined here
#     intranet_id = ""
#     url_prefix = "https://w3-unifiedprofile-api.dal1a.cirrus.ibm.com/v3/profiles/"
#     url_suffix = "/profile"
#     notesid = notesid.replace(" ", "%20").replace("/", "%2F")
#     my_response = requests.get(url_prefix + notesid + url_suffix)
#     # 03 get and parse the response
#
#     j_data = json.loads(my_response.content)
#     j_status = json.loads(str(my_response.status_code))
#     if j_status == 200:
#         intranet_id = j_data.get('content').get('mail')[0]
#     return intranet_id
#
#
def get_notesid(intranet_id):
    # 01 url defined here
    notesid = ""
    url_prefix = "https://w3-unifiedprofile-api.dal1a.cirrus.ibm.com/v3/profiles/"
    url_suffix = "/profile"
    intranet_id = intranet_id.replace("@", "%40")
    my_response = requests.get(url_prefix + intranet_id + url_suffix)
    # 03 get and parse the response

    j_data = json.loads(my_response.content)
    j_status = json.loads(str(my_response.status_code))
    if j_status == 200:
        notesid = j_data.get('content').get('notesEmail')
    return notesid


def get_intranet_id(notesids):
    # URL部分
    url_prefix = "https://w3-unifiedprofile-api.dal1a.cirrus.ibm.com/v3/profiles/"
    url_suffix = "/profile"
    intranet_ids = set()  # 使用集合以避免重复

    # 确保 notesids 是一个列表
    if not isinstance(notesids, list):
        raise ValueError("输入的 notesids 必须是一个列表")

    for notesid in notesids:
        # 对notesid进行编码
        notesid_encoded = notesid.replace(" ", "%20").replace("/", "%2F")
        # 发送API请求
        my_response = requests.get(url_prefix + notesid_encoded + url_suffix)

        # 解析响应
        if my_response.status_code == 200:
            j_data = my_response.json()
            intranet_id = j_data.get('content', {}).get('mail', [None])[0]
            if intranet_id:
                intranet_ids.add(intranet_id)  # 添加到集合中

    return list(intranet_ids)  # 将集合转换为列表后返回


# def get_notesid1(intranet_ids):
#     # URL部分
#     url_prefix = "https://w3-unifiedprofile-api.dal1a.cirrus.ibm.com/v3/profiles/"
#     url_suffix = "/profile"
#     notes_ids = set()  # 用于存储唯一结果的集合
#
#     # 确保 intranet_ids 是一个列表
#     if not isinstance(intranet_ids, list):
#         raise ValueError("输入的 intranet_ids 必须是一个列表")
#
#     for intranet_id in intranet_ids:
#         # 对 intranet_id 进行编码
#         intranet_id_encoded = intranet_id.replace("@", "%40")
#         # 发送 API 请求
#         my_response = requests.get(url_prefix + intranet_id_encoded + url_suffix)
#
#         # 解析响应
#         if my_response.status_code == 200:
#             j_data = my_response.json()
#             notesid = j_data.get('content', {}).get('notesEmail')
#             if notesid:
#                 notes_ids.add(notesid)  # 添加到集合中
#
#     return list(notes_ids)  # 返回一个列表


# 根据传入的参数找到对应的Role
def get_matching_role(file_path, match_value):
    # 读取CSV文件
    # df = read_csv(file_path)
    df = data_loader.get_dataframe("PM_PMO_PAL")

    # 定义需要匹配的列
    columns_to_check = ['Project ID', 'Project Manager IntranetId', 'PMO IntranetId', 'PAL IntranetId']
    df_temp = df[columns_to_check]
    df_temp = df_temp.reindex(columns=columns_to_check)

    role_list = []

    if df_temp.eq(match_value).any().any() == True:
        if (df_temp.isin([match_value]).any().any() == True):
            list_target_columns = df_temp.columns[df_temp.isin([match_value]).any()].tolist()
            # print(list_target_columns)
            role_list = [role.replace(' IntranetId', '') for role in list_target_columns]
        else:
            print("No results found")
    else:
        print("No results found")

    return role_list


# 根据传入的参数找到对应的Project ID
def get_matching_ProjectID(file_path, match_value):
    # 读取CSV文件
    # df = read_csv(file_path)
    df = data_loader.get_dataframe("PM_PMO_PAL")

    # 定义需要匹配的列
    columns_to_check = ['Project ID', 'Project Manager IntranetId', 'PMO IntranetId', 'PAL IntranetId']
    df_temp = df[columns_to_check]
    df_temp = df_temp.reindex(columns=columns_to_check)

    list_project_id_new = []

    if df_temp.eq(match_value).any().any() == True:
        if (df_temp.isin([match_value]).any().any() == True):
            list_target_columns = df_temp.columns[df_temp.isin([match_value]).any()].tolist()
            for i in range(len(list_target_columns)):
                list_project_id = df_temp[df_temp[list_target_columns[i]] == match_value]['Project ID'].values.tolist()
                list_project_id_new = list_project_id + list_project_id_new
        else:
            print("No results found")
    else:
        print("No results found")
    list_project_id_new = list(set(list_project_id_new))
    return list_project_id_new


def get_AccountID_From_ProjectID(match_values):
    # 读取CSV文件
    # df = read_csv(file_path)
    df = data_loader.get_dataframe("PM_PMO_PAL")

    # 定义需要匹配的列
    columns_to_check = ['Bms Id', 'Project Id']

    df_temp = df[columns_to_check]
    df_temp = df_temp.reindex(columns=columns_to_check)

    list_account_id_new = []

    # 遍历每个match_value
    for match_value in match_values:
        if df_temp.eq(match_value).any().any():
            if df_temp.isin([match_value]).any().any():
                # 获取包含匹配值的列名
                list_target_columns = df_temp.columns[df_temp.isin([match_value]).any()].tolist()
                for column in list_target_columns:
                    # 获取匹配到的Bms Id列表
                    list_account_id = df_temp[df_temp[column] == match_value]['Bms Id'].values.tolist()
                    # 将匹配到的Bms Id添加到新列表中
                    list_account_id_new.extend(list_account_id)
            else:
                print(f"没有找到匹配结果: {match_value}")
        else:
            print(f"没有找到匹配结果: {match_value}")

    # 去重并过滤掉NaN值
    list_account_id_new = list(set(list_account_id_new))
    filtered_account_ids = [account_id for account_id in list_account_id_new if not pd.isna(account_id)]
    return filtered_account_ids


def get_WBSID_From_ProjectID(match_values):
    # 读取CSV文件
    # df = read_csv(file_path)
    df = data_loader.get_dataframe("PM_PMO_PAL")

    # 定义需要匹配的列
    columns_to_check = ['WBS Element Id', 'Project Id']

    df_temp = df[columns_to_check]
    df_temp = df_temp.reindex(columns=columns_to_check)

    list_wbs_id_new = []

    # 遍历每个match_value
    for match_value in match_values:
        if df_temp.eq(match_value).any().any():
            if df_temp.isin([match_value]).any().any():
                # 获取包含匹配值的列名
                list_target_columns = df_temp.columns[df_temp.isin([match_value]).any()].tolist()
                for column in list_target_columns:
                    # 获取匹配到的WBS Element Id列表
                    list_wbs_id = df_temp[df_temp[column] == match_value]['WBS Element Id'].values.tolist()
                    # 将匹配到的WBS Element Id添加到新列表中
                    list_wbs_id_new.extend(list_wbs_id)
            else:
                print(f"没有找到匹配结果: {match_value}")
        else:
            print(f"没有找到匹配结果: {match_value}")

    # 去重并过滤掉NaN值
    list_wbs_id_new = list(set(list_wbs_id_new))
    filtered_wbs_ids = [wbs_id for wbs_id in list_wbs_id_new if not pd.isna(wbs_id)]
    return filtered_wbs_ids


def get_notes_id_by_project_id(project_ids):
    # df = read_csv(file_path)
    df = data_loader.get_dataframe("PM_PMO_PAL")
    if isinstance(project_ids, str):
        project_ids = [project_ids]

    filtered_df = df[df["Project Id"].isin(project_ids)]
    notes_ids = list(set(filtered_df["PM Notes Id"].tolist()))

    return notes_ids


def getUser(user_intranet_id):
    df = data_loader.get_dataframe("PM_PMO_PAL")
    columns_to_check = ['PM Notes Id', 'PMO Notes Id', 'PAL Notes Id']
    df_temp = df[columns_to_check]
    df_temp = df_temp.reindex(columns=columns_to_check)

    user_notes_id = get_notesid(user_intranet_id)

    if df_temp.eq(user_notes_id).any().any():
        user_existed = 'Y'
        print("当前User在人员表里可以查询得到")
    else:
        user_existed = 'N'
        print("当前User在人员表里查不到")

    return user_existed


def getUserRole(user_intranet_id):
    # df = read_csv(file_path)
    df = data_loader.get_dataframe("PM_PMO_PAL")
    columns_to_check = ['Project Id', 'PM Notes Id', 'PMO Notes Id', 'PAL Notes Id']
    df_temp = df[columns_to_check]
    df_temp = df_temp.reindex(columns=columns_to_check)

    user_notes_id = get_notesid(user_intranet_id)

    role_list = []

    if df_temp.eq(user_notes_id).any().any():
        if df_temp.isin([user_notes_id]).any().any():
            list_target_columns = df_temp.columns[df_temp.isin([user_notes_id]).any()].tolist()
            # print(list_target_columns)
            role_list = [role.replace(' Notes Id', '') for role in list_target_columns]
        else:
            print("No results found1")
    else:
        print("No results found2")

    return role_list


def getUserRoleProjectID(user_intranet_id):
    # df = read_csv(file_path)
    df = data_loader.get_dataframe("PM_PMO_PAL")
    columns_to_check = ['Project Id', 'PM Notes Id', 'PMO Notes Id', 'PAL Notes Id']
    df_temp = df[columns_to_check]
    df_temp = df_temp.reindex(columns=columns_to_check)

    user_notes_id = get_notesid(user_intranet_id)

    list_project_id_new = []

    if df_temp.eq(user_notes_id).any().any():
        if df_temp.isin([user_notes_id]).any().any():
            list_target_columns = df_temp.columns[df_temp.isin([user_notes_id]).any()].tolist()
            for i in range(len(list_target_columns)):
                list_project_id = df_temp[df_temp[list_target_columns[i]] == user_notes_id][
                    'Project Id'].values.tolist()
                list_project_id_new = list_project_id + list_project_id_new
        else:
            print("No results found")
    else:
        print("No results found")
    list_project_id_new = list(set(list_project_id_new))
    return list_project_id_new


def getUserIDFromUnregister(account_ids, wbs_ids):
    # 读取Excel文件中的指定工作表
    # df = pd.read_excel(file_path, sheet_name="Sheet1")
    df = data_loader.get_dataframe("Unregister")
    columns_to_check = ['Account ID', 'Wbs Element', 'Project Focal internal']
    df_temp = df[columns_to_check]

    # 用于收集唯一的intranet ID的集合
    intranet_ids = set()

    # 如果account_ids是字符串，将其转换为列表
    if isinstance(account_ids, str):
        account_ids = [account_ids]

    # 如果wbs_ids是字符串，将其转换为列表
    if isinstance(wbs_ids, str):
        wbs_ids = [wbs_ids]

    # 在'Account ID'列中检查account_ids
    for account_id in account_ids:
        account_id_match = df_temp[df_temp['Account ID'] == account_id]
        if not account_id_match.empty:
            intranet_ids.update(account_id_match['Project Focal internal'].values)

    # 在'Wbs Element'列中检查wbs_ids
    for wbs_id in wbs_ids:
        wbs_id_match = df_temp[df_temp['Wbs Element'] == wbs_id]
        if not wbs_id_match.empty:
            intranet_ids.update(wbs_id_match['Project Focal internal'].values)

    # 如果intranet_ids为空，打印提示信息
    if not intranet_ids:
        print("Account ID或者WBS ID不存在。")
        return None
    else:
        return list(intranet_ids)

def getAccountIDFromUnregister(wbs_ids):
    # 读取Excel文件中的指定工作表
    # df = pd.read_excel(file_path, sheet_name="Sheet1")
    df = data_loader.get_dataframe("Unregister")
    columns_to_check = ['Account ID', 'Wbs Element']
    df_temp = df[columns_to_check]

    # 用于收集唯一的account_ids的集合
    account_ids = set()

    # 如果wbs_ids是字符串，将其转换为列表
    if isinstance(wbs_ids, str):
        wbs_ids = [wbs_ids]

    # 在'Wbs Element'列中检查wbs_ids
    for wbs_id in wbs_ids:
        wbs_id_match = df_temp[df_temp['Wbs Element'] == wbs_id]
        if not wbs_id_match.empty:
            account_ids.update(wbs_id_match['Account ID'].values)

    # 如果intranet_ids为空，打印提示信息
    if not account_ids:
        print("通过前端传入的 WBS_ID 搜索与之对应的 AccountID， 没有返回结果。")
        return None
    else:
        return list(account_ids)


def get_filter_gbs_values(file_path, sheet_name='Sheet1'):
    # 读取Excel文件
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    # 假设列名是 'Request ID'
    column_name = 'Request ID'

    # 筛选出以 'GBS' 开头的值
    gbs_values = df[df[column_name].str.startswith('GBS')]

    return gbs_values

def check_ica_ids(ica_id, master_ica):
    if isinstance(ica_id, str):
        ica_id = [ica_id]

    # 检查是否有值存在于master_ica中
    for item in ica_id:
        if item in master_ica:
            return True
    return False

def getUserRoleProjectID1(file_path, user_intranet_id):
    df = read_csv(file_path)
    # 从指定的来源加载数据框
    # df = data_loader.get_dataframe("PM_PMO_PAL")

    # 需要检查的列
    columns_to_check = ['Project Id', 'PM Notes Id', 'PMO Notes Id', 'PAL Notes Id']

    # 创建一个包含相关列的临时数据框
    df_temp = df[columns_to_check]

    # 使用外部方法获取 user_notes_id
    user_notes_id = get_notesid(user_intranet_id)
    print("user_notes_id:", user_notes_id)

    # user_notes_id = 'Min MF Fan/China/IBM'

    # 初始化一个字典来存储结果
    role_dict = {}

    # 遍历每个需要检查的列
    for column in ['PM Notes Id', 'PMO Notes Id', 'PAL Notes Id']:
        # 过滤出 user_notes_id 与列值匹配的行
        matching_rows = df_temp[df_temp[column] == user_notes_id]

        # 对于每一行匹配的记录，提取相关的值
        for index, row in matching_rows.iterrows():
            # 获取列名并去除 " Notes Id"
            role_name = column.replace(" Notes Id", "")
            # 获取 Project Id
            project_id = row['Project Id']

            # 如果角色名不在字典中，则初始化一个集合
            if role_name not in role_dict:
                role_dict[role_name] = set()

            # 将 Project Id 添加到角色名对应的集合中（去重）
            role_dict[role_name].add(project_id)

    # 将字典转换为最终的结果列表
    role_list = [[role, *sorted(ids)] for role, ids in role_dict.items()]

    # 返回最终的角色和项目ID列表
    return role_list



# Example usage:
if __name__ == "__main__":
    excel_file = '../data/user_data.xlsx'
    file_path1 = '../data/Project_MasterDtlReportD.csv'
    file_path2 = '../data/BmsDtlReportD.csv'
    file_path_Unregister = '../data/Rowdata/2401/2024 Unregister project contact focal with Code.xlsx'
    read_file_path_ChargeOut = '../data/Rowdata/2401/CIC GBS-CIC Manual Charge Out Report May.2024.xlsx'

    # json_message_str = '{"message": ["Query Excel", "whzb@cn.ibm.com     ", ["CN-09-00362", "     CN-09-00398", "CN-14-06223", "CN-11-02020    "], "OC3730", "C.25NZY.001", ["641LE091", "   641LE239", "7600146141    ", "    7600063012"], "2020403"]}'
    # json_message_str = '{"message": ["Query Excel", "whzb@cn.ibm.com     ", "NA", ["OC3730", "     01X-MH3", "01M-DWG     "], ["B.38214", "     C.25NZY.001", "C.25DNH.018     "], "NA", ""]}'

    json_message_str = '{"message": ["Query Excel", "minfan@cn.ibm.com     ", "NA", "OC3730", "C.25NZY.001", "NA", ""]}'


