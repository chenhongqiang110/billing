from urllib import request
import pandas as pd
from WorkFlow_Registration import test
from getAutoLaborReport import write_AutoLabor_data_to_excel
from getAutoWWERReport import write_AutoWWER_data_to_excel
from getCostCodeReport import write_Cost_Code_data_to_excel
from getCreditReport import write_Need_Confirmation_data_to_excel
from getInvoiceNumberReport import write_Invoice_Number_data_to_excel
from getManualReport import write_Manual_data_to_excel
from getTEAReport import write_TEA_data_to_excel
from sendReportEmail import sendMail_Report
from sendVerificationEmail import sendMail_Verification
from sendmailtest import sendMailtest
from utils.commonFunction import get_label_from_command, get_jsonMessage_by_index, validate_string, get_month_range, \
    get_month_range_for_ChargeOut, get_month_range_for_Need_Confirmation
from flask import Flask, request, jsonify
from utils.readFile import getUser, getUserRole, getUserRoleProjectID, getUserIDFromUnregister, get_intranet_id, \
    get_AccountID_From_ProjectID, get_WBSID_From_ProjectID, get_notes_id_by_project_id, getAccountIDFromUnregister, \
    check_ica_ids
from dataLoader import DataLoader

# 前台联调
app = Flask(__name__)
data_loader = DataLoader()
master_ica = ['7600006870', '7600006871', '7600155148', '7600010463', '7600066801', '7600010464', '7600056823',
              '7600127181', '7600118890']


# @app.route('/message', methods=['POST'])
@app.route('/', methods=['POST'])
def post_billing():
    data = request.get_json()
    # data = {"message": ["Query Excel", "whzb@cn.ibm.com", "CN-14-05103", "OC3730", "C.25NZY.001", "NA", ""]}
    print("data:", data)
    target = get_jsonMessage_by_index(data, 0)
    print("target: ", target)
    choice = get_label_from_command(target)
    print("choice: ", choice)
    username = get_jsonMessage_by_index(data, 1)

    t = test()
    verification_code = t.generate_and_store_verification_code(username)
    # print("verification_code:", verification_code)

    excel_file = 'data/user_data.xlsx'
    result_list = []
    file_path_Attachement = []

    file_path_BMS = ''
    file_path_CATS = ''
    file_path_ChOut = ''

    # 初始化一个空的 DataFrame
    combined_df_BMS = pd.DataFrame()
    combined_df_CATS_Labor = pd.DataFrame()
    combined_df_CATS_WWER = pd.DataFrame()
    combined_df_CHargeOut_ISB = pd.DataFrame()
    combined_df_CHargeOut_SAP = pd.DataFrame()

    # 注册流程之前的用户校验
    if choice == 'V':  # 注册流程之前的用户校验
        user_is_existed = t.check_username_in_excel(username)
        # print("user_is_existed:", user_is_existed)

        if user_is_existed == 'N':
            sendMail_Verification("tingliu@cn.ibm.com", username, verification_code)
            result_list = 'OK'
            result_list = [result_list, verification_code]
        else:
            result_list.append("NG")

    # 用户登录成功流程
    elif choice == 'L':  # 用户登录成功流程
        uid = get_jsonMessage_by_index(data, 1)
        pwd = get_jsonMessage_by_index(data, 2)
        label = t.userFlagVerification(uid, pwd)
        print("label:", label)

        if label == 'Y':  # User邮箱地址存在user_data表里
            user_is_existed = getUser(uid)  # 判断用户是否存在BMS人员表当中
            if user_is_existed == 'Y':
                user_role = getUserRole(uid)
                user_project_id = getUserRoleProjectID(uid)
                result_list = 'Y'
                result_list = [result_list, user_role, user_project_id]
            else:
                result_list = 'N'
        elif label == 'V':  # User 是VIP用户
            result_list = 'V'
        elif label == 'U':  # User邮箱地址不存在user_data表里
            result_list = 'U'
        elif label == 'P':  # User 登录密码错误
            result_list = 'P'
        elif label == 'F':  # User 登录邮件格式错误
            result_list = 'F'
        else:
            pass

    # 注册流程
    elif choice == 'R':  # 注册流程
        user_name = username
        pwd = get_jsonMessage_by_index(data, 2)
        # print("pwd:", pwd)
        user_input_verification_code = get_jsonMessage_by_index(data, 4)
        verification = get_jsonMessage_by_index(data, 3)
        # user_input_verification_code = "Tu89Zx"
        # verification = "Tu89Zx"
        # print("verification:", verification)
        # print("user_input_verification_code:", user_input_verification_code)

        if user_input_verification_code is not None or verification is not None:
            # if user_input_verification_code.eq(verification).any().any():
            if user_input_verification_code == verification:
                # 这里filepath不能删的原因是因为要往表里写用户的数据 (用户id和密码)
                t.add_user_to_excel(user_name, pwd, excel_file)
                result_list.append("OK")
            else:
                print("验证码不一致，无法注册。")
                result_list.append("NG")

        else:
            print("传入的验证码为空值，校验不成功。")

    # 修改密码流程
    elif choice == 'M':  # 修改密码流程
        user_name = username
        new_password = get_jsonMessage_by_index(data, 2)
        verify = validate_string(new_password)
        if verify:
            # 这里filepath不能删的原因是因为要往表里写用户的数据 (用户id和密码)
            t.add_user_to_excel_modify_pwd(user_name, new_password, excel_file)
            result_list.append("OK")
        else:
            result_list.append("NG")

    # 项目注册与否(是否有AccountID和WBSID)
    elif choice == 'U':  # 项目注册与否（是否有AccountID和WBSID）
        accountid = get_jsonMessage_by_index(data, 1)
        wbsid = get_jsonMessage_by_index(data, 2)
        if accountid == 'NA' and wbsid == 'NA':
            result_list.append("NG")
        else:
            # 判断有数据
            hasData = getUserIDFromUnregister(accountid, wbsid)
            if hasData is None:
                result_list.append("NG")
            else:
                result_list.append("OK")

    # 用户登录进去然后开始查询
    elif choice == 'Q_M':  # 用户登录进去然后开始查询
        receiver_email = get_jsonMessage_by_index(data, 1)
        project_id = get_jsonMessage_by_index(data, 2)
        account_id = get_jsonMessage_by_index(data, 3)
        wbs_id = get_jsonMessage_by_index(data, 4)
        ica_id = get_jsonMessage_by_index(data, 5)
        time_range = get_jsonMessage_by_index(data, 6)
        print("前端传递过来的_project_id:", project_id)
        print("前端传递过来的_account_id:", account_id)
        print("前端传递过来的_wbs_id:", wbs_id)
        print("前端传递过来的_ica_id:", ica_id)
        print("前端传递过来的时间段:", time_range)

        if not time_range:  # 输入的时间字段为空值，不做任何查询直接返回NG
            result_list.append("NG")
            print("PM Report Search: 输入的时间字段为空值，不做任何查询直接返回NG")
        else:
            months_filter_for_BMS_CATS = get_month_range(time_range)
            months_filter_for_ChargeOut = get_month_range_for_ChargeOut(time_range)

            # 输入的时间字段有值但值不规范则直接返回NG 否则进行report条件搜索
            if months_filter_for_BMS_CATS is not None or months_filter_for_ChargeOut is not None:

                # Project ID 为NA的分支
                if not project_id or project_id == 'NA':  # Project ID 不存在的分支

                    # 前端传入的WBSID有值
                    if wbs_id and wbs_id != 'NA':  # 前端传入的WBSID有值
                        print("wbs_id输入有值")
                        if isinstance(wbs_id, str):
                            wbs_id = [wbs_id]
                        # print("Project ID 为NA的分支_wbs_id:", wbs_id)
                        account_id_From_WBSID = getAccountIDFromUnregister(wbs_id)
                        # print("Project ID 为NA的分支下通过传入的wbs_id去Unregister表里查找出的AccountID:", account_id_From_WBSID)
                        # 如果用户仅输入wbs_id，先通过传入的WBSID去Unregistered表里面查找出对应的AccountID
                        # 然后再查找BMS_Report 字段列名-“AccountId”
                        #
                        #
                        df = data_loader.get_dataframe("CIC_BMS")
                        if account_id_From_WBSID is not None:
                            filtered_df_BMS = df[df['AccountId'].isin(account_id_From_WBSID) & (
                                    df['Billing Status'].str.lower() == 'successful')]

                            if filtered_df_BMS.empty:
                                # print("当前传入的WBS_ID去Unregister表里查找出的AccountID在BMS_Report里查找不到数据")
                                pass
                            else:
                                combined_df_BMS = pd.concat([combined_df_BMS, filtered_df_BMS], ignore_index=True)
                                print(
                                    "BMS_Report: 以当前传入的WBS_ID去Unregister表里查找出的AccountID来查出的数据已添加到临时DataFrame中")
                        else:
                            print(
                                "当前传入的WBS_ID去Unregister表里查找的AccountID没有返回结果，所以不做BMS_Report数据的查找")
                        # 如果用户仅输入wbs_id，查找CATS_Report 字段列名-“WBS Elem.”
                        #
                        #
                        df = data_loader.get_dataframe("CIC_CATS_Labor")
                        filtered_df_Labor = df[df['WBS Elem.'].isin(wbs_id)]
                        if not filtered_df_Labor.empty:
                            combined_df_CATS_Labor = pd.concat([combined_df_CATS_Labor, filtered_df_Labor],
                                                               ignore_index=True)

                        df = data_loader.get_dataframe("CIC_CATS_WWER")
                        filtered_df_WWER = df[df['WBS Elem.'].isin(wbs_id)]
                        if not filtered_df_WWER.empty:
                            combined_df_CATS_WWER = pd.concat([combined_df_CATS_WWER, filtered_df_WWER],
                                                              ignore_index=True)

                        if filtered_df_Labor.empty and filtered_df_WWER.empty:
                            # print("当前WBS_ID在CATS_Report里查找不到数据")
                            pass
                        else:
                            print("CATS_Report: 以当前WBS_ID查出的数据已添加到临时DataFrame中")

                        # 如果用户仅输入wbs_id，查找Chargeout_Report 字段列名-“PIC ID/Claim ID”
                        #
                        #
                        df = data_loader.get_dataframe("CIC_ChargeOut_ISB")
                        filtered_df_ISB = df[df['PIC ID/Claim ID \n( Client Account ID)'].isin(wbs_id) & (
                                df['Billing Status'].str.lower() == 'successful')]

                        if not filtered_df_ISB.empty:
                            combined_df_CHargeOut_ISB = pd.concat([combined_df_CHargeOut_ISB, filtered_df_ISB],
                                                                  ignore_index=True)

                        df = data_loader.get_dataframe("CIC_ChargeOut_SAP")
                        filtered_df_SAP = df[
                            df['PIC ID/Claim ID'].isin(wbs_id) & (df['Billing Status'].str.lower() == 'successful')]
                        if not filtered_df_SAP.empty:
                            combined_df_CHargeOut_SAP = pd.concat([combined_df_CHargeOut_SAP, filtered_df_SAP],
                                                                  ignore_index=True)

                        if filtered_df_ISB.empty and filtered_df_SAP.empty:
                            # print("当前WBS_ID在ChargeOut_Report里查找不到数据")
                            pass
                        else:
                            print("Chargeout_Report: 以当前WBS_ID查出的数据已添加到临时DataFrame中")

                    # 前端传入的AccountID有值
                    if account_id and account_id != 'NA':  # 前端传入的AccountID为NA
                        print("account_id输入有值")
                        if isinstance(account_id, str):
                            account_id = [account_id]
                        # print("Project ID 为NA的分支_account_id:", account_id)

                        # 如果用户仅输入AccountID，查找BMS_Report, 字段列名-“AccountId”
                        #
                        #

                        df = data_loader.get_dataframe("CIC_BMS")
                        filtered_df_BMS = df[
                            df['AccountId'].isin(account_id) & (df['Billing Status'].str.lower() == 'successful')]

                        if filtered_df_BMS.empty:
                            # print("当前Account ID在BMS_Report里查找不到数据")
                            pass
                        else:
                            combined_df_BMS = pd.concat([combined_df_BMS, filtered_df_BMS], ignore_index=True)
                            print("BMS_Report: 以当前Account ID查出的数据已添加到临时DataFrame中")

                        # 如果用户仅输入AccountID，查找CATS_Report, 字段列名-“Account ID”
                        #
                        #

                        df = data_loader.get_dataframe("CIC_CATS_Labor")
                        filtered_df_Labor = df[df['Account ID'].isin(account_id)]
                        if not filtered_df_Labor.empty:
                            combined_df_CATS_Labor = pd.concat([combined_df_CATS_Labor, filtered_df_Labor],
                                                               ignore_index=True)

                        df = data_loader.get_dataframe("CIC_CATS_WWER")
                        filtered_df_WWER = df[df['Account ID'].isin(account_id)]
                        if not filtered_df_WWER.empty:
                            combined_df_CATS_WWER = pd.concat([combined_df_CATS_WWER, filtered_df_WWER],
                                                              ignore_index=True)

                        if filtered_df_Labor.empty and filtered_df_WWER.empty:
                            # print("当前Account ID在CATS_Report里查找不到数据")
                            pass
                        else:
                            print("CATS_Report: 以当前Account ID查出的数据已添加到临时DataFrame中")

                        # 如果用户仅输入AccountID，查找Chargeout_Report 字段列名-“PIC ID/Claim ID”
                        #
                        #

                        df = data_loader.get_dataframe("CIC_ChargeOut_ISB")
                        filtered_df_ISB = df[df['PIC ID/Claim ID \n( Client Account ID)'].isin(account_id) & (
                                df['Billing Status'].str.lower() == 'successful')]
                        if not filtered_df_ISB.empty:
                            combined_df_CHargeOut_ISB = pd.concat([combined_df_CHargeOut_ISB, filtered_df_ISB],
                                                                  ignore_index=True)
                        df = data_loader.get_dataframe("CIC_ChargeOut_SAP")
                        filtered_df_SAP = df[
                            df['PIC ID/Claim ID'].isin(account_id) & (df['Billing Status'].str.lower() == 'successful')]
                        if not filtered_df_SAP.empty:
                            combined_df_CHargeOut_SAP = pd.concat([combined_df_CHargeOut_SAP, filtered_df_SAP],
                                                                  ignore_index=True)
                        if filtered_df_ISB.empty and filtered_df_SAP.empty:
                            # print("当前Account ID在ChargeOut_Report里查找不到数据")
                            pass
                        else:
                            print("Chargeout_Report: 以当前Account ID查出的数据已添加到临时DataFrame中")

                    focal_id = getUserIDFromUnregister(account_id, wbs_id)

                    # 输入的时间段有值的情况下才做写表的操作， 为空则不做。
                    if time_range:  # 输入的时间段有值的情况下才做写表的操作， 为空则不做。
                        # 将可能找出的BMS_Report保存到同一个工作表
                        if not combined_df_BMS.empty:
                            file_path_BMS = 'report/Bms_Report.xlsx'
                            # 通过用户输入的时间段来过滤 DataFrame
                            months_to_filter = get_month_range(time_range)
                            if months_to_filter is not None:
                                combined_df_BMS = combined_df_BMS[
                                    combined_df_BMS['Ledger Month Num'].isin(months_to_filter)]
                                # 去重 DataFrame 并写表
                                combined_df_BMS.drop_duplicates(inplace=True)
                                combined_df_BMS.to_excel(file_path_BMS, sheet_name='BMS', index=False)
                                print("以当前传入的 Account ID 或 WBS_ID 查出的数据生成的BMS_Report已保存在本地")

                        # 将可能找出的CATS_Report保存到同一个工作表
                        if not combined_df_CATS_Labor.empty or not combined_df_CATS_WWER.empty:
                            file_path_CATS = 'report/CATS_Report.xlsx'
                            # 通过用户输入的时间段来过滤 DataFrame
                            months_to_filter = get_month_range(time_range)
                            if months_to_filter is not None:
                                with pd.ExcelWriter(file_path_CATS, engine='openpyxl') as writer:
                                    if not combined_df_CATS_Labor.empty:
                                        combined_df_CATS_Labor = combined_df_CATS_Labor[
                                            combined_df_CATS_Labor['Ledger Month'].isin(months_to_filter)]
                                        # 去重 DataFrame 并写表
                                        combined_df_CATS_Labor.drop_duplicates(inplace=True)
                                        combined_df_CATS_Labor.to_excel(writer, sheet_name='Labor', index=False)
                                    if not combined_df_CATS_WWER.empty:
                                        combined_df_CATS_WWER = combined_df_CATS_WWER[
                                            combined_df_CATS_WWER['Ledger Month Num'].isin(months_to_filter)]
                                        # 去重 DataFrame 并写表
                                        combined_df_CATS_WWER.drop_duplicates(inplace=True)
                                        combined_df_CATS_WWER.to_excel(writer, sheet_name='WWER', index=False)
                                print("以当前传入的 Account ID 或 WBS_ID 查出的数据生成的CATS_Report已保存在本地")

                        # 将可能找出的ChargeOut_Report保存到同一个工作表
                        if not combined_df_CHargeOut_ISB.empty or not combined_df_CHargeOut_SAP.empty:
                            file_path_ChOut = 'report/ChargeOut_Report.xlsx'
                            # 通过用户输入的时间段来过滤 DataFrame
                            months_to_filter = get_month_range_for_ChargeOut(time_range)
                            if months_to_filter is not None:
                                with pd.ExcelWriter(file_path_ChOut, engine='openpyxl') as writer:
                                    if not combined_df_CHargeOut_ISB.empty:
                                        # 筛选 'Request ID' 列包含 months_to_filter 中任何一个值的行
                                        pattern = '|'.join(months_to_filter)
                                        combined_df_CHargeOut_ISB = combined_df_CHargeOut_ISB[
                                            combined_df_CHargeOut_ISB['Request  ID'].str.contains(pattern)]
                                        # 去重 DataFrame 并写表
                                        combined_df_CHargeOut_ISB.drop_duplicates(inplace=True)
                                        combined_df_CHargeOut_ISB.to_excel(writer, sheet_name='ISB billing',
                                                                           index=False)
                                    if not combined_df_CHargeOut_SAP.empty:
                                        # 筛选 'Request ID' 列包含 months_to_filter 中任何一个值的行
                                        pattern = '|'.join(months_to_filter)
                                        combined_df_CHargeOut_SAP = combined_df_CHargeOut_SAP[
                                            combined_df_CHargeOut_SAP['Request  ID'].str.contains(pattern)]
                                        # 去重 DataFrame 并写表
                                        combined_df_CHargeOut_SAP.drop_duplicates(inplace=True)
                                        combined_df_CHargeOut_SAP.to_excel(writer, sheet_name='SAP billing',
                                                                           index=False)
                                print("以当前传入的 Account ID 或 WBS_ID 查出的数据生成的ChargeOut_Report已保存在本地")
                    else:
                        print("输入的时间字段为空， 无法吧搜索出来的报表数据写入Excel")

                # Project ID 有值的分支
                else:  # Project ID 存在的分支

                    if isinstance(project_id, str):
                        project_id = [project_id]
                    print("Project ID 有值的分支_project_id:", project_id)
                    account_id = get_AccountID_From_ProjectID(project_id)
                    print("Project ID 有值的分支_account_id:", account_id)
                    wbs_id = get_WBSID_From_ProjectID(project_id)
                    print("Project ID 有值的分支_wbs_id:", wbs_id)
                    NotesIDs = get_notes_id_by_project_id(project_id)
                    print("NotesIDs:", NotesIDs)
                    focal_id = get_intranet_id(NotesIDs)
                    print("focal_id:", focal_id)

                    # 如果如果用ProjectID有值的话， 用ProjectID， 查找BMS和Chargeout两张表，忽略CATS
                    #
                    #
                    #
                    # 用ProjectID，查找BMS_Report, 字段列名-“Project ID”
                    #
                    #
                    df = data_loader.get_dataframe("CIC_BMS")
                    filtered_df_BMS = df[
                        df['Project ID'].isin(project_id) & (df['Billing Status'].str.lower() == 'successful')]
                    if filtered_df_BMS.empty:
                        # print("当前 Project ID 在BMS_Report里查找不到数据")
                        pass
                    else:
                        combined_df_BMS = pd.concat([combined_df_BMS, filtered_df_BMS], ignore_index=True)
                        print("BMS_Report: 以当前Project ID查出的数据已添加到临时DataFrame中")
                    # 用ProjectID，查找Chargeout_Report 字段列名-“Project Name”
                    #
                    #
                    df = data_loader.get_dataframe("CIC_ChargeOut_ISB")
                    filtered_df_ISB = df[df['Project Name'].apply(lambda x: any(pid in x for pid in project_id)) & (
                            df['Billing Status'].str.lower() == 'successful')]
                    if not filtered_df_ISB.empty:
                        combined_df_CHargeOut_ISB = pd.concat([combined_df_CHargeOut_ISB, filtered_df_ISB],
                                                              ignore_index=True)
                    df = data_loader.get_dataframe("CIC_ChargeOut_SAP")
                    filtered_df_SAP = df[df['Project Name'].apply(lambda x: any(pid in x for pid in project_id)) & (
                            df['Billing Status'].str.lower() == 'successful')]
                    if not filtered_df_SAP.empty:
                        combined_df_CHargeOut_SAP = pd.concat([combined_df_CHargeOut_SAP, filtered_df_SAP],
                                                              ignore_index=True)

                    if filtered_df_ISB.empty and filtered_df_SAP.empty:
                        # print("当前Project ID在ChargeOut_Report里查找不到数据")
                        pass
                    else:
                        print("Chargeout_Report: 以当前Project ID查出的数据已添加到临时DataFrame中")

                    # account_id 有值的分支
                    if account_id:  # account_id 有值的分支

                        # 如果用户仅输入AccountID，查找BMS_Report, 字段列名-“AccountId”
                        #
                        #
                        df = data_loader.get_dataframe("CIC_BMS")
                        filtered_df_BMS = df[
                            df['AccountId'].isin(account_id) & (df['Billing Status'].str.lower() == 'successful')]
                        if filtered_df_BMS.empty:
                            # print("当前Account ID在BMS_Report里查找不到数据")
                            pass
                        else:
                            combined_df_BMS = pd.concat([combined_df_BMS, filtered_df_BMS], ignore_index=True)
                            print("BMS_Report: 以当前Account ID查出的数据已添加到临时DataFrame中")

                        # 如果用户仅输入AccountID，查找CATS_Report, 字段列名-“Account ID”
                        #
                        #
                        df = data_loader.get_dataframe("CIC_CATS_Labor")
                        filtered_df_Labor = df[df['Account ID'].isin(account_id)]
                        if not filtered_df_Labor.empty:
                            combined_df_CATS_Labor = pd.concat([combined_df_CATS_Labor, filtered_df_Labor],
                                                               ignore_index=True)
                        df = data_loader.get_dataframe("CIC_CATS_WWER")
                        filtered_df_WWER = df[df['Account ID'].isin(account_id)]
                        if not filtered_df_WWER.empty:
                            combined_df_CATS_WWER = pd.concat([combined_df_CATS_WWER, filtered_df_WWER],
                                                              ignore_index=True)
                        if filtered_df_Labor.empty and filtered_df_WWER.empty:
                            # print("当前Account ID在CATS_Report里查找不到数据")
                            pass
                        else:
                            print("CATS_Report: 以当前Account ID查出的数据已添加到临时DataFrame中")

                        # 如果用户仅输入AccountID，查找Chargeout_Report 字段列名-“PIC ID/Claim ID”
                        #
                        #
                        df = data_loader.get_dataframe("CIC_ChargeOut_ISB")
                        filtered_df_ISB = df[df['PIC ID/Claim ID \n( Client Account ID)'].isin(account_id) & (
                                df['Billing Status'].str.lower() == 'successful')]
                        if not filtered_df_ISB.empty:
                            combined_df_CHargeOut_ISB = pd.concat([combined_df_CHargeOut_ISB, filtered_df_ISB],
                                                                  ignore_index=True)
                        df = data_loader.get_dataframe("CIC_ChargeOut_SAP")
                        filtered_df_SAP = df[
                            df['PIC ID/Claim ID'].isin(account_id) & (df['Billing Status'].str.lower() == 'successful')]
                        if not filtered_df_SAP.empty:
                            combined_df_CHargeOut_SAP = pd.concat([combined_df_CHargeOut_SAP, filtered_df_SAP],
                                                                  ignore_index=True)
                        if filtered_df_ISB.empty and filtered_df_SAP.empty:
                            # print("当前Account ID在ChargeOut_Report里查找不到数据")
                            pass
                        else:
                            print("Chargeout_Report: 以当前Account ID查出的数据已添加到临时DataFrame中")

                    # wbs_id 有值的分支
                    if wbs_id:  # wbs_id 有值的分支

                        # 如果用户仅输入wbs_id，查找BMS_Report 字段列名-“Work Item Id”
                        #
                        #
                        df = data_loader.get_dataframe("CIC_BMS")
                        filtered_df_BMS = df[
                            df['Work Item Id'].isin(wbs_id) & (df['Billing Status'].str.lower() == 'successful')]

                        if filtered_df_BMS.empty:
                            # print("当前WBS_ID在BMS_Report里查找不到数据")
                            pass
                        else:
                            combined_df_BMS = pd.concat([combined_df_BMS, filtered_df_BMS], ignore_index=True)
                            print("BMS_Report: 以当前WBS_ID查出的数据已添加到临时DataFrame中")

                        # 如果用户仅输入wbs_id，查找CATS_Report 字段列名-“WBS Elem.”
                        #
                        #
                        df = data_loader.get_dataframe("CIC_CATS_Labor")
                        filtered_df_Labor = df[df['WBS Elem.'].isin(wbs_id)]
                        if not filtered_df_Labor.empty:
                            combined_df_CATS_Labor = pd.concat([combined_df_CATS_Labor, filtered_df_Labor],
                                                               ignore_index=True)

                        df = data_loader.get_dataframe("CIC_CATS_WWER")
                        filtered_df_WWER = df[df['WBS Elem.'].isin(wbs_id)]
                        if not filtered_df_WWER.empty:
                            combined_df_CATS_WWER = pd.concat([combined_df_CATS_WWER, filtered_df_WWER],
                                                              ignore_index=True)

                        if filtered_df_Labor.empty and filtered_df_WWER.empty:
                            # print("当前WBS_ID在CATS_Report里查找不到数据")
                            pass
                        else:
                            print("CATS_Report: 以当前WBS_ID查出的数据已添加到临时DataFrame中")

                        # 如果用户仅输入wbs_id，查找Chargeout_Report 字段列名-“PIC ID/Claim ID”
                        #
                        #
                        df = data_loader.get_dataframe("CIC_ChargeOut_ISB")
                        filtered_df_ISB = df[df['PIC ID/Claim ID \n( Client Account ID)'].isin(wbs_id) & (
                                df['Billing Status'].str.lower() == 'successful')]
                        if not filtered_df_ISB.empty:
                            combined_df_CHargeOut_ISB = pd.concat([combined_df_CHargeOut_ISB, filtered_df_ISB],
                                                                  ignore_index=True)

                        df = data_loader.get_dataframe("CIC_ChargeOut_SAP")
                        filtered_df_SAP = df[
                            df['PIC ID/Claim ID'].isin(wbs_id) & (df['Billing Status'].str.lower() == 'successful')]
                        if not filtered_df_SAP.empty:
                            combined_df_CHargeOut_SAP = pd.concat([combined_df_CHargeOut_SAP, filtered_df_SAP],
                                                                  ignore_index=True)

                        if filtered_df_ISB.empty and filtered_df_SAP.empty:
                            # print("当前WBS_ID在ChargeOut_Report里查找不到数据")
                            pass
                        else:
                            print("Chargeout_Report: 以当前WBS_ID查出的数据已添加到临时DataFrame中")

                    if ica_id != 'NA':  # ICA ID 有值的分支
                        # ICA ID 有值的分支
                        if isinstance(ica_id, str):
                            ica_id = [ica_id]
                        # print("Project ID 有值的分支_ica_id:", ica_id)

                        # 过滤掉在master_ica中的值
                        filtered_ica_id = [item for item in ica_id if item not in master_ica]
                        # print("filtered_ica_id:", filtered_ica_id)

                        # 如果用户仅输入ICA，查找Chargeout_Report 字段列名-“ICA Number”
                        #
                        #
                        df = data_loader.get_dataframe("CIC_ChargeOut_ISB")
                        filtered_df_ISB = df[df['Performing country ICA Number'].isin(filtered_ica_id) & (
                                df['Billing Status'].str.lower() == 'successful')]
                        if not filtered_df_ISB.empty:
                            combined_df_CHargeOut_ISB = pd.concat([combined_df_CHargeOut_ISB, filtered_df_ISB],
                                                                  ignore_index=True)
                        df = data_loader.get_dataframe("CIC_ChargeOut_SAP")
                        filtered_df_SAP = df[
                            df['ICA Number'].isin(filtered_ica_id) & (df['Billing Status'].str.lower() == 'successful')]
                        if not filtered_df_SAP.empty:
                            combined_df_CHargeOut_SAP = pd.concat([combined_df_CHargeOut_SAP, filtered_df_SAP],
                                                                  ignore_index=True)
                        if filtered_df_ISB.empty and filtered_df_SAP.empty:
                            # print("当前ICA_ID在ChargeOut_Report里查找不到数据")
                            pass
                        else:
                            print("Chargeout_Report: 以当前ICA_ID查出的数据已添加到临时DataFrame中")

                    # 输入的时间段有值的情况下才做写表的操作， 为空则不做。
                    if time_range:  # 输入的时间段有值的情况下才做写表的操作， 为空则不做。
                        # 将可能找出的BMS_Report保存到同一个工作表
                        if not combined_df_BMS.empty:
                            file_path_BMS = 'report/Bms_Report.xlsx'
                            # 通过用户输入的时间段来过滤 DataFrame
                            months_to_filter = get_month_range(time_range)
                            if months_to_filter is not None:
                                combined_df_BMS = combined_df_BMS[
                                    combined_df_BMS['Ledger Month Num'].isin(months_to_filter)]
                                # 去重 DataFrame 并写表
                                combined_df_BMS.drop_duplicates(inplace=True)
                                combined_df_BMS.to_excel(file_path_BMS, sheet_name='BMS', index=False)
                                print("以当前传入的 Project ID 或 ICA_ID 查出的数据生成的BMS_Report已保存在本地")

                        # 将可能找出的CATS_Report保存到同一个工作表
                        if not combined_df_CATS_Labor.empty or not combined_df_CATS_WWER.empty:
                            file_path_CATS = 'report/CATS_Report.xlsx'
                            # 通过用户输入的时间段来过滤 DataFrame
                            months_to_filter = get_month_range(time_range)
                            if months_to_filter is not None:
                                with pd.ExcelWriter(file_path_CATS, engine='openpyxl') as writer:
                                    if not combined_df_CATS_Labor.empty:
                                        combined_df_CATS_Labor = combined_df_CATS_Labor[
                                            combined_df_CATS_Labor['Ledger Month'].isin(months_to_filter)]
                                        # 去重 DataFrame 并写表
                                        combined_df_CATS_Labor.drop_duplicates(inplace=True)
                                        combined_df_CATS_Labor.to_excel(writer, sheet_name='Labor', index=False)
                                    if not combined_df_CATS_WWER.empty:
                                        combined_df_CATS_WWER = combined_df_CATS_WWER[
                                            combined_df_CATS_WWER['Ledger Month Num'].isin(months_to_filter)]
                                        # 去重 DataFrame 并写表
                                        combined_df_CATS_WWER.drop_duplicates(inplace=True)
                                        combined_df_CATS_WWER.to_excel(writer, sheet_name='WWER', index=False)
                                print("以当前传入的 Project ID 或 ICA_ID 查出的数据生成的CATS_Report已保存在本地")

                        # 将可能找出的ChargeOut_Report保存到同一个工作表
                        if not combined_df_CHargeOut_ISB.empty or not combined_df_CHargeOut_SAP.empty:
                            file_path_ChOut = 'report/ChargeOut_Report.xlsx'
                            # 通过用户输入的时间段来过滤 DataFrame
                            months_to_filter = get_month_range_for_ChargeOut(time_range)
                            if months_to_filter is not None:
                                with pd.ExcelWriter(file_path_ChOut, engine='openpyxl') as writer:
                                    if not combined_df_CHargeOut_ISB.empty:
                                        # 筛选 'Request ID' 列包含 months_to_filter 中任何一个值的行
                                        pattern = '|'.join(months_to_filter)
                                        combined_df_CHargeOut_ISB = combined_df_CHargeOut_ISB[
                                            combined_df_CHargeOut_ISB['Request  ID'].str.contains(pattern)]
                                        # 去重 DataFrame 并写表
                                        combined_df_CHargeOut_ISB.drop_duplicates(inplace=True)
                                        combined_df_CHargeOut_ISB.to_excel(writer, sheet_name='ISB billing',
                                                                           index=False)
                                    if not combined_df_CHargeOut_SAP.empty:
                                        # 筛选 'Request ID' 列包含 months_to_filter 中任何一个值的行
                                        pattern = '|'.join(months_to_filter)
                                        combined_df_CHargeOut_SAP = combined_df_CHargeOut_SAP[
                                            combined_df_CHargeOut_SAP['Request  ID'].str.contains(pattern)]
                                        # 去重 DataFrame 并写表
                                        combined_df_CHargeOut_SAP.drop_duplicates(inplace=True)
                                        combined_df_CHargeOut_SAP.to_excel(writer, sheet_name='SAP billing',
                                                                           index=False)
                                print(
                                    "以当前传入的 Project ID 或 ICA_ID 查出的数据生成的ChargeOut_Report已保存在本地")  # 输入的时间段有值的情况下才做写表的操作， 为空则不做。
                    else:
                        print("输入的时间字段为空， 无法吧搜索出来的报表数据写入Excel")

                if (combined_df_BMS.empty and
                        (combined_df_CATS_Labor.empty and combined_df_CATS_WWER.empty) and
                        (combined_df_CHargeOut_ISB.empty and combined_df_CHargeOut_SAP.empty)):
                    result_list.append("NG")
                else:
                    is_master_ica = check_ica_ids(ica_id, master_ica)
                    if is_master_ica:
                        # 前端传入的icaID中有MasterICA
                        result_list.append("OKM")
                    else:
                        result_list.append("OK")
                    if not combined_df_BMS.empty:
                        file_path_Attachement.append(file_path_BMS)
                    if not combined_df_CATS_Labor.empty or not combined_df_CATS_WWER.empty:
                        file_path_Attachement.append(file_path_CATS)
                    if not combined_df_CHargeOut_ISB.empty or not combined_df_CHargeOut_SAP.empty:
                        file_path_Attachement.append(file_path_ChOut)
                    print("file_path_Attachement_PM查询: ", file_path_Attachement)

                print("focal_id汇总", focal_id)
                # focal_id = ['whzb@cn.ibm.com', 'whzb@cn.ibm.com']
                # sendMail_Report("tingliu@cn.ibm.com", "Billing Report", focal_id,
                #                 ['tingliu@cn.ibm.com', 'hongqian@cn.ibm.com', 'sungang@cn.ibm.com', 'whzb@cn.ibm.com'],
                #                 file_path_Attachement)
                tempwd = ""
                sendMailtest("hongqian@cn.ibm.com", "Billing Report", focal_id,
                             ['tingliu@cn.ibm.com', 'hongqian@cn.ibm.com', 'sungang@cn.ibm.com', 'whzb@cn.ibm.com'],
                             file_path_Attachement, tempwd)

                result_list.append(focal_id)
            else:
                result_list.append("NG")


    # Billing Type 查询
    elif choice == 'Q_T':  # Billing Type 查询
        focal_id = get_jsonMessage_by_index(data, 1)
        billing_type = get_jsonMessage_by_index(data, 2)
        print("billing_type:", billing_type)
        time_range_from_billing_type_search = get_jsonMessage_by_index(data, 3)
        print("time_range_from_billing_type_search:", time_range_from_billing_type_search)

        if not time_range_from_billing_type_search:  # 输入的时间字段为空值，不做任何查询直接返回NG
            result_list.append("NG")
            print("Billing Type Search: 输入的时间字段为空值，不做任何查询操作直接返回NG")
        else:
            months_filter_for_BMS_CATS = get_month_range(time_range_from_billing_type_search)
            months_filter_for_ChargeOut = get_month_range_for_ChargeOut(time_range_from_billing_type_search)
            months_filter_for_Need_Confirmation = get_month_range_for_Need_Confirmation(
                time_range_from_billing_type_search)
            # print("months_filter_for_BMS_CATS:", months_filter_for_BMS_CATS)
            # print("months_filter_for_ChargeOut:", months_filter_for_ChargeOut)
            # print("months_filter_for_Need_Confirmation:", months_filter_for_Need_Confirmation)

            # 输入的时间字段有值但值不规范则直接返回NG 否则进行report条件搜索
            if months_filter_for_BMS_CATS is not None or months_filter_for_ChargeOut is not None or months_filter_for_Need_Confirmation is not None:

                if billing_type == 'Auto_Labor':

                    file_path_Auto_Labor, result_list = write_AutoLabor_data_to_excel(
                        time_range_from_billing_type_search,
                        months_filter_for_BMS_CATS)
                    file_path_Attachement.extend(file_path_Auto_Labor)

                    if 'OK' in result_list:
                        focal_id = ['whzb@cn.ibm.com', 'whzb@cn.ibm.com']
                        sendMail_Report("tingliu@cn.ibm.com", "Billing Type Auto Labor Report", focal_id,
                                        ['tingliu@cn.ibm.com', 'hongqian@cn.ibm.com', 'sungang@cn.ibm.com',
                                         'whzb@cn.ibm.com'],
                                        file_path_Attachement)

                elif billing_type == 'Auto_WWER':

                    file_path_Auto_WWER, result_list = write_AutoWWER_data_to_excel(
                        time_range_from_billing_type_search,
                        months_filter_for_BMS_CATS)
                    file_path_Attachement.extend(file_path_Auto_WWER)

                    if 'OK' in result_list:
                        focal_id = ['whzb@cn.ibm.com', 'whzb@cn.ibm.com']
                        sendMail_Report("tingliu@cn.ibm.com", "Billing Type Auto WWER Report", focal_id,
                                        ['tingliu@cn.ibm.com', 'hongqian@cn.ibm.com', 'sungang@cn.ibm.com',
                                         'whzb@cn.ibm.com'],
                                        file_path_Attachement)

                elif billing_type == 'Manual':

                    file_path_Manual, result_list = write_Manual_data_to_excel(time_range_from_billing_type_search,
                                                                               months_filter_for_BMS_CATS,
                                                                               months_filter_for_ChargeOut)
                    file_path_Attachement.extend(file_path_Manual)

                    if 'OK' in result_list:
                        focal_id = ['whzb@cn.ibm.com', 'whzb@cn.ibm.com']
                        sendMail_Report("tingliu@cn.ibm.com", "Billing Type Manual Report", focal_id,
                                        ['tingliu@cn.ibm.com', 'hongqian@cn.ibm.com', 'sungang@cn.ibm.com',
                                         'whzb@cn.ibm.com'],
                                        file_path_Attachement)

                elif billing_type == 'Credit':

                    file_path_Credit, result_list = write_Need_Confirmation_data_to_excel(
                        time_range_from_billing_type_search, months_filter_for_Need_Confirmation)
                    file_path_Attachement.extend(file_path_Credit)

                    if 'OK' in result_list:
                        focal_id = ['whzb@cn.ibm.com', 'whzb@cn.ibm.com']
                        sendMail_Report("tingliu@cn.ibm.com", "Billing Type Credit Report", focal_id,
                                        ['tingliu@cn.ibm.com', 'hongqian@cn.ibm.com', 'sungang@cn.ibm.com',
                                         'whzb@cn.ibm.com'],
                                        file_path_Attachement)

            else:
                result_list.append("NG")
                print("当前输入的查询时间条件不符合数据格式规范，故不做任何查询操作")

    # Billing Result 查询
    elif choice == 'Q_S':  # Billing Result 查询
        # {"message": ["Query by billing result", "whzb@cn.ibm.com     ", "", ["CTR018419556", "     CTR018958928", "2403641BISX0005P3I", "CN-11-02020    "], ["SHF224030001GCG", "   CTR018144628CIC"]]}

        focal_id = get_jsonMessage_by_index(data, 1)
        invoice = get_jsonMessage_by_index(data, 2)
        print("invoice:", invoice)
        tea = get_jsonMessage_by_index(data, 3)
        print("tea:", tea)
        cost_code = get_jsonMessage_by_index(data, 4)
        print("cost_code:", cost_code)

        # 三个查询条件都没有输入值的时候
        if not invoice and not tea and not cost_code:  # 三个查询条件都没有输入值的时候
            result_list.append("NG")
            print("Billing Result Search: 当前没有输入任何查询条件，不做任何查询操作直接返回NG")
        else:
            if invoice:

                file_path_invoice, result_list = write_Invoice_Number_data_to_excel(invoice)
                file_path_Attachement.extend(file_path_invoice)
                print("file_path_Attachement:", file_path_Attachement)

                if 'OK' in result_list:
                    focal_id = ['whzb@cn.ibm.com', 'whzb@cn.ibm.com']
                    sendMail_Report("tingliu@cn.ibm.com", "Billing Type invoice Report", focal_id,
                                    ['tingliu@cn.ibm.com', 'hongqian@cn.ibm.com', 'sungang@cn.ibm.com',
                                     'whzb@cn.ibm.com'],
                                    file_path_Attachement)

            elif tea:

                file_path_tea, result_list = write_TEA_data_to_excel(tea)
                file_path_Attachement.extend(file_path_tea)
                print("file_path_Attachement:", file_path_Attachement)

                if 'OK' in result_list:
                    focal_id = ['whzb@cn.ibm.com', 'whzb@cn.ibm.com']
                    sendMail_Report("tingliu@cn.ibm.com", "Billing Type TEA Report", focal_id,
                                    ['tingliu@cn.ibm.com', 'hongqian@cn.ibm.com', 'sungang@cn.ibm.com',
                                     'whzb@cn.ibm.com'],
                                    file_path_Attachement)

            elif cost_code:

                file_path_cost_code, result_list = write_Cost_Code_data_to_excel(cost_code)
                file_path_Attachement.extend(file_path_cost_code)
                print("file_path_Attachement:", file_path_Attachement)

                if 'OK' in result_list:
                    focal_id = ['whzb@cn.ibm.com', 'whzb@cn.ibm.com']
                    sendMail_Report("tingliu@cn.ibm.com", "Billing Type Cost Code Report", focal_id,
                                    ['tingliu@cn.ibm.com', 'hongqian@cn.ibm.com', 'sungang@cn.ibm.com',
                                     'whzb@cn.ibm.com'],
                                    file_path_Attachement)








    elif choice == '':
        pass
    elif choice == '':
        pass
    else:
        pass

    print("result_list:", result_list)

    return jsonify({"message": result_list})


# 联调End

# Example usage:
if __name__ == "__main__":
    # 前台联调
    app.run(host='0.0.0.0', port=8080, threaded=False)
