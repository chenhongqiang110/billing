import pandas as pd
import random
import string
from dataLoader import DataLoader


# 全局字典用于存储验证码
verification_codes = {}
File_Path = "data/user_data.xlsx"
data_loader = DataLoader()


class test:

    def userFlagVerification(self, username, password):

        df = data_loader.get_dataframe("user_data")
        user_info_dict = {}

        columns = ["Username", "Password", "Label"]
        df = df.reindex(columns=columns)

        for index, row in df.iterrows():
            user_info_dict.update({row["Username"]: (row["Password"], row["Label"])})
        # print(user_info_dict)
        if "@cn.ibm.com" in username:
            if username in user_info_dict.keys():
                pwd = user_info_dict.get(username)[0]
                # pwd = int(pwd)
                label = user_info_dict.get(username)[1]
                # print("str(pwd):", str(pwd))
                if str(pwd) == password:
                    if label == "Y":
                        print("user is VIP!!!!!")
                        role_status = "V"
                    else:
                        print("User is normal user!!!!!!")
                        role_status = "Y"
                else:
                    print("Invalid Password!!!!")
                    role_status = "P"
            else:
                print("Username Not Existed!!!!!")
                role_status = "U"
        else:
            print("Invalid Format!!!!!!!!!!!!!!!!")
            role_status = "F"
        return role_status

    def check_username_in_excel(self, username):

        # 读取Excel文件
        # df = read_excel(File_Path, sheetName="Sheet1")
        df = data_loader.get_dataframe("user_data")

        # 检查是否存在匹配的用户名
        if username in df["Username"].values:
            return "Y"
        else:
            return "N"

    def add_user_to_excel(self, username, password, file_path):

        try:
            # 尝试读取现有的Excel文件
            try:
                df = data_loader.get_dataframe("user_data")
            except FileNotFoundError:
                return None

            # 创建一个新的DataFrame行
            new_row = pd.DataFrame(
                {"Username": [username], "Password": [password], "Label": [None]}
            )

            # 将新行添加到DataFrame中
            df = pd.concat([df, new_row], ignore_index=True)

            # 将更新后的DataFrame写回到Excel文件
            df.to_excel(file_path, index=False, engine="openpyxl")

            print("用户信息已成功添加并保存到Excel文件中。")
        except Exception as e:
            print(f"Unexpected error: {e}")

    def add_user_to_excel_modify_pwd(self, username, password, file_path):

        try:
            # 尝试读取现有的Excel文件
            try:
                # df = read_excel(file_path, sheetName="Sheet1")
                df = data_loader.get_dataframe("user_data")
            except FileNotFoundError:
                return None

            df.loc[df["Username"] == username, "Password"] = password

            # 将更新后的DataFrame写回到Excel文件
            df.to_excel(file_path, index=False, engine="openpyxl")

            print("用户的新密码已经更新成功。")
        except Exception as e:
            print(f"Unexpected error: {e}")

    def generate_and_store_verification_code(self, user_id):
        """
        生成一个6位的验证码，包含字母和数字，并将其存储在全局字典中
        :param user_id: 用户ID，用于区分不同用户的验证码
        :return: 6位验证码字符串
        """
        # 定义验证码字符集，包含数字和字母
        characters = string.ascii_letters + string.digits

        # 随机选择6个字符
        verification_code = "".join(random.choices(characters, k=6))

        # 将验证码存储在全局字典中
        verification_codes[user_id] = verification_code
        return verification_code


# Example usage:
if __name__ == "__main__":
    json_message_str2 = (
        '{"message": ["Validation", "whzb@cn.ibm.com", "654321", "v2gPCt","111111"]}'
    )
    #
    # result = get_jsonMessage_by_index(json_message_str2, 0)
    # result2 = get_jsonMessage_by_index(json_message_str2, 1)
    #
    # result4 = getUserIDFromRegistration(json_message_str2)
    # result5 = check_username_in_excel(result4)

    # add_user_to_excel("whzb@cn.ibm.com","654321",file_path)
    # print("result:", add_user_to_excel)

    # print("result:", result)
    # print("result2:", result2)
    # print("result4:", result4)
    # print("result5:", result5)
