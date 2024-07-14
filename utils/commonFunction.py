import json
import re
from calendar import month_abbr


def get_label_from_command(target):
    """
    在固定列表中查找目标字符串，并返回对应的值
    :param target: 目标字符串
    :return: 对应的值，如果找到匹配项，否则返回 "N"
    target_string = "Login"
    result = get_label_from_command(target_string)
    print(result)  # 输出: "L"
    """
    none_value = "N"
    # 固定列表，包含元组（键，值）
    data_list = [("UnregisterCheck", "U"), ("Login", "L"), ("Query Excel", "Q_M"), ("Register", "R"),
                 ("Validation", "V"), ("ModifyPWD", "M"), ("Query by billing type", "Q_T"),
                 ("Query by billing result", "Q_S")]

    for key, value in data_list:
        if key == target:
            return value
    return none_value


def get_jsonMessage_by_index(json_message, index):
    try:
        # 如果输入是字典，直接使用，否则尝试解析为字典
        if isinstance(json_message, dict):
            data = json_message
        else:
            data = json.loads(json_message)

        # 确保 'message' 字段存在并且是一个列表
        if 'message' in data and isinstance(data['message'], list):
            messages = data['message']

            # 获取指定索引处的值
            value = messages[index] if 0 <= index < len(messages) else None

            # 检查值是否为列表，并去掉每个元素两端的空格
            if isinstance(value, list):
                cleaned_list = [item.strip() for item in value]
                return cleaned_list
            elif isinstance(value, str):
                return value.strip()
            else:
                return value
        else:
            print("Error: 'message' field is missing or is not a list")
            return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON message: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


def validate_string(input_code):
    # 正则表达式用于匹配6位长度，且只包含字母和数字的字符串
    pattern = r'^[A-Za-z0-9]{6}$'
    # 使用re.match进行匹配
    if re.match(pattern, input_code):
        return True
    else:
        return False


def get_month_range(time_range: str) -> set:
    # 去掉时间范围字符串两端的空格
    time_range = time_range.strip()
    # 定义时间范围的正则表达式模式
    pattern = r'^\d{6}(-\d{6})?$'
    # 检查输入格式是否符合要求
    if not re.match(pattern, time_range):
        print("BMS_CATS时间输入格式错误，仅支持“YYYYMM”或“YYYYMM-YYYYMM”格式。")
    else:
        # 检查输入中是否包含连字符（-）
        if '-' in time_range:
            # 拆分时间范围字符串
            start, end = time_range.split('-')
            # 提取起始和结束月份
            start_month = int(start[-2:])
            end_month = int(end[-2:])
            # 生成月份范围集合
            month_range = {month for month in range(start_month, end_month + 1)}
        else:
            # 处理单个月份输入
            month = int(time_range[-2:])
            month_range = {month}

        return month_range


def get_month_range_for_ChargeOut(time_range: str) -> list:
    # 去掉时间范围字符串两端的空格
    time_range = time_range.strip()

    # 定义一个辅助函数来验证日期格式
    def is_valid_format(tr):
        # 验证单月格式，例如 "202401"
        if len(tr) == 6 and tr.isdigit():
            return True
        # 验证范围格式，例如 "202401-202405"
        if len(tr) == 13 and tr[6] == '-' and tr[:6].isdigit() and tr[7:].isdigit():
            return True
        return False

    # 检查输入格式是否有效
    if not is_valid_format(time_range):
        print("ChargeOut时间错误: 参数 time_range 的格式不正确。必须是 'YYYYMM' 或 'YYYYMM-YYYYMM' 格式。")
    else:
        # 检查输入中是否包含连字符（-）
        if '-' in time_range:
            # 拆分时间范围字符串为起始和结束时间
            start, end = time_range.split('-')
            # 提取起始和结束月份
            start_year = int(start[:4])
            start_month = int(start[-2:])
            end_year = int(end[:4])
            end_month = int(end[-2:])

            # 生成月份范围列表
            month_range = []
            current_year = start_year
            current_month = start_month

            while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
                # 格式化当前年份和月份
                month_range.append(f"{current_year:04d}{current_month:02d}")
                # 增加月份，处理年份变化
                if current_month == 12:
                    current_month = 1
                    current_year += 1
                else:
                    current_month += 1
        else:
            # 处理单个月份输入
            month_range = [time_range]

        return month_range


def get_month_range_for_Need_Confirmation(time_range: str) -> list:
    # 去掉时间范围字符串两端的空格
    time_range = time_range.strip()
    # 定义时间范围的正则表达式模式
    pattern = r'^\d{6}(-\d{6})?$'

    # 检查输入格式是否符合要求
    if not re.match(pattern, time_range):
        print("查询Need_Confirmation表的时间输入格式错误，仅支持“YYYYMM”或“YYYYMM-YYYYMM”格式。")
    else:
        # 检查输入中是否包含连字符（-）
        if '-' in time_range:
            # 拆分时间范围字符串
            start, end = time_range.split('-')
            # 提取起始年份和月份
            start_year = int(start[:4])
            start_month = int(start[-2:])
            end_year = int(end[:4])
            end_month = int(end[-2:])

            # 检查年份是否相同，若不同则返回错误信息
            if start_year != end_year:
                print("查询Need_Confirmation表时间输入格式错误，不支持跨年的月份范围。")
                return []

            # 生成月份范围列表
            month_range = [month_abbr[month] for month in range(start_month, end_month + 1)]
        else:
            # 处理单个月份输入
            month = int(time_range[-2:])
            month_range = [month_abbr[month]]

        return month_range


# Example usage:
if __name__ == "__main__":
    excel_file = '../data/user_data.xlsx'
    file_path1 = '../data/Project_MasterDtlReportD.csv'
    file_path2 = '../data/BmsDtlReportD.csv'

    json_message_str = '{"message": ["Query Excel", "whzb@cn.ibm.com", ["CN-09-00001", "CN-14-05103"], "OC3730", "C.25NZY.001", "NA", ""]}'
    json_message_str2 = '{"message": ["Register", "whzb@cn.ibm.com"]}'

    print("result:", get_month_range_for_Need_Confirmation("202423098 "))
    print("result2:", get_month_range_for_Need_Confirmation("202403-202405"))
    print("result3:", get_month_range_for_Need_Confirmation(" 202403-202410"))
    print("result4:", get_month_range_for_Need_Confirmation("202401"))

    # 测试示例
    # print("result:", get_month_range(" 202401-202405 "))  # 输出 {1, 2, 3, 4, 5}
    # print("result2:", get_month_range("202403-202405"))  # 输出 {3, 4, 5}
    # print("result3:", get_month_range(" 202409-202503"))  # 输出 {3, 4, 5, 6, 7, 8, 9, 10}
    # print("result5:", get_month_range("2024101"))  # 输出 {10}
    # #
    # print("lala:", get_month_range_for_ChargeOut('202401'))  # 输出: ['202401-202403']
    # print("lala2:", get_month_range_for_ChargeOut('202402-202406'))  # 输出: ['202402', '202403', '202404', '202405', '202406']
    #
