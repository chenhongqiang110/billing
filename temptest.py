from utils.readFile import read_csv
from IPython.display import Image, display
import matplotlib.pyplot as plt
from openpyxl import Workbook
from openpyxl.drawing.image import Image as xlImage


def get_Person(file_path, match_value):
    # 读取CSV文件
    df = read_csv(file_path)
    isPerson = []

    # 定义需要匹配的列
    columns_to_check = ['Project ID', 'Project Manager IntranetId', 'PMO IntranetId', 'PAL IntranetId']

    # 确保这些列存在于数据框中
    if not all(col in df.columns for col in columns_to_check):
        print("Some columns are missing in the CSV file.")
        return []

    # 筛选出需要检查的列
    df_temp = df[columns_to_check]

    # 检查是否存在匹配值
    if df_temp.isin([match_value]).any().any():
        # 获取包含匹配值的列列表
        isPerson = "Y"
    else:
        print("No results found")
        isPerson = "N"

    return isPerson






# Data for the pie chart
categories = ["Category 1", "Category 2", "Category 3"]
values = [10, 20, 30]

# Create the pie chart
fig, ax = plt.subplots()
ax.pie(values, labels=categories, autopct='%1.1f%%', startangle=90)
ax.axis('equal')  # Equal aspect ratio ensures the pie is drawn as a circle.

# Set title
plt.title("Pie Chart of Categories")

# Save the chart as an image
image_path = "/mnt/data/pie_chart.png"
fig.savefig(image_path)
plt.close(fig)

# Create an Excel workbook and sheet
wb = Workbook()
ws = wb.active
ws.title = "Pie Chart"

# Add the image to the Excel sheet
img = xlImage(image_path)
ws.add_image(img, 'A1')

# Save the workbook
excel_path = "/mnt/data/pie_chart.xlsx"
wb.save(excel_path)

# Display the image
display(Image(filename=image_path))

# Provide the path to download the Excel file
# excel_path


#
# def userFlagVerification1(text, filepath):
#     df_Normal = read_excel(filepath, sheetName="Normal")
#     df_VIP = read_excel(filepath, sheetName="VIP")
#     username = text.split("%")[0]
#     password = text.split("%")[1]
#
#     email_verication_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9.-]+$'
#
#     columns = ['username', 'password']
#     df_VIP = df_VIP.reindex(columns=columns)
#     df_Normal = df_Normal.reindex(columns=columns)
#
#     if re.match(email_verication_regex, username) and re.fullmatch(r'\d+', password):
#         if df_VIP.eq(username).any().any() == True or df_Normal.eq(username).any().any() == True:
#             if df_VIP.eq(username).any().any() == True:
#                 pwd = df_VIP[df_VIP['username'] == username].iloc[0, 1]
#                 if str(pwd) == password:
#                     print("user is in the VIP!!!!!")
#                     status = "V"
#                 else:
#                     print("Invalid Password!!!!")
#                     status = "P"
#             else:
#                 pwd = df_Normal[df_Normal['username'] == username].iloc[0, 1]
#                 if str(pwd) == password:
#                     print("User is in the normal!!!!!!")
#                     status = "Y"
#                 else:
#                     print("Invalid Password!!!!")
#                     status = "P"
#         else:
#             print("Username Not Existed!!!!!")
#             status = "N"
#     else:
#         print("Invalid Format!!!!!!!!!!!!!!!!")
#         status = "F"
#
#     return status


# Example usage:
if __name__ == "__main__":

    file_path = 'data/Project_MasterDtlReportD.csv'  # 替换为实际CSV文件路径
    match_value = 'furaodl@cn.ibm.com'  # 替换为前端传入的匹配值
    # match_value = 'zgxnwei@cn.ibm.com'  # 替换为前端传入的匹配值

