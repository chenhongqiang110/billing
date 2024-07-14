import time
import pandas as pd
import os
import yaml


class DataLoader:
    #
    #
    # 使用单例模式来确保DataLoader实例在整个应用生命周期中只有一个实例。
    # 注意看13-18行代码， 另外初始化方法def __init__(self)里面也有变化(注意看)
    #
    #
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(DataLoader, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, config_path=None):

        if "initialized" not in self.__dict__:
            print("Initializing DataLoader...")

            # 确定 DataLoader.py 文件所在的目录
            base_path = os.path.dirname(os.path.abspath(__file__))

            if config_path is None:
                config_path = os.path.join(base_path, 'config.yaml')

            with open(config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)

            # 基础信息 Report
            self.file_path_people_In_user_data = config['file_paths']['user_data']
            self.file_path_people_In_PM_PMO_PAL = config['file_paths']['PM_PMO_PAL']
            self.file_path_Unregister = config['file_paths']['Unregister']

            # CIC & GCG BMS Report
            self.read_file_path_CIC_BMS_202401 = config['file_paths']['CIC_BMS_202401']
            self.read_file_path_CIC_BMS_202402 = config['file_paths']['CIC_BMS_202402']
            self.read_file_path_CIC_BMS_202403 = config['file_paths']['CIC_BMS_202403']
            self.read_file_path_CIC_BMS_202404 = config['file_paths']['CIC_BMS_202404']
            self.read_file_path_CIC_BMS_202405 = config['file_paths']['CIC_BMS_202405']
            self.read_file_path_GCG_BMS_202401 = config['file_paths']['GCG_BMS_202401']
            self.read_file_path_GCG_BMS_202402 = config['file_paths']['GCG_BMS_202402']
            self.read_file_path_GCG_BMS_202403 = config['file_paths']['GCG_BMS_202403']
            self.read_file_path_GCG_BMS_202404 = config['file_paths']['GCG_BMS_202404']
            self.read_file_path_GCG_BMS_202405 = config['file_paths']['GCG_BMS_202405']

            # CIC & GCG CATS Report
            self.read_file_path_CIC_CATS_202401 = config['file_paths']['CIC_CATS_202401']
            self.read_file_path_CIC_CATS_202402 = config['file_paths']['CIC_CATS_202402']
            self.read_file_path_CIC_CATS_202403 = config['file_paths']['CIC_CATS_202403']
            self.read_file_path_CIC_CATS_202404 = config['file_paths']['CIC_CATS_202404']
            self.read_file_path_CIC_CATS_202405 = config['file_paths']['CIC_CATS_202405']
            self.read_file_path_GCG_CATS_202401 = config['file_paths']['GCG_CATS_202401']
            self.read_file_path_GCG_CATS_202402 = config['file_paths']['GCG_CATS_202402']
            self.read_file_path_GCG_CATS_202403 = config['file_paths']['GCG_CATS_202403']
            self.read_file_path_GCG_CATS_202404 = config['file_paths']['GCG_CATS_202404']
            self.read_file_path_GCG_CATS_202405 = config['file_paths']['GCG_CATS_202405']

            # CIC & GCG Charge Out Report
            self.read_file_path_CIC_ChargeOut_202401 = config['file_paths']['CIC_ChargeOut_202401']
            self.read_file_path_CIC_ChargeOut_202402 = config['file_paths']['CIC_ChargeOut_202402']
            self.read_file_path_CIC_ChargeOut_202403 = config['file_paths']['CIC_ChargeOut_202403']
            self.read_file_path_CIC_ChargeOut_202404 = config['file_paths']['CIC_ChargeOut_202404']
            self.read_file_path_CIC_ChargeOut_202405 = config['file_paths']['CIC_ChargeOut_202405']
            self.read_file_path_GCG_ChargeOut_202401 = config['file_paths']['GCG_ChargeOut_202401']
            self.read_file_path_GCG_ChargeOut_202402 = config['file_paths']['GCG_ChargeOut_202402']
            self.read_file_path_GCG_ChargeOut_202403 = config['file_paths']['GCG_ChargeOut_202403']
            self.read_file_path_GCG_ChargeOut_202404 = config['file_paths']['GCG_ChargeOut_202404']
            self.read_file_path_GCG_ChargeOut_202405 = config['file_paths']['GCG_ChargeOut_202405']

            # Billing Report-Need Confirmation Report
            self.read_file_path_Need_Confirmation_202401 = config['file_paths']['Need_Confirmation_202401']
            self.read_file_path_Need_Confirmation_202402 = config['file_paths']['Need_Confirmation_202402']
            self.read_file_path_Need_Confirmation_202403 = config['file_paths']['Need_Confirmation_202403']
            self.read_file_path_Need_Confirmation_202404 = config['file_paths']['Need_Confirmation_202404']
            self.read_file_path_Need_Confirmation_202405 = config['file_paths']['Need_Confirmation_202405']

            self.dataframes = {}
            self.load_all_tables()
            self.initialized = True

    def load_all_tables(self):
        self.load_data_table_In_user_data()
        self.load_data_table_In_PM_PMO_PAL()
        self.load_data_table_Unregister()

        self.load_data_table_CIC_BMS()
        self.load_data_table_CIC_CATS_Labor()
        self.load_data_table_CIC_CATS_WWER()
        self.load_data_table_CIC_ChargeOut_ISB()
        self.load_data_table_CIC_ChargeOut_SAP()

        self.load_data_table_GCG_BMS()
        self.load_data_table_GCG_CATS_Labor()
        self.load_data_table_GCG_CATS_WWER()
        self.load_data_table_GCG_ChargeOut_ISB()
        self.load_data_table_GCG_ChargeOut_SAP()

        self.load_data_table_Need_Confirmation()

        # self.get_Need_Confirmation_CIC_Billing_Tracking_202401()
        # self.get_Need_Confirmation_CIC_Billing_Tracking_202402()
        # self.get_Need_Confirmation_CIC_Billing_Tracking_202403()
        # self.get_Need_Confirmation_CIC_Billing_Tracking_202404()
        # self.get_Need_Confirmation_CIC_Billing_Tracking_202405()
        #
        # self.get_Need_Confirmation_GCG_Billing_Tracking_202401()
        # self.get_Need_Confirmation_GCG_Billing_Tracking_202402()
        # self.get_Need_Confirmation_GCG_Billing_Tracking_202403()
        # self.get_Need_Confirmation_GCG_Billing_Tracking_202404()
        # self.get_Need_Confirmation_GCG_Billing_Tracking_202405()



    def load_data_table_In_user_data(self):
        start_time = time.time()
        df = pd.read_excel(self.file_path_people_In_user_data, sheet_name="Sheet1")
        self.dataframes["user_data"] = df
        end_time = time.time()
        print(f"Loaded user_data in {end_time - start_time:.2f} seconds")

    def load_data_table_In_PM_PMO_PAL(self):
        start_time = time.time()
        df = pd.read_csv(
            self.file_path_people_In_PM_PMO_PAL, encoding="ISO-8859-1", low_memory=False
        )
        self.dataframes["PM_PMO_PAL"] = df
        end_time = time.time()
        print(f"Loaded PM_PMO_PAL in {end_time - start_time:.2f} seconds")

    def load_data_table_Unregister(self):
        start_time = time.time()
        df = pd.read_excel(self.file_path_Unregister, sheet_name="Sheet1")
        self.dataframes["Unregister"] = df
        end_time = time.time()
        print(f"Loaded Unregister in {end_time - start_time:.2f} seconds")

    def load_data_table_Need_Confirmation(self):
        start_time = time.time()
        df_202401 = pd.read_excel(
            self.read_file_path_Need_Confirmation_202401,
            sheet_name="Confirmation Detail",
            engine="openpyxl",
            dtype={"Billing Invoice Number": str},
        )
        df_202402 = pd.read_excel(
            self.read_file_path_Need_Confirmation_202402,
            sheet_name="Confirmation Detail",
            engine="openpyxl",
            dtype={"Billing Invoice Number": str},
        )
        df_202403 = pd.read_excel(
            self.read_file_path_Need_Confirmation_202403,
            sheet_name="Confirmation Detail",
            engine="openpyxl",
            dtype={"Billing Invoice Number": str},
        )
        df_202404 = pd.read_excel(
            self.read_file_path_Need_Confirmation_202404,
            sheet_name="Confirmation Detail",
            engine="openpyxl",
            dtype={"Billing Invoice Number": str},
        )
        df_202405 = pd.read_excel(
            self.read_file_path_Need_Confirmation_202405,
            sheet_name="Confirmation Detail",
            engine="openpyxl",
            dtype={"Billing Invoice Number": str},
        )

        # 将两个DataFrame合并成一个
        combined_df_Need_Confirmation = pd.concat(
            [df_202401, df_202402, df_202403, df_202404, df_202405], ignore_index=True
        )

        # 将合并后的DataFrame存储在'dataframes'字典中，键为'BMS'
        self.dataframes["Need_Confirmation"] = combined_df_Need_Confirmation
        end_time = time.time()
        print(f"Loaded Need_Confirmation in {end_time - start_time:.2f} seconds")


    def load_data_table_CIC_BMS(self):
        start_time = time.time()
        df_202401 = pd.read_excel(
            self.read_file_path_CIC_BMS_202401,
            sheet_name="Details",
            engine="openpyxl",
            skiprows=1,
            dtype={"SOURCE_KEY": str, "Cias Invoice Num": str},
        )
        df_202402 = pd.read_excel(
            self.read_file_path_CIC_BMS_202402,
            sheet_name="Details",
            engine="openpyxl",
            skiprows=1,
            dtype={"SOURCE_KEY": str, "Cias Invoice Num": str},
        )
        df_202403 = pd.read_excel(
            self.read_file_path_CIC_BMS_202403,
            sheet_name="Details",
            engine="openpyxl",
            skiprows=1,
            dtype={"SOURCE_KEY": str, "Cias Invoice Num": str},
        )
        df_202404 = pd.read_excel(
            self.read_file_path_CIC_BMS_202404,
            sheet_name="Details",
            engine="openpyxl",
            skiprows=1,
            dtype={"SOURCE_KEY": str, "Cias Invoice Num": str},
        )
        df_202405 = pd.read_excel(
            self.read_file_path_CIC_BMS_202405,
            sheet_name="Details",
            engine="openpyxl",
            skiprows=1,
            dtype={"SOURCE_KEY": str, "Cias Invoice Num": str},
        )

        # 将两个DataFrame合并成一个
        combined_df_CIC_BMS = pd.concat(
            [df_202401, df_202402, df_202403, df_202404, df_202405], ignore_index=True
        )

        # 将合并后的DataFrame存储在'dataframes'字典中，键为'BMS'
        self.dataframes["CIC_BMS"] = combined_df_CIC_BMS
        end_time = time.time()
        print(f"Loaded CIC_BMS in {end_time - start_time:.2f} seconds")

    def load_data_table_GCG_BMS(self):
        start_time = time.time()
        df_202401 = pd.read_excel(
            self.read_file_path_GCG_BMS_202401,
            sheet_name="Details",
            engine="openpyxl",
            skiprows=1,
            dtype={"SOURCE_KEY": str, "Cias Invoice Num": str},
        )
        df_202402 = pd.read_excel(
            self.read_file_path_GCG_BMS_202402,
            sheet_name="Details",
            engine="openpyxl",
            skiprows=1,
            dtype={"SOURCE_KEY": str, "Cias Invoice Num": str},
        )
        df_202403 = pd.read_excel(
            self.read_file_path_GCG_BMS_202403,
            sheet_name="Details",
            engine="openpyxl",
            skiprows=1,
            dtype={"SOURCE_KEY": str, "Cias Invoice Num": str},
        )
        df_202404 = pd.read_excel(
            self.read_file_path_GCG_BMS_202404,
            sheet_name="Details",
            engine="openpyxl",
            skiprows=1,
            dtype={"SOURCE_KEY": str, "Cias Invoice Num": str},
        )
        df_202405 = pd.read_excel(
            self.read_file_path_GCG_BMS_202405,
            sheet_name="Details",
            engine="openpyxl",
            skiprows=1,
            dtype={"SOURCE_KEY": str, "Cias Invoice Num": str},
        )

        # 将两个DataFrame合并成一个
        combined_df_GCG_BMS = pd.concat(
            [df_202401, df_202402, df_202403, df_202404, df_202405], ignore_index=True
        )

        # 将合并后的DataFrame存储在'dataframes'字典中，键为'BMS'
        self.dataframes["GCG_BMS"] = combined_df_GCG_BMS
        end_time = time.time()
        print(f"Loaded GCG_BMS in {end_time - start_time:.2f} seconds")

    def load_data_table_CIC_CATS_Labor(self):
        start_time = time.time()
        df_CATS_CIC_Labor_202401 = pd.read_excel(
            self.read_file_path_CIC_CATS_202401,
            sheet_name="Labor",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_CIC_Labor_202402 = pd.read_excel(
            self.read_file_path_CIC_CATS_202402,
            sheet_name="Labor",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_CIC_Labor_202403 = pd.read_excel(
            self.read_file_path_CIC_CATS_202403,
            sheet_name="Labor",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_CIC_Labor_202404 = pd.read_excel(
            self.read_file_path_CIC_CATS_202404,
            sheet_name="Labor",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_CIC_Labor_202405 = pd.read_excel(
            self.read_file_path_CIC_CATS_202405,
            sheet_name="Labor",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )

        # 将两个DataFrame合并成一个
        combined_df_CIC_Labor = pd.concat(
            [
                df_CATS_CIC_Labor_202401,
                df_CATS_CIC_Labor_202402,
                df_CATS_CIC_Labor_202403,
                df_CATS_CIC_Labor_202404,
                df_CATS_CIC_Labor_202405,
            ],
            ignore_index=True,
        )

        # 将合并后的DataFrame存储在'dataframes'字典中，键为'CATS_Labor'
        self.dataframes["CIC_CATS_Labor"] = combined_df_CIC_Labor
        end_time = time.time()
        print(f"Loaded CIC_CATS_Labor in {end_time - start_time:.2f} seconds")

    def load_data_table_GCG_CATS_Labor(self):
        start_time = time.time()
        df_CATS_GCG_Labor_202401 = pd.read_excel(
            self.read_file_path_GCG_CATS_202401,
            sheet_name="Labor",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_GCG_Labor_202402 = pd.read_excel(
            self.read_file_path_GCG_CATS_202402,
            sheet_name="Labor",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_GCG_Labor_202403 = pd.read_excel(
            self.read_file_path_GCG_CATS_202403,
            sheet_name="Labor",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_GCG_Labor_202404 = pd.read_excel(
            self.read_file_path_GCG_CATS_202404,
            sheet_name="Labor",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_GCG_Labor_202405 = pd.read_excel(
            self.read_file_path_GCG_CATS_202405,
            sheet_name="Labor",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )

        # 将两个DataFrame合并成一个
        combined_df_GCG_CATS_Labor = pd.concat(
            [
                df_CATS_GCG_Labor_202401,
                df_CATS_GCG_Labor_202402,
                df_CATS_GCG_Labor_202403,
                df_CATS_GCG_Labor_202404,
                df_CATS_GCG_Labor_202405,
            ],
            ignore_index=True,
        )

        # 将合并后的DataFrame存储在'dataframes'字典中，键为'CATS_Labor'
        self.dataframes["GCG_CATS_Labor"] = combined_df_GCG_CATS_Labor
        end_time = time.time()
        print(f"Loaded GCG_CATS_Labor in {end_time - start_time:.2f} seconds")

    def load_data_table_CIC_CATS_WWER(self):
        start_time = time.time()
        df_CATS_CIC_WWER_202401 = pd.read_excel(
            self.read_file_path_CIC_CATS_202401,
            sheet_name="WWER",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_CIC_WWER_202402 = pd.read_excel(
            self.read_file_path_CIC_CATS_202402,
            sheet_name="WWER",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_CIC_WWER_202403 = pd.read_excel(
            self.read_file_path_CIC_CATS_202403,
            sheet_name="WWER",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_CIC_WWER_202404 = pd.read_excel(
            self.read_file_path_CIC_CATS_202404,
            sheet_name="WWER",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_CIC_WWER_202405 = pd.read_excel(
            self.read_file_path_CIC_CATS_202405,
            sheet_name="WWER",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )

        # 将两个DataFrame合并成一个
        combined_df_CATS_CIC_WWER = pd.concat(
            [
                df_CATS_CIC_WWER_202401,
                df_CATS_CIC_WWER_202402,
                df_CATS_CIC_WWER_202403,
                df_CATS_CIC_WWER_202404,
                df_CATS_CIC_WWER_202405,
            ],
            ignore_index=True,
        )

        # 将合并后的DataFrame存储在'dataframes'字典中，键为'CATS_WWER'
        self.dataframes["CIC_CATS_WWER"] = combined_df_CATS_CIC_WWER
        end_time = time.time()
        print(f"Loaded CIC_CATS_WWER in {end_time - start_time:.2f} seconds")

    def load_data_table_GCG_CATS_WWER(self):
        start_time = time.time()
        df_CATS_GCG_WWER_202401 = pd.read_excel(
            self.read_file_path_GCG_CATS_202401,
            sheet_name="WWER",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_GCG_WWER_202402 = pd.read_excel(
            self.read_file_path_GCG_CATS_202402,
            sheet_name="WWER",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_GCG_WWER_202403 = pd.read_excel(
            self.read_file_path_GCG_CATS_202403,
            sheet_name="WWER",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_GCG_WWER_202404 = pd.read_excel(
            self.read_file_path_GCG_CATS_202404,
            sheet_name="WWER",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )
        df_CATS_GCG_WWER_202405 = pd.read_excel(
            self.read_file_path_GCG_CATS_202405,
            sheet_name="WWER",
            engine="openpyxl",
            skiprows=1,
            dtype={"Personnel Number": str, "Emp Num": str},
        )

        # 将两个DataFrame合并成一个
        combined_df_CATS_GCG_WWER = pd.concat(
            [
                df_CATS_GCG_WWER_202401,
                df_CATS_GCG_WWER_202402,
                df_CATS_GCG_WWER_202403,
                df_CATS_GCG_WWER_202404,
                df_CATS_GCG_WWER_202405,
            ],
            ignore_index=True,
        )

        # 将合并后的DataFrame存储在'dataframes'字典中，键为'CATS_WWER'
        self.dataframes["GCG_CATS_WWER"] = combined_df_CATS_GCG_WWER
        end_time = time.time()
        print(f"Loaded GCG_CATS_WWER in {end_time - start_time:.2f} seconds")

    def load_data_table_CIC_ChargeOut_ISB(self):
        start_time = time.time()
        df_ChargeOut_CIC_ISB_202401 = pd.read_excel(
            self.read_file_path_CIC_ChargeOut_202401,
            sheet_name="ISB billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str},
        )
        df_ChargeOut_CIC_ISB_202402 = pd.read_excel(
            self.read_file_path_CIC_ChargeOut_202402,
            sheet_name="ISB billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str},
        )
        df_ChargeOut_CIC_ISB_202403 = pd.read_excel(
            self.read_file_path_CIC_ChargeOut_202403,
            sheet_name="ISB billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str},
        )
        df_ChargeOut_CIC_ISB_202404 = pd.read_excel(
            self.read_file_path_CIC_ChargeOut_202404,
            sheet_name="ISB billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str},
        )
        df_ChargeOut_CIC_ISB_202405 = pd.read_excel(
            self.read_file_path_CIC_ChargeOut_202405,
            sheet_name="ISB billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str},
        )

        # 将两个DataFrame合并成一个
        combined_df_ChargeOut_CIC_ISB = pd.concat(
            [
                df_ChargeOut_CIC_ISB_202401,
                df_ChargeOut_CIC_ISB_202402,
                df_ChargeOut_CIC_ISB_202403,
                df_ChargeOut_CIC_ISB_202404,
                df_ChargeOut_CIC_ISB_202405,
            ],
            ignore_index=True,
        )

        # 将合并后的DataFrame存储在'dataframes'字典中，键为'ChargeOut_ISB'
        self.dataframes["CIC_ChargeOut_ISB"] = combined_df_ChargeOut_CIC_ISB
        end_time = time.time()
        print(f"Loaded CIC_ChargeOut_ISB in {end_time - start_time:.2f} seconds")

    def load_data_table_GCG_ChargeOut_ISB(self):
        start_time = time.time()
        df_ChargeOut_GCG_ISB_202401 = pd.read_excel(
            self.read_file_path_GCG_ChargeOut_202401,
            sheet_name="ISB billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str},
        )
        df_ChargeOut_GCG_ISB_202402 = pd.read_excel(
            self.read_file_path_GCG_ChargeOut_202402,
            sheet_name="ISB billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str},
        )
        df_ChargeOut_GCG_ISB_202403 = pd.read_excel(
            self.read_file_path_GCG_ChargeOut_202403,
            sheet_name="ISB billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str},
        )
        df_ChargeOut_GCG_ISB_202404 = pd.read_excel(
            self.read_file_path_GCG_ChargeOut_202404,
            sheet_name="ISB billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str},
        )
        df_ChargeOut_GCG_ISB_202405 = pd.read_excel(
            self.read_file_path_GCG_ChargeOut_202405,
            sheet_name="ISB billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str},
        )

        # 将两个DataFrame合并成一个
        combined_df_ChargeOut_GCG_ISB = pd.concat(
            [
                df_ChargeOut_GCG_ISB_202401,
                df_ChargeOut_GCG_ISB_202402,
                df_ChargeOut_GCG_ISB_202403,
                df_ChargeOut_GCG_ISB_202404,
                df_ChargeOut_GCG_ISB_202405,
            ],
            ignore_index=True,
        )

        # 将合并后的DataFrame存储在'dataframes'字典中，键为'ChargeOut_ISB'
        self.dataframes["GCG_ChargeOut_ISB"] = combined_df_ChargeOut_GCG_ISB
        end_time = time.time()
        print(f"Loaded GCG_ChargeOut_ISB in {end_time - start_time:.2f} seconds")

    def load_data_table_CIC_ChargeOut_SAP(self):
        start_time = time.time()
        df_ChargeOut_CIC_SAP_202401 = pd.read_excel(
            self.read_file_path_CIC_ChargeOut_202401,
            sheet_name="SAP billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str, "ICA Number": str},
        )
        df_ChargeOut_CIC_SAP_202402 = pd.read_excel(
            self.read_file_path_CIC_ChargeOut_202402,
            sheet_name="SAP billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str, "ICA Number": str},
        )
        df_ChargeOut_CIC_SAP_202403 = pd.read_excel(
            self.read_file_path_CIC_ChargeOut_202403,
            sheet_name="SAP billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str, "ICA Number": str},
        )
        df_ChargeOut_CIC_SAP_202404 = pd.read_excel(
            self.read_file_path_CIC_ChargeOut_202404,
            sheet_name="SAP billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str, "ICA Number": str},
        )
        df_ChargeOut_CIC_SAP_202405 = pd.read_excel(
            self.read_file_path_CIC_ChargeOut_202405,
            sheet_name="SAP billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str, "ICA Number": str},
        )

        # 将两个DataFrame合并成一个
        combined_df_ChargeOut_CIC_SAP = pd.concat(
            [
                df_ChargeOut_CIC_SAP_202401,
                df_ChargeOut_CIC_SAP_202402,
                df_ChargeOut_CIC_SAP_202403,
                df_ChargeOut_CIC_SAP_202404,
                df_ChargeOut_CIC_SAP_202405,
            ],
            ignore_index=True,
        )

        # 将合并后的DataFrame存储在'dataframes'字典中，键为'ChargeOut_SAP'
        self.dataframes["CIC_ChargeOut_SAP"] = combined_df_ChargeOut_CIC_SAP
        end_time = time.time()
        print(f"Loaded CIC_ChargeOut_SAP in {end_time - start_time:.2f} seconds")

    def load_data_table_GCG_ChargeOut_SAP(self):
        start_time = time.time()
        df_ChargeOut_GCG_SAP_202401 = pd.read_excel(
            self.read_file_path_GCG_ChargeOut_202401,
            sheet_name="SAP billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str, "ICA Number": str},
        )
        df_ChargeOut_GCG_SAP_202402 = pd.read_excel(
            self.read_file_path_GCG_ChargeOut_202402,
            sheet_name="SAP billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str, "ICA Number": str},
        )
        df_ChargeOut_GCG_SAP_202403 = pd.read_excel(
            self.read_file_path_GCG_ChargeOut_202403,
            sheet_name="SAP billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str, "ICA Number": str},
        )
        df_ChargeOut_GCG_SAP_202404 = pd.read_excel(
            self.read_file_path_GCG_ChargeOut_202404,
            sheet_name="SAP billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str, "ICA Number": str},
        )
        df_ChargeOut_GCG_SAP_202405 = pd.read_excel(
            self.read_file_path_GCG_ChargeOut_202405,
            sheet_name="SAP billing",
            engine="openpyxl",
            skiprows=3,
            dtype={"Invoice Number": str, "ICA Number": str},
        )

        # 将两个DataFrame合并成一个
        combined_df_ChargeOut_GCG_SAP = pd.concat(
            [
                df_ChargeOut_GCG_SAP_202401,
                df_ChargeOut_GCG_SAP_202402,
                df_ChargeOut_GCG_SAP_202403,
                df_ChargeOut_GCG_SAP_202404,
                df_ChargeOut_GCG_SAP_202405,
            ],
            ignore_index=True,
        )

        # 将合并后的DataFrame存储在'dataframes'字典中，键为'ChargeOut_SAP'
        self.dataframes["GCG_ChargeOut_SAP"] = combined_df_ChargeOut_GCG_SAP
        end_time = time.time()
        print(f"Loaded GCG_ChargeOut_SAP in {end_time - start_time:.2f} seconds")

    def get_dataframe(self, name):
        return self.dataframes.get(name, None)


    def get_Need_Confirmation_CIC_Billing_Tracking_202401(self):
        # 读取Excel文件
        df = pd.read_excel(self.read_file_path_Need_Confirmation_202401, sheet_name="CIC Billing Result Tracking")
        # 提取A5-A23和Q5-Q23的数据
        fees_names = df.iloc[3:22, 0].tolist()  # A列的索引为0，行索引范围为4到22（包含）
        fees_values = df.iloc[3:22, 16].tolist()  # Q列的索引为16，行索引范围为4到22（包含）
        # 创建字典
        fees_dict = dict(zip(fees_names, fees_values))
        return fees_dict

    def get_Need_Confirmation_CIC_Billing_Tracking_202402(self):
        # 读取Excel文件
        df = pd.read_excel(self.read_file_path_Need_Confirmation_202402, sheet_name="CIC Billing Result Tracking")
        # 提取A5-A23和Q5-Q23的数据
        fees_names = df.iloc[3:22, 0].tolist()  # A列的索引为0，行索引范围为4到22（包含）
        fees_values = df.iloc[3:22, 16].tolist()  # Q列的索引为16，行索引范围为4到22（包含）
        # 创建字典
        fees_dict = dict(zip(fees_names, fees_values))
        return fees_dict

    def get_Need_Confirmation_CIC_Billing_Tracking_202403(self):
        # 读取Excel文件
        df = pd.read_excel(self.read_file_path_Need_Confirmation_202403, sheet_name="CIC Billing Result Tracking")
        # 提取A5-A23和Q5-Q23的数据
        fees_names = df.iloc[3:22, 0].tolist()  # A列的索引为0，行索引范围为4到22（包含）
        fees_values = df.iloc[3:22, 16].tolist()  # Q列的索引为16，行索引范围为4到22（包含）
        # 创建字典
        fees_dict = dict(zip(fees_names, fees_values))
        return fees_dict

    def get_Need_Confirmation_CIC_Billing_Tracking_202404(self):
        # 读取Excel文件
        df = pd.read_excel(self.read_file_path_Need_Confirmation_202404, sheet_name="CIC Billing Result Tracking")
        # 提取A5-A23和Q5-Q23的数据
        fees_names = df.iloc[3:22, 0].tolist()  # A列的索引为0，行索引范围为4到22（包含）
        fees_values = df.iloc[3:22, 16].tolist()  # Q列的索引为16，行索引范围为4到22（包含）
        # 创建字典
        fees_dict = dict(zip(fees_names, fees_values))
        return fees_dict

    def get_Need_Confirmation_CIC_Billing_Tracking_202405(self):
        # 读取Excel文件
        df = pd.read_excel(self.read_file_path_Need_Confirmation_202405, sheet_name="CIC Billing Result Tracking")
        # 提取A5-A23和Q5-Q23的数据
        fees_names = df.iloc[3:22, 0].tolist()  # A列的索引为0，行索引范围为4到22（包含）
        fees_values = df.iloc[3:22, 16].tolist()  # Q列的索引为16，行索引范围为4到22（包含）
        # 创建字典
        fees_dict = dict(zip(fees_names, fees_values))
        return fees_dict

    def get_Need_Confirmation_GCG_Billing_Tracking_202401(self):
        # 读取Excel文件
        df = pd.read_excel(self.read_file_path_Need_Confirmation_202401, sheet_name="GCG Billing Result Tracking")
        # 提取A5-A23和Q5-Q23的数据
        fees_names = df.iloc[3:22, 0].tolist()  # A列的索引为0，行索引范围为4到22（包含）
        fees_values = df.iloc[3:22, 16].tolist()  # Q列的索引为16，行索引范围为4到22（包含）
        # 创建字典
        fees_dict = dict(zip(fees_names, fees_values))
        return fees_dict

    def get_Need_Confirmation_GCG_Billing_Tracking_202402(self):
        # 读取Excel文件
        df = pd.read_excel(self.read_file_path_Need_Confirmation_202402, sheet_name="GCG Billing Result Tracking")
        # 提取A5-A23和Q5-Q23的数据
        fees_names = df.iloc[3:22, 0].tolist()  # A列的索引为0，行索引范围为4到22（包含）
        fees_values = df.iloc[3:22, 16].tolist()  # Q列的索引为16，行索引范围为4到22（包含）
        # 创建字典
        fees_dict = dict(zip(fees_names, fees_values))
        return fees_dict

    def get_Need_Confirmation_GCG_Billing_Tracking_202403(self):
        # 读取Excel文件
        df = pd.read_excel(self.read_file_path_Need_Confirmation_202403, sheet_name="GCG Billing Result Tracking")
        # 提取A5-A23和Q5-Q23的数据
        fees_names = df.iloc[3:22, 0].tolist()  # A列的索引为0，行索引范围为4到22（包含）
        fees_values = df.iloc[3:22, 16].tolist()  # Q列的索引为16，行索引范围为4到22（包含）
        # 创建字典
        fees_dict = dict(zip(fees_names, fees_values))
        return fees_dict

    def get_Need_Confirmation_GCG_Billing_Tracking_202404(self):
        # 读取Excel文件
        df = pd.read_excel(self.read_file_path_Need_Confirmation_202404, sheet_name="GCG Billing Result Tracking")
        # 提取A5-A23和Q5-Q23的数据
        fees_names = df.iloc[3:22, 0].tolist()  # A列的索引为0，行索引范围为4到22（包含）
        fees_values = df.iloc[3:22, 16].tolist()  # Q列的索引为16，行索引范围为4到22（包含）
        # 创建字典
        fees_dict = dict(zip(fees_names, fees_values))
        return fees_dict

    def get_Need_Confirmation_GCG_Billing_Tracking_202405(self):
        # 读取Excel文件
        df = pd.read_excel(self.read_file_path_Need_Confirmation_202405, sheet_name="GCG Billing Result Tracking")
        # 提取A5-A23和Q5-Q23的数据
        fees_names = df.iloc[3:22, 0].tolist()  # A列的索引为0，行索引范围为4到22（包含）
        fees_values = df.iloc[3:22, 16].tolist()  # Q列的索引为16，行索引范围为4到22（包含）
        # 创建字典
        fees_dict = dict(zip(fees_names, fees_values))
        return fees_dict


if __name__ == "__main__":
    data_loader = DataLoader()
    #
    # # 获取某个DataFrame
    # df1 = data_loader.get_dataframe("CIC_BMS")
    # if df1 is not None:
    #     print("CIC_BMS:", df1.head())
    #
    # df2 = data_loader.get_dataframe("CIC_CATS_Labor")
    # if df2 is not None:
    #     print("CIC_CATS_Labor:", df2.head())
    #
    # df3 = data_loader.get_dataframe("CIC_CATS_WWER")
    # if df3 is not None:
    #     print("CIC_CATS_WWER:", df3.head())
    #
    # df4 = data_loader.get_dataframe("CIC_ChargeOut_ISB")
    # if df4 is not None:
    #     print("CIC_ChargeOut_ISB:", df4.head())
    #
    # df5 = data_loader.get_dataframe("CIC_ChargeOut_SAP")
    # if df5 is not None:
    #     print("CIC_ChargeOut_SAP:", df5.head())
    #
    # df11 = data_loader.get_dataframe("GCG_BMS")
    # if df11 is not None:
    #     print("GCG_BMS:", df11.head())
    #
    # df12 = data_loader.get_dataframe("GCG_CATS_Labor")
    # if df12 is not None:
    #     print("GCG_CATS_Labor:", df12.head())
    #
    # df13 = data_loader.get_dataframe("GCG_CATS_WWER")
    # if df13 is not None:
    #     print("GCG_CATS_WWER:", df13.head())
    #
    # df14 = data_loader.get_dataframe("GCG_ChargeOut_ISB")
    # if df14 is not None:
    #     print("GCG_ChargeOut_ISB:", df14.head())
    #
    # df15 = data_loader.get_dataframe("GCG_ChargeOut_SAP")
    # if df15 is not None:
    #     print("GCG_ChargeOut_SAP:", df15.head())
    #
    # df6 = data_loader.get_dataframe("user_data")
    # if df6 is not None:
    #     print("user_data:", df6.head())
    #
    # df7 = data_loader.get_dataframe("PM_PMO_PAL")
    # if df7 is not None:
    #     print("PM_PMO_PAL:", df7.head())
    #
    # df8 = data_loader.get_dataframe("Unregister")
    # if df8 is not None:
    #     print("Unregister:", df8.head())
    #
    # df16 = data_loader.get_dataframe("Need_Confirmation")
    # if df16 is not None:
    #     print("Need_Confirmation:", df16.head())

    # df18 = data_loader.get_dataframe("Need_Confirmation_CIC_Billing_Tracking")
    # if df18 is not None:
    #     print("Need_Confirmation_CIC_Billing_Tracking:", df18)
    #
    # df17 = data_loader.get_dataframe("Need_Confirmation_GCG_Billing_Tracking")
    # if df17 is not None:
    #     print("Need_Confirmation_GCG_Billing_Tracking:", df17)

    fees_dict1 = data_loader.get_Need_Confirmation_CIC_Billing_Tracking_202401()
    print("CIC_Billing_Tracking_202401: ", fees_dict1)
    fees_dict2 = data_loader.get_Need_Confirmation_CIC_Billing_Tracking_202402()
    print("CIC_Billing_Tracking_202402: ", fees_dict2)
    fees_dict3 = data_loader.get_Need_Confirmation_CIC_Billing_Tracking_202403()
    print("CIC_Billing_Tracking_202403: ", fees_dict3)
    fees_dict4 = data_loader.get_Need_Confirmation_CIC_Billing_Tracking_202404()
    print("CIC_Billing_Tracking_202404: ", fees_dict4)
    fees_dict5 = data_loader.get_Need_Confirmation_CIC_Billing_Tracking_202405()
    print("CIC_Billing_Tracking_202405: ", fees_dict5)

    fees_dict6 = data_loader.get_Need_Confirmation_GCG_Billing_Tracking_202401()
    print("GCG_Billing_Tracking_202401: ", fees_dict6)
    fees_dict7 = data_loader.get_Need_Confirmation_GCG_Billing_Tracking_202402()
    print("GCG_Billing_Tracking_202402: ", fees_dict7)
    fees_dict8 = data_loader.get_Need_Confirmation_GCG_Billing_Tracking_202403()
    print("GCG_Billing_Tracking_202403: ", fees_dict8)
    fees_dict9 = data_loader.get_Need_Confirmation_GCG_Billing_Tracking_202404()
    print("GCG_Billing_Tracking_202404: ", fees_dict9)
    fees_dict10 = data_loader.get_Need_Confirmation_GCG_Billing_Tracking_202405()
    print("GCG_Billing_Tracking_202405: ", fees_dict10)
