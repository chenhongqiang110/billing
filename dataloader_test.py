import os
import yaml
import pandas as pd
import time

class DataLoader:
    def __init__(self, config_path=None):
        if "initialized" not in self.__dict__:
            print("Initializing DataLoader...")

            # 确定 DataLoader.py 文件所在的目录
            base_path = os.path.dirname(os.path.abspath(__file__))

            if config_path is None:
                config_path = os.path.join(base_path, 'config.yaml')

            with open(config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)

            self.config = config['file_paths']
            self.dataframes = {}
            self.load_all_tables()
            self.initialized = True

    def load_all_tables(self):
        for report_type in self.config:
            if report_type.startswith('Need_Confirmation'):
                self.load_data_table(report_type)

    def load_data_table(self, report_type):
        start_time = time.time()
        dfs = []
        for month in range(1, 13):
            month_str = f"{month:02d}"
            key = f"{report_type}_2024{month_str}"
            if key in self.config:
                df = pd.read_excel(
                    self.config[key],
                    sheet_name="Confirmation Detail",
                    engine="openpyxl",
                    dtype={"Billing Invoice Number": str},
                )
                dfs.append(df)
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            self.dataframes[report_type] = combined_df
            end_time = time.time()
            print(f"Loaded {report_type} in {end_time - start_time:.2f} seconds")




