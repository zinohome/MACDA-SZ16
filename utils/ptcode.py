#!/usr/bin/python3
# -*- coding:utf-8 -*-
"""
@author: ibmzhangjun@139.com
@file: ptcode.py
@time: 2025/7/16 下午4:22
@desc: 
"""
import os

import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'predictdata')

class PTCode:
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._init_data()
        return cls._instance

    def _init_data(self):
        # 读取 S16_part_code.xlsx 文件
        part_code_df = pd.read_excel(os.path.join(DATA_DIR, 'S16_part_code.xlsx'))
        # 读取 S16_predict_code.xlsx 文件
        predict_code_df = pd.read_excel(os.path.join(DATA_DIR, 'S16_predit_code.xlsx'))

        # 假设车厢号列名为 '车厢号'，去掉车厢号中的所有空格后作为键
        part_code_df['part_name'] = part_code_df['part_name'].astype(str).str.replace(' ', '')
        predict_code_df['warning_name'] = predict_code_df['warning_name'].astype(str).str.replace(' ', '')

        # 假设车厢号列名为 '车厢号'，将数据转换为字典
        self.part_code_dict = part_code_df.set_index('part_name').apply(lambda row: row.to_dict(), axis=1).to_dict()
        self.predict_code_dict = predict_code_df.set_index('warning_name').apply(lambda row: row.to_dict(), axis=1).to_dict()

    def get_part_code_data(self, part_name):
        """获取 S16_part_code.xlsx 文件中指定车厢号的数据"""
        return self.part_code_dict.get(part_name)

    def get_predict_code_data(self, part_name):
        """获取 S16_predict_code.xlsx 文件中指定车厢号的数据"""
        return self.predict_code_dict.get(part_name)

    def get_part_map(self, part_name):
        part_mapping = {
            "通风机累计运行时间-U11": "机组1通风机累计运行时间",
            "通风机累计运行时间-U21": "机组2通风机累计运行时间",
            "冷凝风机累计运行时间-U11": "机组1冷凝风机1累计运行时间",
            "冷凝风机累计运行时间-U12": "机组1冷凝风机2累计运行时间",
            "冷凝风机累计运行时间-U21": "机组2冷凝风机1累计运行时间",
            "冷凝风机累计运行时间-U22": "机组2冷凝风机2累计运行时间",
            "压缩机累计运行时间-U11": "机组1压缩机1累计运行时间",
            "压缩机累计运行时间-U12": "机组1压缩机2累计运行时间",
            "压缩机累计运行时间-U21": "机组2压缩机1累计运行时间",
            "压缩机累计运行时间-U22": "机组2压缩机2累计运行时间",
            "新风阀开关次数-U1": "机组1新风阀开关次数",
            "回风阀开关次数-U1": "机组1回风阀开关次数",
            "新风阀开关次数-U2": "机组2新风阀开关次数",
            "回风阀开关次数-U2": "机组2回风阀开关次数",
            "冷媒泄露预警 U-11": "机组1系统1冷媒泄露预警",
            "冷媒泄露预警 U-12": "机组1系统2冷媒泄露预警",
            "冷媒泄露预警 U-21": "机组2系统1冷媒泄露预警",
            "冷媒泄露预警 U-22": "机组2系统2冷媒泄露预警",
            "制冷系统预警 U-1": "机组1制冷系统（压缩机电流差值过大）预警",
            "制冷系统预警 U-2": "机组2制冷系统（压缩机电流差值过大）预警",
            "新风温度传感器预警": "新风温度传感器预警",
            "回风温度传感器预警": "回风温度传感器预警",
            "车厢温度超温预警": "车厢温度超温预警",
            "滤网脏堵预警 U-1": "机组1滤网脏堵预警",
            "滤网脏堵预警 U-2": "机组2滤网脏堵预警",
            "通风机电流预警 U-11": "机组1通风机1电流预警",
            "通风机电流预警 U-12": "机组1通风机2电流预警",
            "通风机电流预警 U-21": "机组2通风机1电流预警",
            "通风机电流预警 U-22": "机组2通风机2电流预警",
            "冷凝风机电流预警 U-11": "机组1冷凝风机1电流预警",
            "冷凝风机电流预警 U-12": "机组1冷凝风机2电流预警",
            "冷凝风机电流预警 U-21": "机组2冷凝风机1电流预警",
            "冷凝风机电流预警 U-22": "机组2冷凝风机2电流预警",
            "压缩机电流预警 U-11": "机组1压缩机1电流预警",
            "压缩机电流预警 U-12": "机组1压缩机2电流预警",
            "压缩机电流预警 U-21": "机组2压缩机1电流预警",
            "压缩机电流预警 U-22": "机组2压缩机2电流预警",
            "空气质量监测终端预警 U-1": "空气质量预警",
            "空气质量监测终端预警 U-2": "空气质量预警"
        }
        return part_mapping.get(part_name, "未知部件")

    def get_component_map(self, part_name):
        component_mapping = {
            "通风机累计运行时间-U11": "机组1通风机",
            "通风机累计运行时间-U21": "机组2通风机",
            "冷凝风机累计运行时间-U11": "机组1冷凝风机1",
            "冷凝风机累计运行时间-U12": "机组1冷凝风机2",
            "冷凝风机累计运行时间-U21": "机组2冷凝风机1",
            "冷凝风机累计运行时间-U22": "机组2冷凝风机2",
            "压缩机累计运行时间-U11": "机组1压缩机1",
            "压缩机累计运行时间-U12": "机组1压缩机2",
            "压缩机累计运行时间-U21": "机组2压缩机1",
            "压缩机累计运行时间-U22": "机组2压缩机2"
        }
        return component_mapping.get(part_name, "未知部件")

    def get_part_code(self, part_name, carriage_no):
        """根据车厢号和部件名称获取对应的 part_code"""
        if self.get_part_map(part_name) == '未知部件':
            return '1x079999'
        else:
            npart_name = f"{carriage_no}车{self.get_part_map(part_name)}"
            return self.part_code_dict.get(npart_name).get('part_code')

    def get_predict_code(self, part_name, carriage_no):
        """根据车厢号和部件名称获取对应的 part_code"""
        if self.get_part_map(part_name) == '未知部件':
            return '1x079999'
        else:
            npart_name = f"{carriage_no}车{self.get_part_map(part_name)}"
            return self.predict_code_dict.get(npart_name).get('warning_code')

if __name__ == "__main__":
    # 创建 PTCode 单例实例
    pt_code = PTCode()
    # 打印 part_code_dict
    print("S16_part_code.xlsx 转换后的字典长度:", len(pt_code.part_code_dict))
    print("S16_part_code.xlsx 转换后的字典键值对:")
    print(pt_code.part_code_dict)
    # 打印 predict_code_dict
    print("\nS16_predict_code.xlsx 转换后的字典长度:", len(pt_code.predict_code_dict))
    #print("S16_predict_code.xlsx 转换后的字典键值对:")
    print(pt_code.predict_code_dict)
    print(pt_code.get_part_map('压缩机累计运行时间-U11'))
    print(pt_code.get_part_code('压缩机累计运行时间-U11',6))
    print(pt_code.get_part_map('冷媒泄露预警 U-11'))
    print(pt_code.get_predict_code('冷媒泄露预警 U-11',6))