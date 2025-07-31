from setuptools import setup

setup(
    name="lydlab",  # 包的名称（pip list中显示的名称）
    version="0.0.2",
    py_modules=["lydlab"],  # 关键：指定单个.py文件（不含.py后缀）
    author="zhaomoqiang",
    description="数据集下载工具",
)
