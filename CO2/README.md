# 全球二氧化碳（CO₂）排放数据分析

> 基于 Global Carbon Budget 2022 官方数据集，分析各国历史碳排放趋势、能源结构贡献及人均排放差异。

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python)
![Pandas](https://img.shields.io/badge/Pandas-Data_Analysis-orange)
![Matplotlib](https://img.shields.io/badge/Matplotlib-Visualization-green)
![PySpark](https://img.shields.io/badge/PySpark-Big_Data_Computing-red?logo=apachespark)

---

## 🔍 项目简介

本项目对 **Global Carbon Project (GCP)** 发布的全球 CO₂ 排放数据进行处理与分析，重点回答以下问题：
- 哪些国家是历史累计和近年排放的主要贡献者？
- 化石燃料（煤、石油、天然气）在各国排放中的占比如何？
- 2000 年后全球排放格局发生了哪些变化？

通过清洗原始数据、聚合多维指标，并生成直观图表，为理解气候变化驱动因素提供数据支持。

---

## 📂 目录结构

```bash

CO2/
├── GCB2022v27_MtCO2_flat.csv # 原始数据（来自 Global Carbon Budget 2022）
├── dataset.csv # 清洗后的结构化数据（由 data_clean.py 生成）
├── data_clean.py # 数据清洗脚本：过滤无效记录、填充缺失值
├── data_analysis.py # 核心分析脚本：PySpark 聚合 + Matplotlib 可视化
├── top5_total.png # 输出：历史总排放 Top5 国家柱状图
└── emissions_2000+.png # 输出：2000年后排放占比饼图

---

## ⚙️ 技术栈

| 功能 | 工具 |
|------|------|
| 数据源 | [Global Carbon Budget 2022](https://doi.org/10.5281/zenodo.7409077) |
| 数据处理 | `pandas`, `PySpark` |
| 可视化 | `matplotlib` |
| 字段说明 | `Country`, `Year`, `Total`, `Coal`, `Oil`, `Gas`, `Cement`, `Flaring`, `Other`, `PerCapita` |

> 💡 单位：百万吨 CO₂（MtCO₂）
