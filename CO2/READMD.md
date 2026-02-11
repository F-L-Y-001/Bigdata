# Bilibili 热门视频多维度分析系统

> 基于 Bilibili 官方每周热门榜单（第1–354期），对视频数据进行爬取、清洗、统计分析与可视化，挖掘用户行为偏好与内容生态特征。

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python)
![Spark](https://img.shields.io/badge/Apache_Spark-3.x-orange?logo=apachespark)
![Matplotlib](https://img.shields.io/badge/Matplotlib-Visualization-green?logo=matplotlib)

---

## 🔍 项目简介

本项目通过自动化脚本采集 Bilibili 每周热门视频数据（约数万条记录），利用 **PySpark** 进行高效分布式计算，结合 **Pandas + Matplotlib** 生成高质量可视化图表，从以下维度深入分析：

- 🔝 收录次数最多的 Top10 UP 主与视频分区  
- 👀 播放量、弹幕、评论、点赞、投币、收藏、分享 Top10 视频  
- 💬 视频标题高频词云（中文分词 + 停用词过滤）  
- 📊 用户互动指标（播放、点赞、投币等）间的斯皮尔曼相关性  
- 🎯 基于历史排名的二分类模型（预测是否进入 Top10）

所有图表均标注作者信息，可直接用于报告或展示。
