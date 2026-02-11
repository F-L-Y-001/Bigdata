import pandas as pd
import json
import os
import re

# ============================
# 配置参数
# ============================
input_file = "./data/bilibili_weekly_data.json"
output_csv = "bilibili_weekly_clean.csv"
txt_file = "bilibili_week.txt"
MAX_TITLE_LEN = 100  # B站标题合理上限（实际通常 ≤80）

# ============================
# 1. 读取 JSON Lines 数据
# ============================
print("📥 正在读取原始数据...")
with open(input_file, "r", encoding="utf-8") as f:
    data_list = json.load(f)

df = pd.DataFrame(data_list)
print(f"✅ 原始数据共 {len(df)} 条记录")

# ============================
# 2. 提取字段 + 清洗 title
# ============================

def clean_title(title):
    """清洗标题：仅保留字符串并去除首尾空白"""
    if not isinstance(title, str) or pd.isna(title):
        return ""
    return title.strip()

# 定义目标列顺序（与后续保存一致）
key = [
    'up', 'time', 'title', 'desc', 'view', 'danmaku', 'reply',
    'favorite', 'coin', 'share', 'like', 'rcmd_reason', 'tname', 'his_rank'
]

_df = pd.DataFrame(columns=key)

# --- 正确提取并赋值基础文本字段 ---
# 注意：必须保证左右列名语义一致！
_df['title'] = df['title'].apply(clean_title)
_df['desc'] = df['desc']
_df['rcmd_reason'] = df['rcmd_reason']
_df['tname'] = df['tname']

# --- 提取 UP 主 ---
_df['up'] = df['owner'].apply(lambda x: x['name'] if isinstance(x, dict) else None)

# --- 提取统计信息 ---
stat_list = df['stat'].apply(lambda x: x if isinstance(x, dict) else {})
_df['view'] = stat_list.apply(lambda x: x.get('view', 0))
_df['danmaku'] = stat_list.apply(lambda x: x.get('danmaku', 0))
_df['reply'] = stat_list.apply(lambda x: x.get('reply', 0))
_df['favorite'] = stat_list.apply(lambda x: x.get('favorite', 0))
_df['coin'] = stat_list.apply(lambda x: x.get('coin', 0))
_df['share'] = stat_list.apply(lambda x: x.get('share', 0))
_df['like'] = stat_list.apply(lambda x: x.get('like', 0))
_df['his_rank'] = stat_list.apply(lambda x: x.get('his_rank', 0))

# --- 提取时间（week）---
_df['time'] = df['week']

# ============================
# 3. 数据清洗
# ============================
print("🧹 开始数据清洗...")

# 删除含任何空值的行（包括 up, title 等关键字段）
initial_count = len(_df)
_df.dropna(how='any', axis=0, inplace=True)
print(f"🗑️  删除 {initial_count - len(_df)} 行（含空值）")

# 过滤异常长标题
long_title_mask = _df['title'].str.len() > MAX_TITLE_LEN
if long_title_mask.any():
    removed_long = long_title_mask.sum()
    _df = _df[~long_title_mask]
    print(f"🗑️  删除 {removed_long} 条标题过长（>{MAX_TITLE_LEN}字符）的记录")

# 去重（以 title + up 为准）
before_dedup = len(_df)
_df = _df.drop_duplicates(subset=['title', 'up'])
print(f"🗑️  删除 {before_dedup - len(_df)} 条重复记录")

# 删除数值异常数据（dislike 已被移除，不再检查）
incorrect_mask = (
    (_df['view'] <= 0) |
    (_df['danmaku'] <= 0) |
    (_df['reply'] <= 0) |
    (_df['favorite'] <= 0) |
    (_df['coin'] <= 0) |
    (_df['share'] <= 0) |
    (_df['like'] <= 0) |
    (_df['his_rank'] <= 0)
)
if incorrect_mask.any():
    removed_bad = incorrect_mask.sum()
    _df = _df[~incorrect_mask]
    print(f"🗑️  删除 {removed_bad} 条数值异常记录")

# ============================
# 4. 填充 desc 和 rcmd_reason 空值（用 title 填充）
# ============================
# desc 为空或 "-" 时用 title 填充
desc_empty = (_df['desc'] == '') | (_df['desc'] == '-') | _df['desc'].isna()
_df.loc[desc_empty, 'desc'] = _df.loc[desc_empty, 'title']

# rcmd_reason 同理
rcmd_empty = (_df['rcmd_reason'] == '') | (_df['rcmd_reason'] == '-') | _df['rcmd_reason'].isna()
_df.loc[rcmd_empty, 'rcmd_reason'] = _df.loc[rcmd_empty, 'title']

# ============================
# 5. 清理文本中的破坏性字符（换行、制表符、逗号、分号等）
# ============================
text_cols = ['title', 'desc', 'rcmd_reason']
for col in text_cols:
    _df[col] = _df[col].astype(str).apply(
        lambda x: re.sub(r'[\n\r\t,;]+', ' ', x).strip() if pd.notna(x) else ""
    )

# ============================
# 6. 按指定顺序整理列（确保输出顺序与 key 一致）
# ============================
_df = _df[key].copy()

# ============================
# 7. 保存结果
# ============================
os.makedirs("data", exist_ok=True)

# 保存为带 BOM 的 UTF-8 CSV（Excel 友好）
_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
print(f"✅ 清洗完成！共 {len(_df)} 条有效记录，已保存为 {output_csv}")

# 保存为 TSV（无表头，供 Spark 使用）
_df.to_csv(txt_file, header=None, index=None, sep='\t', encoding="utf-8")
print(f"✅ TSV 格式已生成：{txt_file}")

# 打印标题长度统计
title_lens = _df['title'].str.len()
print(f"📊 标题长度统计：最小={title_lens.min()}，最大={title_lens.max()}，平均={title_lens.mean():.1f}")