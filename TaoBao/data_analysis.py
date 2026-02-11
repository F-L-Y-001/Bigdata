import pandas as pd
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# 读取
df = pd.read_csv("item_sales.csv")

# 取前20
top20 = df.head(20)

# 画柱状图
plt.figure(figsize=(12, 6))
plt.bar(range(len(top20)), top20['sales'])
plt.xticks(range(len(top20)), top20['item_id'], rotation=90)
plt.title('Top 20 商品销量统计 ——冯立宇')
plt.xlabel('商品ID')
plt.ylabel('销量')
plt.tight_layout()
plt.savefig('sales_top20.png', dpi=300)
plt.show()