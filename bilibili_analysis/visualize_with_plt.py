import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# 设置中文字体（避免乱码）
plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# 输出目录
output_dir = "plots_plt"
os.makedirs(output_dir, exist_ok=True)

# 姓名标注
author = "——冯立宇"


# ==================== 1. Top10 UP主收录次数 ====================
def plot_top_up():
    df = pd.read_csv("static/top_popular_up.csv")
    df = df.iloc[::-1]  # 反转，使柱状图从上到下递增（美观）
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df['up'], df['popular_up_times'], color='skyblue')
    ax.set_xlabel('收录次数')
    ax.set_title(f'收录次数最多的Top10 UP主{author}')
    # 在柱子上标数值
    for bar in bars:
        width = bar.get_width()
        ax.text(width + 0.5, bar.get_y() + bar.get_height() / 2, f'{int(width)}',
                va='center', ha='left')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'top_up.png'), dpi=300)
    plt.close()


# ==================== 2. Top10 视频分区 ====================
def plot_top_subject():
    df = pd.read_csv("static/top_popular_subject.csv")
    df = df.iloc[::-1]
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df['tname'], df['popular_subject_times'], color='lightcoral')
    ax.set_xlabel('收录次数')
    ax.set_title(f'收录次数最多的Top10视频分区{author}')
    for bar in bars:
        width = bar.get_width()
        ax.text(width + 0.5, bar.get_y() + bar.get_height() / 2, f'{int(width)}',
                va='center', ha='left')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'top_subject.png'), dpi=300)
    plt.close()


# ==================== 3. 播放量Top10视频 ====================
def plot_top_view():
    df = pd.read_csv("static/video_view_data.csv")

    # 安全截断：只对字符串类型处理，跳过 NaN
    def safe_truncate(title):
        if pd.isna(title):
            return "（无标题）"
        title = str(title)
        return title[:20] + '...' if len(title) > 20 else title

    df['title'] = df['title'].apply(safe_truncate)
    df = df.iloc[::-1]
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df['title'], df['view'], color='lightgreen')
    ax.set_xlabel('播放量')
    ax.set_title(f'播放量Top10视频{author}')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'top_view.png'), dpi=300)
    plt.close()


# ==================== 4. 互动指标Top10（弹幕、评论、点赞等）====================
def plot_interaction_metrics():
    metrics = {
        'danmaku': '弹幕数',
        'reply': '评论数',
        'favorite': '收藏数',
        'coin': '投币数',
        'share': '分享数',
        'like': '点赞数'
    }

    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    axes = axes.flatten()

    for idx, (key, name) in enumerate(metrics.items()):
        try:
            df = pd.read_csv(f"static/top_popular_{key}.csv")

            # 安全处理标题
            def safe_truncate(title):
                if pd.isna(title):
                    return "（无标题）"
                title = str(title)
                return title[:15] + '...' if len(title) > 15 else title

            df['title'] = df['title'].apply(safe_truncate)
            df = df.iloc[::-1]
            ax = axes[idx]
            ax.barh(df['title'], df[key], color=plt.cm.tab10(idx))
            ax.set_title(f'{name}Top10视频{author}')
            ax.set_xlabel(name)
        except Exception as e:
            print(f"⚠️ 跳过 {key}: {e}")

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'interaction_metrics.png'), dpi=300)
    plt.close()


# ==================== 5. 标题词云（用柱状图模拟）====================
def plot_wordcloud_bar():
    df = pd.read_csv("static/title_word.csv")
    df = df.head(30).iloc[::-1]  # 取前30，反转
    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.barh(df['word'], df['count'], color='mediumpurple')
    ax.set_xlabel('词频')
    ax.set_title(f'视频标题高频词Top30{author}')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'title_wordcloud.png'), dpi=300)
    plt.close()


# ==================== 6. 特征相关性热力图 ====================
def plot_correlation_heatmap():
    df = pd.read_csv("static/correlation_matrix.csv", index_col=0)
    features = df.columns.tolist()

    fig, ax = plt.subplots(figsize=(8, 6))
    cax = ax.imshow(df.values, cmap='coolwarm', aspect='auto', vmin=-1, vmax=1)
    ax.set_xticks(np.arange(len(features)))
    ax.set_yticks(np.arange(len(features)))
    ax.set_xticklabels(features, rotation=45, ha='right')
    ax.set_yticklabels(features)
    plt.colorbar(cax, ax=ax, shrink=0.8)
    ax.set_title(f'特征斯皮尔曼相关系数热力图{author}')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'correlation_heatmap.png'), dpi=300)
    plt.close()


# ==================== 7. 投币最多视频 & UP主 可视化 ====================
def plot_coin_top_video():
    """投币最多的Top10视频"""
    df = pd.read_csv("static/top_popular_coin.csv")
    df = df.iloc[::-1]  # 反转，从上到下递增

    # 安全截断标题
    def safe_truncate(title):
        if pd.isna(title):
            return "（无标题）"
        title = str(title)
        return title[:20] + '...' if len(title) > 20 else title

    df['title'] = df['title'].apply(safe_truncate)

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df['title'], df['coin'], color='#8B0000')  # 深红色
    ax.set_xlabel('投币数')
    ax.set_title(f'投币最多的视频{author}', fontsize=14, fontweight='bold', pad=20)

    # 在柱子上标数值
    for bar in bars:
        width = bar.get_width()
        ax.text(width + 50000, bar.get_y() + bar.get_height() / 2, f'{int(width):,}',
                va='center', ha='left', fontsize=9, color='white')

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'coin_top_video.png'), dpi=300, bbox_inches='tight')
    plt.close()


def plot_coin_top_up():
    """投币最多的Top10 UP主"""
    df = pd.read_csv("static/top_popular_up_coin.csv")  # 假设你已生成该文件
    df = df.iloc[::-1]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df['up'], df['coin'], color='#8B0000')  # 深红色
    ax.set_xlabel('投币数')
    ax.set_title(f'投币最多的UP主{author}', fontsize=14, fontweight='bold', pad=20)

    # 标注数值
    for bar in bars:
        width = bar.get_width()
        ax.text(width + 50000, bar.get_y() + bar.get_height() / 2, f'{int(width):,}',
                va='center', ha='left', fontsize=9, color='white')

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'coin_top_up.png'), dpi=300, bbox_inches='tight')
    plt.close()

# ==================== 主函数 ====================
if __name__ == '__main__':
    print("🎨 开始生成可视化图表...")

    plot_top_up()
    plot_top_subject()
    plot_top_view()
    plot_interaction_metrics()
    plot_wordcloud_bar()
    plot_correlation_heatmap()

    # 新增：投币数据单独可视化
    plot_coin_top_video()
    plot_coin_top_up()

    print(f"✅ 所有图表已保存至 '{output_dir}' 目录，每张图标题末尾均已标注姓名。")