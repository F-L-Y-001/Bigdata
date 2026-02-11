import requests
import time
import random
import json
import os

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.bilibili.com/v/popular/weekly",
    "Origin": "https://www.bilibili.com",
    "Accept": "application/json, text/plain, */*",
    "Cookie": "buvid3=EEBDF40C-AC32-84F0-61A9-5DC3B034D40F04596infoc; b_nut=1752644704; _uuid=96DB3FC10-AB610-7AE9-AA6C-A5EF4FCA810BD05924infoc; buvid_fp=a71c3b28b831b2bc72bd870576e24ddd; buvid4=D103356D-DB8F-3585-8C07-550DB84A64C306421-025071613-dXL3ZGBAOAoWCER0agkl6Q%3D%3D; enable_web_push=DISABLE; home_feed_column=5; browser_resolution=1699-911; PVID=1; LIVE_BUVID=AUTO4017565551693265; CURRENT_FNVAL=4048; CURRENT_QUALITY=0; rpdid=0zbfAHypEj|kqLz4TCH|3Uj|3w1VBMTQ; b_lsid=7C17ECC5_19B9249115E; bili_ticket=eyJhbGciOiJIUzI1NiIsImtpZCI6InMwMyIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3Njc5NDUwMjAsImlhdCI6MTc2NzY4NTc2MCwicGx0IjotMX0.1KPoI5ktvjSVH-bFKwaQaheHKAe7z08DtoRj0MJxne0; bili_ticket_expires=1767944960; SESSDATA=aba18c12%2C1783237890%2C4dac0%2A12; bili_jct=e187033b5f4e61a1f50bfed7c3bdfadb; DedeUserID=516994885; DedeUserID__ckMd5=55d1014baacd40a7; sid=6ohutpjf"
}


def fetch_weekly_data(week_num, max_retries=3):
    url = f"https://api.bilibili.com/x/web-interface/popular/series/one?number={week_num}"
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            data = resp.json()
            if data["code"] == 0:
                return data["data"]["list"]
            elif data["code"] == -352:
                print(f"第 {week_num} 期返回 -352（风控），第 {attempt + 1} 次重试...")
                time.sleep(5)
            else:
                print(f"第 {week_num} 期返回错误码: {data['code']}")
                break
        except Exception as e:
            print(f"第 {week_num} 期异常: {e}")
            time.sleep(5)
    return None


def main():
    START_WEEK = 1
    END_WEEK = 354
    all_videos = []

    # 尝试加载已有数据（断点续爬）
    temp_file = "data/temp_bilibili_weekly_data.json"
    if os.path.exists(temp_file):
        with open(temp_file, "r", encoding="utf-8") as f:
            all_videos = json.load(f)
        already_done = {v["week"] for v in all_videos}
        START_WEEK = max(already_done) + 1
        print(f"检测到已爬取至第 {START_WEEK - 1} 期，从第 {START_WEEK} 期继续...")

    for week in range(START_WEEK, END_WEEK + 1):
        print(f"[{time.strftime('%H:%M:%S')}] 正在爬取第 {week} 期...")
        videos = fetch_weekly_data(week)
        if videos is not None:
            for v in videos:
                item = v.copy()
                item["week"] = week
                all_videos.append(item)
            print(f"第 {week} 期：获取 {len(videos)} 个视频")
        else:
            print(f"第 {week} 期：跳过（风控或无数据）")

        # 保存临时进度
        os.makedirs("data", exist_ok=True)
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(all_videos, f, ensure_ascii=False, indent=2)

        # 关键：加大延迟！
        delay = random.uniform(2.5, 4.0)
        print(f"等待 {delay:.1f} 秒...\n")
        time.sleep(delay)

    # 最终保存
    with open("data/bilibili_weekly_data.json", "w", encoding="utf-8") as f:
        json.dump(all_videos, f, ensure_ascii=False, indent=2)
    print("✅ 全部完成！")


if __name__ == "__main__":
    main()