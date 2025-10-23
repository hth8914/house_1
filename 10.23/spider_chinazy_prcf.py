import json, logging, time
from datetime import datetime
from lxml import html
from DrissionPage import ChromiumPage, ChromiumOptions

# ---------- 日志 ----------
log_file = f'spider_chinazy_{datetime.now():%Y%m%d_%H%M%S}.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_file, encoding='utf-8'),
                              logging.StreamHandler()])

# ---------- 浏览器 ----------
co = ChromiumOptions()
co.set_local_port(9222)
page = ChromiumPage(addr_or_opts=co)

# ---------- 核心 ----------
def main():
    start_url = 'https://www.chinazy.org/zcfg.htm'
    page.get(start_url)
    page.wait(1.5)

    seen = set()          # 去重
    policies = []         # 最终结果
    page_num = 0          # 翻页计数

    while True:
        page_num += 1
        logging.info(f'正在解析第 {page_num} 页')
        try:
            # 滚动 3 次，保证懒加载
            for _ in range(3):
                page.scroll.down(800)
                time.sleep(0.8)

            tree = html.fromstring(page.html)
            rows = tree.xpath('//li[a[@target="_blank" and @title and @href]]')
            if not rows:
                logging.warning('本页未解析到任何政策，跳过')
                continue

            new_cnt = 0
            for row in rows:
                href = row.xpath('./a/@href')[0].strip()
                title = row.xpath('./a/@title')[0].strip()
                # 补全绝对路径
                if href.startswith('/'):
                    href = 'https://www.chinazy.org' + href
                elif href.startswith('info/'):
                    href = 'https://www.chinazy.org/' + href

                if href in seen:
                    continue
                seen.add(href)
                policies.append({'名称': title, '链接': href})
                new_cnt += 1

            logging.info(f'第 {page_num} 页新增 {new_cnt} 条，累计 {len(policies)} 条')

            # 实时落盘，防止中断丢失
            with open(f'policies_{datetime.now():%Y%m%d}.json', 'w', encoding='utf-8') as f:
                json.dump(policies, f, ensure_ascii=False, indent=2)

            # 查找“下一页”按钮（通用：文本或 class 包含“下页”/“下一页”）
            next_btn = page.ele('xpath://a[contains(text(),"下页") or contains(text(),"下一页") or contains(@class,"next")]', timeout=3)
            if not next_btn or next_btn.link is None:
                logging.info('已到末页，抓取结束')
                break

            # 点击后等待 DOM 刷新
            next_btn.click()
            time.sleep(2)

        except Exception as e:
            logging.error(f'第 {page_num} 页出现异常，继续下一页', exc_info=True)
            time.sleep(2)
            continue

    logging.info(f'全部完成，共提取 {len(policies)} 条政策')

if __name__ == '__main__':
    try:
        main()
    finally:
        page.close()   # 只关标签，不杀浏览器