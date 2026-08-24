# Deobf by Thesmartcat2303
# Update: farm nhiều link từ link.txt + tùy chỉnh thread (max speed)

import os
import sys
import time
import json
import socket
import ssl
import hashlib
import base64
import platform
import threading
import subprocess
import signal
import random
import queue
import warnings
import requests
import re

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.exceptions import InsecureRequestWarning

warnings.filterwarnings('ignore')
os.environ['PYTHONWARNINGS'] = 'ignore'
requests.packages.urllib3.disable_warnings()
warnings.simplefilter('ignore', InsecureRequestWarning)

LINK_FILE = 'link.txt'


# ==================== COLORS ====================
class GradientColors:
    @staticmethod
    def get_gradient_by_percent(percent):
        r = int(255 * (100 - percent) / 100)
        g = int(255 * percent / 100)
        b = 150
        return f'\x1b[38;2;{r};{g};{b}m'

    @staticmethod
    def hot_gradient():
        return '\x1b[38;2;255;80;0m'

    @staticmethod
    def cold_gradient():
        return '\x1b[38;2;120;180;255m'

    @staticmethod
    def neon_gradient():
        return random.choice([
            '\x1b[38;2;255;0;255m',
            '\x1b[38;2;0;255;255m',
            '\x1b[38;2;255;0;128m'
        ])

    @staticmethod
    def forest_gradient():
        return '\x1b[38;2;0;220;100m'

    @staticmethod
    def party_mode():
        return random.choice([
            '\x1b[38;2;255;0;0m', '\x1b[38;2;255;255;0m',
            '\x1b[38;2;0;255;0m', '\x1b[38;2;0;255;255m',
            '\x1b[38;2;0;128;255m', '\x1b[38;2;255;0;255m'
        ])


Grad = GradientColors()
RESET = '\x1b[0m'


def print_banner():
    banner = """
 ______  __    _    ___ 
|__  / |/ /   / \\  |_ _|
  / /| ' /   / _ \\  | | 
 / /_| . \\  / ___ \\ | | 
/____|_|\\_\\/_/   \\_\\___|
--------------------------------------------
  AUTO VIEW - TIEN LINK VUOTNHANH (MULTI-LINK)
----------------------------------------------"""
    for line in banner.split('\n'):
        color = Grad.neon_gradient()
        print(f'{color}{line}{RESET}')
        time.sleep(0.01)


def load_links(path=LINK_FILE):
    """Đọc link từ file, 1 link 1 dòng. Bỏ dòng trống và dòng #."""
    if not os.path.exists(path):
        print(f'{Grad.hot_gradient()}❌ Không thấy {path}. Tạo file, mỗi dòng 1 link vuotnhanh.{RESET}')
        with open(path, 'w', encoding='utf-8') as f:
            f.write('# Mỗi dòng 1 link vuotnhanh, vd:\n')
            f.write('# https://vuotnhanh.com/xxxxxx\n')
        return []
    links = []
    with open(path, 'r', encoding='utf-8') as f:
        for raw in f:
            line = raw.strip()
            if line and not line.startswith('#'):
                if not line.startswith('http'):
                    line = 'https://' + line
                links.append(line)
    # bỏ trùng, giữ thứ tự
    seen = set()
    uniq = []
    for l in links:
        if l not in seen:
            seen.add(l)
            uniq.append(l)
    return uniq


def ask_threads():
    """Hỏi số thread, mặc định 200."""
    try:
        val = input(f'\n{Grad.cold_gradient()}Số THREAD (càng cao càng nhanh, Enter = 200): {RESET}').strip()
        threads = int(val) if val else 200
    except ValueError:
        threads = 200
    threads = max(10, min(threads, 1000))
    return threads


# ==================== PROXY MINER ====================
class ProxyMinerUltimate:
    def __init__(self):
        self.all_proxies = []
        self.working_proxies = []
        self.used_ips = set()
        self.lock = threading.Lock()
        self.target_check = 'https://vuotnhanh.com'
        self.total_sources = 0
        self.proxy_queue = queue.Queue()

        # ==================== VN SOURCES ====================
        self.vn_sources = [
            {'name': 'ProxyScrape VN all', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=VN&ssl=all&anonymity=all', 'country': 'VN'},
            {'name': 'ProxyScrape VN http', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=VN&ssl=all&anonymity=all', 'country': 'VN'},
            {'name': 'ProxyScrape VN socks4', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks4&timeout=10000&country=VN&ssl=all&anonymity=all', 'country': 'VN'},
            {'name': 'ProxyScrape VN socks5', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=10000&country=VN&ssl=all&anonymity=all', 'country': 'VN'},
            {'name': 'ProxyScrape V3 VN all', 'url': 'https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&country=VN&timeout=10000&protocol=all', 'country': 'VN'},
            {'name': 'Geonode VN', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&country=VN', 'country': 'VN'},
            {'name': 'ProxyList.download VN http', 'url': 'https://www.proxy-list.download/api/v1/get?type=http&country=VN', 'country': 'VN'},
            {'name': 'ProxyList.download VN socks4', 'url': 'https://www.proxy-list.download/api/v1/get?type=socks4&country=VN', 'country': 'VN'},
            {'name': 'ProxyList.download VN socks5', 'url': 'https://www.proxy-list.download/api/v1/get?type=socks5&country=VN', 'country': 'VN'},
            {'name': 'PubProxy VN', 'url': 'http://pubproxy.com/api/proxy?country=VN&limit=20&format=txt', 'country': 'VN'},
            {'name': 'CoolProxy VN', 'url': 'https://cool-proxy.net/proxies.json?country=VN', 'country': 'VN'},
            {'name': 'Spys.one VN', 'url': 'https://spys.one/free-proxy-list/VN/', 'country': 'VN', 'parser': 'html'},
            {'name': 'HideMyName VN', 'url': 'https://hidemy.name/en/proxy-list/?country=VN', 'country': 'VN', 'parser': 'html'},
            {'name': 'ProxyNova VN', 'url': 'https://www.proxynova.com/proxy-server-list/country-vn/', 'country': 'VN', 'parser': 'html'},
            {'name': 'FreeProxy World VN', 'url': 'https://www.freeproxy.world/?country=VN&type=http&page=1', 'country': 'VN', 'parser': 'html'},
        ]

        # ==================== GLOBAL GITHUB RAW ====================
        self.github_sources = [
            {'name': 'TheSpeedX HTTP', 'url': 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt', 'country': 'ALL'},
            {'name': 'TheSpeedX SOCKS4', 'url': 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt', 'country': 'ALL'},
            {'name': 'TheSpeedX SOCKS5', 'url': 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt', 'country': 'ALL'},
            {'name': 'monosans HTTP', 'url': 'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt', 'country': 'ALL'},
            {'name': 'monosans SOCKS4', 'url': 'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt', 'country': 'ALL'},
            {'name': 'monosans SOCKS5', 'url': 'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt', 'country': 'ALL'},
            {'name': 'monosans ALL', 'url': 'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/all.txt', 'country': 'ALL'},
            {'name': 'jetkai HTTP', 'url': 'https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt', 'country': 'ALL'},
            {'name': 'jetkai HTTPS', 'url': 'https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt', 'country': 'ALL'},
            {'name': 'jetkai SOCKS4', 'url': 'https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt', 'country': 'ALL'},
            {'name': 'jetkai SOCKS5', 'url': 'https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt', 'country': 'ALL'},
            {'name': 'ShiftyTR HTTP', 'url': 'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt', 'country': 'ALL'},
            {'name': 'ShiftyTR HTTPS', 'url': 'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt', 'country': 'ALL'},
            {'name': 'ShiftyTR SOCKS4', 'url': 'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt', 'country': 'ALL'},
            {'name': 'ShiftyTR SOCKS5', 'url': 'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt', 'country': 'ALL'},
            {'name': 'clarketm', 'url': 'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt', 'country': 'ALL'},
            {'name': 'hookzof SOCKS5', 'url': 'https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt', 'country': 'ALL'},
            {'name': 'sunny9577', 'url': 'https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt', 'country': 'ALL'},
            {'name': 'roosterkid HTTPS', 'url': 'https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS.txt', 'country': 'ALL'},
            {'name': 'roosterkid SOCKS5', 'url': 'https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5.txt', 'country': 'ALL'},
            {'name': 'proxifly ALL', 'url': 'https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.txt', 'country': 'ALL'},
            {'name': 'proxifly HTTP', 'url': 'https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt', 'country': 'ALL'},
            {'name': 'proxifly SOCKS4', 'url': 'https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks4/data.txt', 'country': 'ALL'},
            {'name': 'proxifly SOCKS5', 'url': 'https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt', 'country': 'ALL'},
            {'name': 'Anonym0usWork HTTP', 'url': 'https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/http_proxies.txt', 'country': 'ALL'},
            {'name': 'Anonym0usWork SOCKS4', 'url': 'https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks4_proxies.txt', 'country': 'ALL'},
            {'name': 'Anonym0usWork SOCKS5', 'url': 'https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks5_proxies.txt', 'country': 'ALL'},
            {'name': 'VPSLab http_all', 'url': 'https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_all.txt', 'country': 'ALL'},
            {'name': 'VPSLab http_ssl', 'url': 'https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_ssl.txt', 'country': 'ALL'},
            {'name': 'VPSLab socks4', 'url': 'https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks4_all.txt', 'country': 'ALL'},
            {'name': 'VPSLab socks5', 'url': 'https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks5_all.txt', 'country': 'ALL'},
            {'name': 'mzyui ALL', 'url': 'https://raw.githubusercontent.com/mzyui/proxy-list/main/all.txt', 'country': 'ALL'},
            {'name': 'mzyui HTTP', 'url': 'https://raw.githubusercontent.com/mzyui/proxy-list/main/http.txt', 'country': 'ALL'},
            {'name': 'mzyui SOCKS4', 'url': 'https://raw.githubusercontent.com/mzyui/proxy-list/main/socks4.txt', 'country': 'ALL'},
            {'name': 'mzyui SOCKS5', 'url': 'https://raw.githubusercontent.com/mzyui/proxy-list/main/socks5.txt', 'country': 'ALL'},
            {'name': 'Thordata ALL', 'url': 'https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/all.txt', 'country': 'ALL'},
            {'name': 'Thordata HTTP', 'url': 'https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/http.txt', 'country': 'ALL'},
            {'name': 'Thordata SOCKS4', 'url': 'https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/socks4.txt', 'country': 'ALL'},
            {'name': 'Thordata SOCKS5', 'url': 'https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/socks5.txt', 'country': 'ALL'},
            {'name': 'ProxyScraper http', 'url': 'https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/http.txt', 'country': 'ALL'},
            {'name': 'ProxyScraper sock4', 'url': 'https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/sock4.txt', 'country': 'ALL'},
            {'name': 'ProxyScraper sock5', 'url': 'https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/sock5.txt', 'country': 'ALL'},
            {'name': 'proxyscrape free-proxy-list ALL', 'url': 'https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/all/data.txt', 'country': 'ALL'},
            {'name': 'proxyscrape free-proxy-list HTTP', 'url': 'https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/protocols/http/data.txt', 'country': 'ALL'},
            {'name': 'proxyscrape free-proxy-list SOCKS4', 'url': 'https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/protocols/socks4/data.txt', 'country': 'ALL'},
            {'name': 'proxyscrape free-proxy-list SOCKS5', 'url': 'https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/protocols/socks5/data.txt', 'country': 'ALL'},
            {'name': 'hproxy ALL', 'url': 'https://raw.githubusercontent.com/hproxy-com/free-proxy-list/main/all.txt', 'country': 'ALL'},
            {'name': 'openproxylist HTTP', 'url': 'https://openproxylist.xyz/http.txt', 'country': 'ALL'},
            {'name': 'openproxylist SOCKS4', 'url': 'https://openproxylist.xyz/socks4.txt', 'country': 'ALL'},
            {'name': 'openproxylist SOCKS5', 'url': 'https://openproxylist.xyz/socks5.txt', 'country': 'ALL'},
            {'name': 'iplocate HTTP', 'url': 'https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/http.txt', 'country': 'ALL'},
            {'name': 'iplocate SOCKS4', 'url': 'https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/socks4.txt', 'country': 'ALL'},
            {'name': 'iplocate SOCKS5', 'url': 'https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/socks5.txt', 'country': 'ALL'},
            {'name': 'fyvri HTTP', 'url': 'https://raw.githubusercontent.com/fyvri/fresh-proxy-list/main/http.txt', 'country': 'ALL'},
            {'name': 'fyvri SOCKS4', 'url': 'https://raw.githubusercontent.com/fyvri/fresh-proxy-list/main/socks4.txt', 'country': 'ALL'},
            {'name': 'fyvri SOCKS5', 'url': 'https://raw.githubusercontent.com/fyvri/fresh-proxy-list/main/socks5.txt', 'country': 'ALL'},
            {'name': 'vmheaven HTTP', 'url': 'https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/main/http.txt', 'country': 'ALL'},
            {'name': 'vmheaven SOCKS4', 'url': 'https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/main/socks4.txt', 'country': 'ALL'},
            {'name': 'vmheaven SOCKS5', 'url': 'https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/main/socks5.txt', 'country': 'ALL'},
            {'name': 'databay ALL', 'url': 'https://cdn.jsdelivr.net/gh/databay-labs/free-proxy-list@main/proxies/all.txt', 'country': 'ALL'},
        ]

        # ==================== API SOURCES ====================
        self.api_sources = [
            {'name': 'ProxyScrape v2 ALL', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=all&ssl=all&anonymity=all', 'country': 'ALL'},
            {'name': 'ProxyScrape v2 HTTP', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all', 'country': 'ALL'},
            {'name': 'ProxyScrape v2 SOCKS4', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks4&timeout=10000&country=all&ssl=all&anonymity=all', 'country': 'ALL'},
            {'name': 'ProxyScrape v2 SOCKS5', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=10000&country=all&ssl=all&anonymity=all', 'country': 'ALL'},
            {'name': 'ProxyScrape v3 ALL', 'url': 'https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&timeout=10000&protocol=all', 'country': 'ALL'},
            {'name': 'ProxyScrape v4', 'url': 'https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=ipport&format=text', 'country': 'ALL'},
            {'name': 'Geonode global HTTP', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps', 'country': 'ALL'},
            {'name': 'Geonode global SOCKS', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=socks4%2Csocks5', 'country': 'ALL'},
            {'name': 'ProxyList.download HTTP', 'url': 'https://www.proxy-list.download/api/v1/get?type=http', 'country': 'ALL'},
            {'name': 'ProxyList.download HTTPS', 'url': 'https://www.proxy-list.download/api/v1/get?type=https', 'country': 'ALL'},
            {'name': 'ProxyList.download SOCKS4', 'url': 'https://www.proxy-list.download/api/v1/get?type=socks4', 'country': 'ALL'},
            {'name': 'ProxyList.download SOCKS5', 'url': 'https://www.proxy-list.download/api/v1/get?type=socks5', 'country': 'ALL'},
            {'name': 'PubProxy random', 'url': 'http://pubproxy.com/api/proxy?limit=20&format=txt', 'country': 'ALL'},
            {'name': 'HProxy', 'url': 'https://hproxy.com/api/proxy-list?format=txt', 'country': 'ALL'},
            {'name': 'Databay', 'url': 'https://databay.com/api/v1/proxy-list?format=txt&limit=1000', 'country': 'ALL'},
            {'name': 'CoolProxy global', 'url': 'https://cool-proxy.net/proxies.json', 'country': 'ALL'},
        ]

        # ==================== HTML SITES ====================
        self.html_sources = [
            {'name': 'free-proxy-list.net', 'url': 'https://free-proxy-list.net/', 'country': 'ALL', 'parser': 'html'},
            {'name': 'sslproxies.org', 'url': 'https://www.sslproxies.org/', 'country': 'ALL', 'parser': 'html'},
            {'name': 'us-proxy.org', 'url': 'https://www.us-proxy.org/', 'country': 'US', 'parser': 'html'},
            {'name': 'socks-proxy.net', 'url': 'https://www.socks-proxy.net/', 'country': 'ALL', 'parser': 'html'},
            {'name': 'spys.one HTTP', 'url': 'https://spys.one/en/http-proxy-list/', 'country': 'ALL', 'parser': 'html'},
            {'name': 'spys.one SOCKS', 'url': 'https://spys.one/en/socks-proxy-list/', 'country': 'ALL', 'parser': 'html'},
            {'name': 'hidemy.name HTTP', 'url': 'https://hidemy.name/en/proxy-list/?type=hs', 'country': 'ALL', 'parser': 'html'},
            {'name': 'hidemy.name SOCKS', 'url': 'https://hidemy.name/en/proxy-list/?type=45', 'country': 'ALL', 'parser': 'html'},
            {'name': 'proxynova global', 'url': 'https://www.proxynova.com/proxy-server-list/', 'country': 'ALL', 'parser': 'html'},
            {'name': 'proxylistplus HTTP1', 'url': 'https://list.proxylistplus.com/Fresh-HTTP-Proxy-List-1', 'country': 'ALL', 'parser': 'html'},
            {'name': 'proxylistplus HTTP2', 'url': 'https://list.proxylistplus.com/Fresh-HTTP-Proxy-List-2', 'country': 'ALL', 'parser': 'html'},
            {'name': 'proxylistplus SSL', 'url': 'https://list.proxylistplus.com/SSL-List-1', 'country': 'ALL', 'parser': 'html'},
            {'name': 'proxylistplus SOCKS', 'url': 'https://list.proxylistplus.com/SOCKS-List-1', 'country': 'ALL', 'parser': 'html'},
            {'name': 'free-proxy.cz', 'url': 'http://free-proxy.cz/en/proxylist/country/all/http/ping/all', 'country': 'ALL', 'parser': 'html'},
            {'name': 'proxydb', 'url': 'https://proxydb.net/?protocol=http&protocol=https&protocol=socks4&protocol=socks5', 'country': 'ALL', 'parser': 'html'},
        ]

        # ==================== COUNTRY SPECIFIC ====================
        self.country_sources = []
        countries = ['US','CN','ID','BR','IN','RU','DE','FR','GB','JP','KR','TH','MY','SG','PH','BD','PK','TR','UA','MX','AR','CL','CO','ZA','EG','NG','IT','ES','NL','PL','CA','AU','TW','VN']
        for c in countries:
            self.country_sources.append({'name': f'ProxyScrape {c}', 'url': f'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country={c}&ssl=all&anonymity=all', 'country': c})
            self.country_sources.append({'name': f'Geonode {c}', 'url': f'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country={c}', 'country': c})

        self.all_sources = self.vn_sources + self.github_sources + self.api_sources + self.html_sources + self.country_sources
        self.total_sources = len(self.all_sources)
        print(f'{Grad.forest_gradient()}✅ ĐÃ KHỞI TẠO {self.total_sources} NGUỒN PROXY (WORLDWIDE){RESET}')

    def get_gradient_color(self, percent):
        return Grad.get_gradient_by_percent(percent)

    def create_new_session(self):
        session = requests.Session()
        session.headers.update({
            'User-Agent': random.choice([
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
                'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15',
                'Mozilla/5.0 (Linux; Android 13; SM-S908B) AppleWebKit/537.36'
            ])
        })
        session.verify = False
        return session

    def parse_html_table(self, html):
        from bs4 import BeautifulSoup
        proxies = []
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    ip_cell = cells[0].get_text(strip=True)
                    port_cell = cells[1].get_text(strip=True)
                    if re.match(r'^\d+\.\d+\.\d+\.\d+$', ip_cell):
                        if port_cell.isdigit():
                            proxies.append(f'{ip_cell}:{port_cell}')
        return proxies

    def source_worker(self, source, source_index, total_sources):
        try:
            session = self.create_new_session()
            resp = session.get(source['url'], timeout=25)
            if resp.status_code == 200:
                proxies = []
                if source.get('parser') == 'html':
                    proxies = self.parse_html_table(resp.text)
                else:
                    content_type = resp.headers.get('Content-Type', '')
                    text = resp.text.strip()
                    if 'json' in content_type or text.startswith('{') or text.startswith('['):
                        try:
                            data = resp.json()
                            if isinstance(data, dict) and 'data' in data:
                                for item in data['data']:
                                    ip = item.get('ip') or item.get('host')
                                    port = item.get('port')
                                    if ip and port:
                                        proxies.append(f'{ip}:{port}')
                            elif isinstance(data, list):
                                for item in data:
                                    if isinstance(item, dict):
                                        ip = item.get('ip') or item.get('host')
                                        port = item.get('port')
                                        if ip and port:
                                            proxies.append(f'{ip}:{port}')
                                    elif isinstance(item, str) and ':' in item:
                                        proxies.append(item.strip())
                        except:
                            lines = text.split('\n')
                            proxies = [p.strip() for p in lines if p.strip() and ':' in p]
                    else:
                        lines = text.split('\n')
                        proxies = [p.strip() for p in lines if p.strip() and ':' in p]

                new_proxies = []
                for proxy in proxies:
                    proxy = proxy.strip()
                    if '://' in proxy:
                        proxy = proxy.split('://')[-1]
                    if ':' in proxy:
                        parts = proxy.split(':')
                        ip = parts[0]
                        if re.match(r'^\d+\.\d+\.\d+\.\d+$', ip):
                            new_proxies.append({
                                'proxy': f'{ip}:{parts[1].split()[0]}' if len(parts) > 1 else proxy,
                                'ip': ip,
                                'source': source['name'],
                                'country': source.get('country', 'ALL')
                            })

                with self.lock:
                    for p in new_proxies:
                        if p['ip'] not in self.used_ips:
                            self.used_ips.add(p['ip'])
                            self.all_proxies.append(p)
                            self.proxy_queue.put(p)

            percent = source_index / total_sources * 100
            color = self.get_gradient_color(percent)
            sys.stdout.write(f'\r{color}[ZKAI] ĐÀO PROXY [{percent:.1f}%] {RESET}')
            sys.stdout.flush()
        except Exception:
            pass

    def fetch_all_sources_multithread(self):
        with self.lock:
            self.all_proxies = []
            self.used_ips = set()

        print(f'\n{Grad.hot_gradient()}🌟 ZKAI - ĐANG ĐÀO {self.total_sources} NGUỒN PROXY WORLDWIDE{RESET}')

        with ThreadPoolExecutor(max_workers=min(80, self.total_sources)) as executor:
            futures = []
            for i, source in enumerate(self.all_sources, 1):
                future = executor.submit(self.source_worker, source, i, self.total_sources)
                futures.append(future)
            for future in as_completed(futures):
                pass

        print()
        total_new = len(self.all_proxies)
        if total_new != 0:
            print(f'{Grad.forest_gradient()}✅ [ZKAI] Ok. Đã đào được {total_new} proxy{RESET}')
        else:
            print(f'{Grad.hot_gradient()}❌ [ZKAI] Tài nguyên proxy bay sạch.{RESET}')

        return self.all_proxies

    def test_proxy_worker(self, proxy_info, result_list):
        proxy = proxy_info['proxy']
        proxies = {
            'http': f'http://{proxy}',
            'https': f'http://{proxy}'
        }
        headers = {'User-Agent': 'Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36'}

        try:
            start = time.time()
            resp = requests.get(self.target_check, proxies=proxies, headers=headers, timeout=4, verify=False)
            latency = time.time() - start

            if resp.status_code == 200:
                with self.lock:
                    result_list.append({
                        'proxy': proxy,
                        'ip': proxy_info['ip'],
                        'country': proxy_info['country'],
                        'source': proxy_info['source'],
                        'latency': round(latency, 2)
                    })
        except:
            pass

    def filter_proxies_multithread(self, max_workers=400):
        if not self.all_proxies:
            return []

        results = []
        total = len(self.all_proxies)
        checked = 0

        print(f'\n{Grad.cold_gradient()}🔍 ZKAI - CHECK {total} PROXY...{RESET}')

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for proxy in self.all_proxies:
                future = executor.submit(self.test_proxy_worker, proxy, results)
                futures.append(future)

            for future in as_completed(futures):
                checked += 1
                percent = checked / total * 100
                color = self.get_gradient_color(percent)
                sys.stdout.write(f'\r{color}[ZKAI] CHECK PROXY [{percent:.1f}%] | SỐNG: {len(results)}{RESET}')
                sys.stdout.flush()

        print()
        results.sort(key=lambda x: x['latency'])
        self.working_proxies = results

        if results:
            print(f'{Grad.forest_gradient()}✅ [ZKAI] Còn {len(results)} proxy sống{RESET}')
        else:
            print(f'{Grad.hot_gradient()}❌ [ZKAI] DIE MẸ TÀI NGUYÊN RỒI =((({RESET}')

        return results


# ==================== MULTI-LINK FARMER ====================
class MultiLinkFarmer:
    """Bắn nhiều link cùng lúc: mỗi proxy x mỗi link = 1 job."""

    EARN_PER_HIT = 12

    def __init__(self, links, threads):
        self.links = links
        self.threads = threads
        self.success_count = 0
        self.earned = 0
        # (ip, link) đã thành công -> không bắn lại trong phiên
        self.done_pairs = set()
        self.link_stats = {l: {'ok': 0, 'fail': 0} for l in links}
        self.lock = threading.Lock()

    def create_session_for_proxy(self, proxy_info):
        session = requests.Session()
        session.verify = False
        session.proxies = {
            'http': f'http://{proxy_info["proxy"]}',
            'https': f'http://{proxy_info["proxy"]}'
        }
        session.headers.update({
            'User-Agent': random.choice([
                'Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36',
                'Mozilla/5.0 (Linux; Android 11; SM-M127G) AppleWebKit/537.36',
                'Mozilla/5.0 (Linux; Android 12; SM-A125F) AppleWebKit/537.36',
                'Mozilla/5.0 (Linux; Android 13; Pixel 6) AppleWebKit/537.36'
            ])
        })
        return session

    def click_once(self, proxy_info, target_url):
        """1 proxy vào 1 link, lấy csrf rồi POST /go/sf."""
        ip = proxy_info['ip']
        try:
            session = self.create_session_for_proxy(proxy_info)
            resp = session.get(target_url, timeout=15)

            if resp.status_code == 200:
                csrf_match = re.search(
                    r'<meta[^>]*name=["\']csrf-token["\'][^>]*content=["\']([^"\']+)',
                    resp.text
                )
                if csrf_match:
                    csrf = csrf_match.group(1)
                    api_resp = session.post(
                        'https://vuotnhanh.com/go/sf',
                        headers={
                            'X-CSRF-TOKEN': csrf,
                            'X-Requested-With': 'XMLHttpRequest'
                        },
                        data={'alias': 'hbiV'},
                        timeout=10
                    )

                    if api_resp.status_code == 200:
                        try:
                            result = api_resp.json()
                            if result.get('status') == 'success' or result.get('url_redirect'):
                                return True
                        except:
                            pass
            return False
        except Exception:
            return False

    def worker(self, proxy_info, target_url, counters):
        key = (proxy_info['ip'], target_url)
        ok = self.click_once(proxy_info, target_url)

        with self.lock:
            if ok and key not in self.done_pairs:
                self.done_pairs.add(key)
                self.success_count += 1
                self.earned += self.EARN_PER_HIT
                self.link_stats[target_url]['ok'] += 1
                counters['ok'] += 1
                color = Grad.party_mode()
                now = time.strftime('%H:%M:%S')
                print(f'\n{color}[ZKAI] [{now}] | THÀNH CÔNG | [{proxy_info["ip"]}] | link #{self.links.index(target_url)+1} | [+12đ]{RESET}')
            else:
                self.link_stats[target_url]['fail'] += 1
                counters['fail'] += 1

            counters['done'] += 1
            percent = counters['done'] / counters['total'] * 100
            c = Grad.get_gradient_by_percent(percent)
            sys.stdout.write(
                f'\r{c}[ZKAI] FARM [{percent:.1f}%] | OK: {counters["ok"]} | FAIL: {counters["fail"]} | TỔNG: {self.earned}đ{RESET}'
            )
            sys.stdout.flush()

    def run_with_proxies_multithread(self, proxies):
        if not proxies:
            print('\n❌ Không có proxy để chạy!')
            return 0

        # job list: mỗi proxy bắn từng link 1 lần
        jobs = [(p, l) for l in self.links for p in proxies]
        random.shuffle(jobs)
        total = len(jobs)

        print(f'\n{Grad.hot_gradient()}🚀 ZKAI - BẮT ĐẦU FARM: {len(proxies)} PROXY x {len(self.links)} LINK = {total} JOB | {self.threads} THREADS{RESET}\n')

        counters = {'done': 0, 'ok': 0, 'fail': 0, 'total': total}

        with ThreadPoolExecutor(max_workers=min(self.threads, total)) as executor:
            futures = [executor.submit(self.worker, p, l, counters) for p, l in jobs]
            for future in as_completed(futures):
                pass

        print()
        return self.success_count

    def print_summary(self):
        print(f'\n{Grad.neon_gradient()}📊 THỐNG KÊ THEO LINK:{RESET}')
        for i, (link, st) in enumerate(self.link_stats.items(), 1):
            print(f'   {i}. {link[:50]:<52} OK: {st["ok"]} | FAIL: {st["fail"]}')
        print(f'\n{Grad.forest_gradient()}💰 TỔNG CỘNG: {self.success_count} clicks, {self.earned}đ{RESET}')


def countdown(seconds, msg='⏳ ĐỢI'):
    for i in range(seconds, 0, -1):
        percent = (seconds - i) / seconds * 100
        color = Grad.get_gradient_by_percent(percent)
        sys.stdout.write(f'\r{color}{msg} {i}s...{RESET}')
        sys.stdout.flush()
        time.sleep(1)
    print()


def main_program():
    warnings.filterwarnings('ignore')
    requests.packages.urllib3.disable_warnings()

    print_banner()

    # ---- ĐỌC LINK ----
    links = load_links()
    if not links:
        sys.exit(1)

    print(f'\n{Grad.forest_gradient()}✅ Đọc được {len(links)} link từ {LINK_FILE}:{RESET}')
    for i, l in enumerate(links, 1):
        print(f'   {i}. {l}')

    # ---- THREAD ----
    threads = ask_threads()
    print(f'{Grad.forest_gradient()}✅ Dùng {threads} threads{RESET}')

    miner = ProxyMinerUltimate()
    farmer = MultiLinkFarmer(links, threads)

    cycle = 0
    while True:
        cycle += 1
        print(f'\n{Grad.neon_gradient()}========== VÒNG {cycle} =========={RESET}')

        all_proxies = miner.fetch_all_sources_multithread()

        if all_proxies:
            working = miner.filter_proxies_multithread(max_workers=max(threads, 400))
            if working:
                print(f'\n{Grad.neon_gradient()}🏆 TOP 10 PROXY NHANH NHẤT:{RESET}')
                for i, p in enumerate(working[:10], 1):
                    print(f'   {i}. {p["proxy"]} - {p["country"]} - {p["latency"]}s')

                farmer.run_with_proxies_multithread(working)
                farmer.print_summary()
                print(f'\n{Grad.forest_gradient()}📈 LŨY KẾ: {farmer.success_count} clicks, {farmer.earned}đ{RESET}')

                countdown(5, '⏳ NGHỈ')
                continue

        countdown(30, '❌ Hết tài nguyên. Đợi')


if __name__ == '__main__':
    try:
        from bs4 import BeautifulSoup
        main_program()
    except ImportError:
        print(f'{Grad.hot_gradient()}📦 Đang cài beautifulsoup4...{RESET}')
        os.system('pip install beautifulsoup4')
        from bs4 import BeautifulSoup
        main_program()
