#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
r2.py — ZKAI farm đa link (upgrade từ r.txt)
- Đọc danh sách link từ link.txt, round-robin qua từng proxy
- Đào proxy từ ~130 nguồn free (github + api + html + quốc gia)
- Gắn đúng protocol (http/https/socks4/socks5) -> bắt được nhiều proxy sống hơn
- Test timeout nới lên 6s + click retry 2 lần -> tỷ lệ sống cao
- Auto chạy, không hỏi link, không hỏi key
"""

import os
import sys
import time
import re
import json
import random
import queue
import socket
import threading
import warnings
import subprocess

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")
requests.packages.urllib3.disable_warnings()

# ==================== PROXY PROTOCOL SUPPORT ====================
try:
    import socks  # PySocks — cần cho socks4/socks5
except ImportError:
    print("[*] Cài PySocks cho proxy SOCKS...")
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "PySocks"], check=False)
    try:
        import socks  # noqa
    except ImportError:
        print("[!] Không cài được PySocks — chỉ dùng proxy http/https.")

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("[*] Cài beautifulsoup4...")
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "beautifulsoup4"], check=False)
    from bs4 import BeautifulSoup


# ==================== GRADIENT UI (giữ style ZKAI) ====================
class Grad:
    @staticmethod
    def _rgb(r, g, b):
        return f"\x1b[38;2;{r};{g};{b}m"

    @staticmethod
    def RESET():
        return "\x1b[0m"

    @staticmethod
    def color(text, r, g, b):
        return f"{Grad._rgb(r, g, b)}{text}{Grad.RESET()}"

    @staticmethod
    def hot(text):
        return Grad.color(text, 255, 60, 60)

    @staticmethod
    def cold(text):
        return Grad.color(text, 60, 160, 255)

    @staticmethod
    def neon(text):
        return Grad.color(text, 60, 255, 220)

    @staticmethod
    def forest(text):
        return Grad.color(text, 60, 220, 120)

    @staticmethod
    def gold(text):
        return Grad.color(text, 255, 200, 60)

    @staticmethod
    def party(text):
        return Grad.color(text, random.randint(120, 255), random.randint(80, 255), random.randint(80, 255))

    @staticmethod
    def by_percent(percent):
        if percent < 33:
            return Grad._rgb(255, int(160 * percent / 33), 60)
        if percent < 66:
            return Grad._rgb(int(255 * (1 - (percent - 33) / 33)), 220, 60)
        return Grad._rgb(60, 255, int(200 * (1 - (percent - 66) / 34)))

# ==================== PROXY SOURCES ====================
# Tong cong: 190 nguon proxy
ALL_PROXY_SOURCES = [
    {'name': 'ProxyScrape VN all', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=VN&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'ProxyScrape VN http', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=VN&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'ProxyScrape VN socks4', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks4&timeout=10000&country=VN&ssl=all&anonymity=all', 'proto': 'socks4'},
    {'name': 'ProxyScrape VN socks5', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=10000&country=VN&ssl=all&anonymity=all', 'proto': 'socks5'},
    {'name': 'ProxyScrape V3 VN all', 'url': 'https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&country=VN&timeout=10000&protocol=all', 'proto': 'http'},
    {'name': 'Geonode VN', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&country=VN', 'proto': 'http'},
    {'name': 'ProxyList.download VN http', 'url': 'https://www.proxy-list.download/api/v1/get?type=http&country=VN', 'proto': 'http'},
    {'name': 'ProxyList.download VN socks4', 'url': 'https://www.proxy-list.download/api/v1/get?type=socks4&country=VN', 'proto': 'socks4'},
    {'name': 'ProxyList.download VN socks5', 'url': 'https://www.proxy-list.download/api/v1/get?type=socks5&country=VN', 'proto': 'socks5'},
    {'name': 'PubProxy VN', 'url': 'http://pubproxy.com/api/proxy?country=VN&limit=20&format=txt', 'proto': 'http'},
    {'name': 'CoolProxy VN', 'url': 'https://cool-proxy.net/proxies.json?country=VN', 'proto': 'http'},
    {'name': 'Spys.one VN', 'url': 'https://spys.one/free-proxy-list/VN/', 'proto': 'http'},
    {'name': 'HideMyName VN', 'url': 'https://hidemy.name/en/proxy-list/?country=VN', 'proto': 'http'},
    {'name': 'ProxyNova VN', 'url': 'https://www.proxynova.com/proxy-server-list/country-vn/', 'proto': 'http'},
    {'name': 'FreeProxy World VN', 'url': 'https://www.freeproxy.world/?country=VN&type=http&page=1', 'proto': 'http'},
    {'name': 'TheSpeedX HTTP', 'url': 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt', 'proto': 'http'},
    {'name': 'TheSpeedX SOCKS4', 'url': 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt', 'proto': 'socks4'},
    {'name': 'TheSpeedX SOCKS5', 'url': 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt', 'proto': 'socks5'},
    {'name': 'monosans HTTP', 'url': 'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt', 'proto': 'http'},
    {'name': 'monosans SOCKS4', 'url': 'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt', 'proto': 'socks4'},
    {'name': 'monosans SOCKS5', 'url': 'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt', 'proto': 'socks5'},
    {'name': 'monosans ALL', 'url': 'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/all.txt', 'proto': 'http'},
    {'name': 'jetkai HTTP', 'url': 'https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt', 'proto': 'http'},
    {'name': 'jetkai HTTPS', 'url': 'https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt', 'proto': 'http'},
    {'name': 'jetkai SOCKS4', 'url': 'https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt', 'proto': 'socks4'},
    {'name': 'jetkai SOCKS5', 'url': 'https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt', 'proto': 'socks5'},
    {'name': 'ShiftyTR HTTP', 'url': 'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt', 'proto': 'http'},
    {'name': 'ShiftyTR HTTPS', 'url': 'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt', 'proto': 'http'},
    {'name': 'ShiftyTR SOCKS4', 'url': 'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt', 'proto': 'socks4'},
    {'name': 'ShiftyTR SOCKS5', 'url': 'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt', 'proto': 'socks5'},
    {'name': 'clarketm', 'url': 'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt', 'proto': 'http'},
    {'name': 'hookzof SOCKS5', 'url': 'https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt', 'proto': 'socks5'},
    {'name': 'sunny9577', 'url': 'https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt', 'proto': 'http'},
    {'name': 'roosterkid HTTPS', 'url': 'https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS.txt', 'proto': 'http'},
    {'name': 'roosterkid SOCKS5', 'url': 'https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5.txt', 'proto': 'socks5'},
    {'name': 'proxifly ALL', 'url': 'https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.txt', 'proto': 'http'},
    {'name': 'proxifly HTTP', 'url': 'https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt', 'proto': 'http'},
    {'name': 'proxifly SOCKS4', 'url': 'https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks4/data.txt', 'proto': 'socks4'},
    {'name': 'proxifly SOCKS5', 'url': 'https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt', 'proto': 'socks5'},
    {'name': 'Anonym0usWork HTTP', 'url': 'https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/http_proxies.txt', 'proto': 'http'},
    {'name': 'Anonym0usWork SOCKS4', 'url': 'https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks4_proxies.txt', 'proto': 'socks4'},
    {'name': 'Anonym0usWork SOCKS5', 'url': 'https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks5_proxies.txt', 'proto': 'socks5'},
    {'name': 'VPSLab http_all', 'url': 'https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_all.txt', 'proto': 'http'},
    {'name': 'VPSLab http_ssl', 'url': 'https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_ssl.txt', 'proto': 'http'},
    {'name': 'VPSLab socks4', 'url': 'https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks4_all.txt', 'proto': 'socks4'},
    {'name': 'VPSLab socks5', 'url': 'https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks5_all.txt', 'proto': 'socks5'},
    {'name': 'mzyui ALL', 'url': 'https://raw.githubusercontent.com/mzyui/proxy-list/main/all.txt', 'proto': 'http'},
    {'name': 'mzyui HTTP', 'url': 'https://raw.githubusercontent.com/mzyui/proxy-list/main/http.txt', 'proto': 'http'},
    {'name': 'mzyui SOCKS4', 'url': 'https://raw.githubusercontent.com/mzyui/proxy-list/main/socks4.txt', 'proto': 'socks4'},
    {'name': 'mzyui SOCKS5', 'url': 'https://raw.githubusercontent.com/mzyui/proxy-list/main/socks5.txt', 'proto': 'socks5'},
    {'name': 'Thordata ALL', 'url': 'https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/all.txt', 'proto': 'http'},
    {'name': 'Thordata HTTP', 'url': 'https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/http.txt', 'proto': 'http'},
    {'name': 'Thordata SOCKS4', 'url': 'https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/socks4.txt', 'proto': 'socks4'},
    {'name': 'Thordata SOCKS5', 'url': 'https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/socks5.txt', 'proto': 'socks5'},
    {'name': 'ProxyScraper http', 'url': 'https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/http.txt', 'proto': 'http'},
    {'name': 'ProxyScraper sock4', 'url': 'https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/sock4.txt', 'proto': 'socks4'},
    {'name': 'ProxyScraper sock5', 'url': 'https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/sock5.txt', 'proto': 'socks5'},
    {'name': 'proxyscrape free-proxy-list ALL', 'url': 'https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/all/data.txt', 'proto': 'http'},
    {'name': 'proxyscrape free-proxy-list HTTP', 'url': 'https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/protocols/http/data.txt', 'proto': 'http'},
    {'name': 'proxyscrape free-proxy-list SOCKS4', 'url': 'https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/protocols/socks4/data.txt', 'proto': 'socks4'},
    {'name': 'proxyscrape free-proxy-list SOCKS5', 'url': 'https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/protocols/socks5/data.txt', 'proto': 'socks5'},
    {'name': 'hproxy ALL', 'url': 'https://raw.githubusercontent.com/hproxy-com/free-proxy-list/main/all.txt', 'proto': 'http'},
    {'name': 'openproxylist HTTP', 'url': 'https://openproxylist.xyz/http.txt', 'proto': 'http'},
    {'name': 'openproxylist SOCKS4', 'url': 'https://openproxylist.xyz/socks4.txt', 'proto': 'socks4'},
    {'name': 'openproxylist SOCKS5', 'url': 'https://openproxylist.xyz/socks5.txt', 'proto': 'socks5'},
    {'name': 'iplocate HTTP', 'url': 'https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/http.txt', 'proto': 'http'},
    {'name': 'iplocate SOCKS4', 'url': 'https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/socks4.txt', 'proto': 'socks4'},
    {'name': 'iplocate SOCKS5', 'url': 'https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/socks5.txt', 'proto': 'socks5'},
    {'name': 'fyvri HTTP', 'url': 'https://raw.githubusercontent.com/fyvri/fresh-proxy-list/main/http.txt', 'proto': 'http'},
    {'name': 'fyvri SOCKS4', 'url': 'https://raw.githubusercontent.com/fyvri/fresh-proxy-list/main/socks4.txt', 'proto': 'socks4'},
    {'name': 'fyvri SOCKS5', 'url': 'https://raw.githubusercontent.com/fyvri/fresh-proxy-list/main/socks5.txt', 'proto': 'socks5'},
    {'name': 'vmheaven HTTP', 'url': 'https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/main/http.txt', 'proto': 'http'},
    {'name': 'vmheaven SOCKS4', 'url': 'https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/main/socks4.txt', 'proto': 'socks4'},
    {'name': 'vmheaven SOCKS5', 'url': 'https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/main/socks5.txt', 'proto': 'socks5'},
    {'name': 'databay ALL', 'url': 'https://cdn.jsdelivr.net/gh/databay-labs/free-proxy-list@main/proxies/all.txt', 'proto': 'http'},
    {'name': 'ProxyScrape v2 ALL', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=all&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'ProxyScrape v2 HTTP', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'ProxyScrape v2 SOCKS4', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks4&timeout=10000&country=all&ssl=all&anonymity=all', 'proto': 'socks4'},
    {'name': 'ProxyScrape v2 SOCKS5', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=10000&country=all&ssl=all&anonymity=all', 'proto': 'socks5'},
    {'name': 'ProxyScrape v3 ALL', 'url': 'https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&timeout=10000&protocol=all', 'proto': 'http'},
    {'name': 'ProxyScrape v4', 'url': 'https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=ipport&format=text', 'proto': 'http'},
    {'name': 'Geonode global HTTP', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps', 'proto': 'http'},
    {'name': 'Geonode global SOCKS', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=socks4%2Csocks5', 'proto': 'socks5'},
    {'name': 'ProxyList.download HTTP', 'url': 'https://www.proxy-list.download/api/v1/get?type=http', 'proto': 'http'},
    {'name': 'ProxyList.download HTTPS', 'url': 'https://www.proxy-list.download/api/v1/get?type=https', 'proto': 'http'},
    {'name': 'ProxyList.download SOCKS4', 'url': 'https://www.proxy-list.download/api/v1/get?type=socks4', 'proto': 'socks4'},
    {'name': 'ProxyList.download SOCKS5', 'url': 'https://www.proxy-list.download/api/v1/get?type=socks5', 'proto': 'socks5'},
    {'name': 'PubProxy random', 'url': 'http://pubproxy.com/api/proxy?limit=20&format=txt', 'proto': 'http'},
    {'name': 'HProxy', 'url': 'https://hproxy.com/api/proxy-list?format=txt', 'proto': 'http'},
    {'name': 'Databay', 'url': 'https://databay.com/api/v1/proxy-list?format=txt&limit=1000', 'proto': 'http'},
    {'name': 'CoolProxy global', 'url': 'https://cool-proxy.net/proxies.json', 'proto': 'http'},
    {'name': 'free-proxy-list.net', 'url': 'https://free-proxy-list.net/', 'proto': 'http'},
    {'name': 'sslproxies.org', 'url': 'https://www.sslproxies.org/', 'proto': 'http'},
    {'name': 'us-proxy.org', 'url': 'https://www.us-proxy.org/', 'proto': 'http'},
    {'name': 'socks-proxy.net', 'url': 'https://www.socks-proxy.net/', 'proto': 'http'},
    {'name': 'spys.one HTTP', 'url': 'https://spys.one/en/http-proxy-list/', 'proto': 'http'},
    {'name': 'spys.one SOCKS', 'url': 'https://spys.one/en/socks-proxy-list/', 'proto': 'http'},
    {'name': 'hidemy.name HTTP', 'url': 'https://hidemy.name/en/proxy-list/?type=hs', 'proto': 'http'},
    {'name': 'hidemy.name SOCKS', 'url': 'https://hidemy.name/en/proxy-list/?type=45', 'proto': 'http'},
    {'name': 'proxynova global', 'url': 'https://www.proxynova.com/proxy-server-list/', 'proto': 'http'},
    {'name': 'proxylistplus HTTP1', 'url': 'https://list.proxylistplus.com/Fresh-HTTP-Proxy-List-1', 'proto': 'http'},
    {'name': 'proxylistplus HTTP2', 'url': 'https://list.proxylistplus.com/Fresh-HTTP-Proxy-List-2', 'proto': 'http'},
    {'name': 'proxylistplus SSL', 'url': 'https://list.proxylistplus.com/SSL-List-1', 'proto': 'http'},
    {'name': 'proxylistplus SOCKS', 'url': 'https://list.proxylistplus.com/SOCKS-List-1', 'proto': 'http'},
    {'name': 'free-proxy.cz', 'url': 'http://free-proxy.cz/en/proxylist/country/all/http/ping/all', 'proto': 'http'},
    {'name': 'proxydb', 'url': 'https://proxydb.net/?protocol=http&protocol=https&protocol=socks4&protocol=socks5', 'proto': 'socks5'},
    {'name': 'ProxyScrape US', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=US&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode US', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=US', 'proto': 'http'},
    {'name': 'ProxyScrape CN', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=CN&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode CN', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=CN', 'proto': 'http'},
    {'name': 'ProxyScrape ID', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=ID&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode ID', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=ID', 'proto': 'http'},
    {'name': 'ProxyScrape BR', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=BR&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode BR', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=BR', 'proto': 'http'},
    {'name': 'ProxyScrape IN', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=IN&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode IN', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=IN', 'proto': 'http'},
    {'name': 'ProxyScrape RU', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=RU&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode RU', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=RU', 'proto': 'http'},
    {'name': 'ProxyScrape DE', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=DE&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode DE', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=DE', 'proto': 'http'},
    {'name': 'ProxyScrape FR', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=FR&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode FR', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=FR', 'proto': 'http'},
    {'name': 'ProxyScrape GB', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=GB&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode GB', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=GB', 'proto': 'http'},
    {'name': 'ProxyScrape JP', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=JP&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode JP', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=JP', 'proto': 'http'},
    {'name': 'ProxyScrape KR', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=KR&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode KR', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=KR', 'proto': 'http'},
    {'name': 'ProxyScrape TH', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=TH&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode TH', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=TH', 'proto': 'http'},
    {'name': 'ProxyScrape MY', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=MY&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode MY', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=MY', 'proto': 'http'},
    {'name': 'ProxyScrape SG', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=SG&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode SG', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=SG', 'proto': 'http'},
    {'name': 'ProxyScrape PH', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=PH&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode PH', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=PH', 'proto': 'http'},
    {'name': 'ProxyScrape BD', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=BD&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode BD', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=BD', 'proto': 'http'},
    {'name': 'ProxyScrape PK', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=PK&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode PK', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=PK', 'proto': 'http'},
    {'name': 'ProxyScrape TR', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=TR&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode TR', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=TR', 'proto': 'http'},
    {'name': 'ProxyScrape UA', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=UA&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode UA', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=UA', 'proto': 'http'},
    {'name': 'ProxyScrape MX', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=MX&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode MX', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=MX', 'proto': 'http'},
    {'name': 'ProxyScrape AR', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=AR&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode AR', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=AR', 'proto': 'http'},
    {'name': 'ProxyScrape CL', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=CL&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode CL', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=CL', 'proto': 'http'},
    {'name': 'ProxyScrape CO', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=CO&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode CO', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=CO', 'proto': 'http'},
    {'name': 'ProxyScrape ZA', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=ZA&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode ZA', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=ZA', 'proto': 'http'},
    {'name': 'ProxyScrape EG', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=EG&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode EG', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=EG', 'proto': 'http'},
    {'name': 'ProxyScrape NG', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=NG&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode NG', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=NG', 'proto': 'http'},
    {'name': 'ProxyScrape IT', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=IT&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode IT', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=IT', 'proto': 'http'},
    {'name': 'ProxyScrape ES', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=ES&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode ES', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=ES', 'proto': 'http'},
    {'name': 'ProxyScrape NL', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=NL&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode NL', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=NL', 'proto': 'http'},
    {'name': 'ProxyScrape PL', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=PL&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode PL', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=PL', 'proto': 'http'},
    {'name': 'ProxyScrape CA', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=CA&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode CA', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=CA', 'proto': 'http'},
    {'name': 'ProxyScrape AU', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=AU&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode AU', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=AU', 'proto': 'http'},
    {'name': 'ProxyScrape TW', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=TW&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'Geonode TW', 'url': 'https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country=TW', 'proto': 'http'},
    {'name': 'ProxyScrape VN', 'url': 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=VN&ssl=all&anonymity=all', 'proto': 'http'},
    {'name': 'ALIILAPRO HTTP', 'url': 'https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/http.txt', 'proto': 'http'},
    {'name': 'ALIILAPRO SOCKS4', 'url': 'https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks4.txt', 'proto': 'socks4'},
    {'name': 'ALIILAPRO SOCKS5', 'url': 'https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks5.txt', 'proto': 'socks5'},
    {'name': 'nguclb HTTP', 'url': 'https://raw.githubusercontent.com/nguclb/ProxyList/main/http.txt', 'proto': 'http'},
    {'name': 'nguclb SOCKS4', 'url': 'https://raw.githubusercontent.com/nguclb/ProxyList/main/socks4.txt', 'proto': 'socks4'},
    {'name': 'nguclb SOCKS5', 'url': 'https://raw.githubusercontent.com/nguclb/ProxyList/main/socks5.txt', 'proto': 'socks5'},
    {'name': 'elliottophellia HTTP', 'url': 'https://raw.githubusercontent.com/elliottophellia/proxy-list/main/http.txt', 'proto': 'http'},
    {'name': 'elliottophellia SOCKS4', 'url': 'https://raw.githubusercontent.com/elliottophellia/proxy-list/main/socks4.txt', 'proto': 'socks4'},
    {'name': 'elliottophellia SOCKS5', 'url': 'https://raw.githubusercontent.com/elliottophellia/proxy-list/main/socks5.txt', 'proto': 'socks5'},
    {'name': 'HyperBeast HTTP', 'url': 'https://raw.githubusercontent.com/HyperBeast/Proxy-List/main/http.txt', 'proto': 'http'},
    {'name': 'RK2002 HTTP', 'url': 'https://raw.githubusercontent.com/rk2002/Proxy-List/main/http.txt', 'proto': 'http'},
    {'name': 'RK2002 SOCKS4', 'url': 'https://raw.githubusercontent.com/rk2002/Proxy-List/main/socks4.txt', 'proto': 'socks4'},
    {'name': 'RK2002 SOCKS5', 'url': 'https://raw.githubusercontent.com/rk2002/Proxy-List/main/socks5.txt', 'proto': 'socks5'},
    {'name': 'Mertguvel SOCKS5', 'url': 'https://raw.githubusercontent.com/Mertguvel/Socks5-Proxy-List/main/socks5.txt', 'proto': 'socks5'},
    {'name': 'spys.me HTTP', 'url': 'https://spys.me/proxy.txt', 'proto': 'http'},
    {'name': 'multiproxy HTTP', 'url': 'https://multiproxy.org/txt_all/proxy.txt', 'proto': 'http'},
    {'name': 'proxy-spider HTTP', 'url': 'https://proxy-spider.com/api/proxies.txt', 'proto': 'http'},
]


def load_links(paths=("link.txt", "uploads/link.txt")):
    """Đọc danh sách link từ file. Trả list link chuẩn hoá."""
    for path in paths:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    links = [ln.strip() for ln in f if ln.strip()]
                links = [ln for ln in links if ln.startswith("http")]
                if links:
                    return links
            except Exception:
                pass
    return []


def extract_alias(url):
    """Trích alias từ link vuotnhanh: https://vuotnhanh.com/GL48 -> GL48"""
    m = re.search(r"vuotnhanh\.com/([A-Za-z0-9]+)", url)
    if m:
        return m.group(1)
    return url.rstrip("/").split("/")[-1]


# ==================== MINER ====================
class ProxyMiner:
    def __init__(self):
        self.lock = threading.Lock()
        self.all_proxies = []
        self.used_ips = set()
        self.sources = ALL_PROXY_SOURCES
        self.total_sources = len(self.sources)

    def _new_session(self):
        s = requests.Session()
        s.verify = False
        s.headers.update({"User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
        ])})
        return s

    # --- đào từng nguồn ---
    def source_worker(self, source, idx, total):
        try:
            session = self._new_session()
            resp = session.get(source["url"], timeout=25)
            if resp.status_code != 200:
                return
            text = resp.text.strip()
            proxies_raw = []
            ct = resp.headers.get("Content-Type", "")

            # JSON
            if "json" in ct or text[:1] in ("{", "["):
                try:
                    data = resp.json()
                    items = data.get("data", data) if isinstance(data, dict) else data
                    if isinstance(items, dict):
                        items = items.get("data", [])
                    for item in items if isinstance(items, list) else []:
                        if isinstance(item, dict):
                            ip = item.get("ip") or item.get("host") or item.get("ip_address")
                            port = item.get("port")
                            if ip and port:
                                proxies_raw.append(f"{ip}:{port}")
                        elif isinstance(item, str) and ":" in item:
                            proxies_raw.append(item.strip())
                except Exception:
                    proxies_raw = [l.strip() for l in text.splitlines() if ":" in l]
            else:
                proxies_raw = [l.strip() for l in text.splitlines() if ":" in l]

            # chuẩn hoá + lọc
            for p in proxies_raw:
                p = p.strip()
                if "://" in p:
                    p = p.split("://")[-1]
                p = re.split(r"[\s,]+", p)[0]
                if not re.match(r"^\d{1,3}(\.\d{1,3}){3}:\d{1,5}$", p):
                    continue
                ip, port = p.rsplit(":", 1)
                if not (0 < int(port) < 65536):
                    continue
                # ưu tiên proxy http/https khi trùng IP
                with self.lock:
                    if ip in self.used_ips:
                        continue
                    self.used_ips.add(ip)
                    self.all_proxies.append({
                        "proxy": p,
                        "ip": ip,
                        "proto": source["proto"],
                        "source": source["name"],
                    })
        except Exception:
            pass

        with self.lock:
            pct = idx / total * 100
            sys.stdout.write(f"\r{Grad.cold('ĐANG ĐÀO NGUỒN PROXY')} [{pct:5.1f}%] | Được: {len(self.all_proxies)}")
            sys.stdout.flush()

    def fetch_all(self):
        with self.lock:
            self.all_proxies = []
            self.used_ips = set()
        print(Grad.hot(f"🌟 ĐÀO {self.total_sources} NGUỒN PROXY WORLDWIDE"))
        with ThreadPoolExecutor(max_workers=min(60, self.total_sources)) as ex:
            fs = [ex.submit(self.source_worker, s, i, self.total_sources)
                  for i, s in enumerate(self.sources, 1)]
            for f in as_completed(fs):
                pass
        print()
        print(Grad.forest(f"✅ Đào được {len(self.all_proxies)} proxy"))
        return self.all_proxies

    # --- test proxy sống ---
    def test_worker(self, info, idx, total, results):
        scheme = {"http": "http", "https": "http", "socks4": "socks4", "socks5": "socks5"}.get(info["proto"], "http")
        proxies = {"http": f"{scheme}://{info['proxy']}", "https": f"{scheme}://{info['proxy']}"}
        try:
            start = time.time()
            r = requests.get("https://vuotnhanh.com/", proxies=proxies,
                             headers={"User-Agent": "Mozilla/5.0 (Linux; Android 12; SM-A125F) AppleWebKit/537.36"},
                             timeout=6, verify=False)
            if r.status_code == 200:
                with self.lock:
                    results.append({**info, "latency": round(time.time() - start, 2)})
        except Exception:
            pass
        with self.lock:
            pct = idx / total * 100
            sys.stdout.write(f"\r{Grad.gold('CHECK LẠI PROXY')} [{pct:5.1f}%] | Sống: {len(results)}")
            sys.stdout.flush()

    def filter_alive(self):
        if not self.all_proxies:
            return []
        print(Grad.cold(f"🔍 CHECK {len(self.all_proxies)} PROXY (1 PROXY = 1 LUỒNG)"))
        results = []
        total = len(self.all_proxies)
        completed_counter = [0]  # mutable int
        with ThreadPoolExecutor(max_workers=200) as ex:
            fs = [ex.submit(self.test_worker, p, completed_counter, total, results)
                  for i, p in enumerate(self.all_proxies, 1)]
            for f in as_completed(fs):
                pass
        print()
        results.sort(key=lambda x: x.get("latency", 99))
        if results:
            print(Grad.forest(f"✅ Còn {len(results)} proxy sống"))
        else:
            print(Grad.hot("❌ DIE MẸ TÀI NGUYÊN RỒI =((("))
        return results


# ==================== CLICKER (đa link) ====================
class Clicker:
    def __init__(self, links):
        self.links = links
        self.lock = threading.Lock()
        self.success_count = 0
        self.earned = 0
        self.used_ips = set()
        self.earned_per_link = {alias: 0 for alias in links}
        self.failed = 0

    def _session(self, info):
        scheme = {"http": "http", "https": "http", "socks4": "socks4", "socks5": "socks5"}.get(info["proto"], "http")
        s = requests.Session()
        s.verify = False
        s.proxies = {"http": f"{scheme}://{info['proxy']}", "https": f"{scheme}://{info['proxy']}"}
        s.headers.update({"User-Agent": random.choice([
            "Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36",
            "Mozilla/5.0 (Linux; Android 11; SM-M127G) AppleWebKit/537.36",
            "Mozilla/5.0 (Linux; Android 12; SM-A125F) AppleWebKit/537.36",
            "Mozilla/5.0 (Linux; Android 13; Pixel 6) AppleWebKit/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15",
        ])})
        return s

    def click_worker(self, info, idx, total, q):
        ip = info["ip"]
        with self.lock:
            if ip in self.used_ips:
                q.put({"ok": False, "ip": ip})
                return
            self.used_ips.add(ip)

        # round-robin: mỗi proxy click 1 link
        link = self.links[idx % len(self.links)]
        alias = extract_alias(link)

        for attempt in range(2):  # retry 1 lần cho proxy chậm/flake
            try:
                s = self._session(info)
                r = s.get(link, timeout=15)
                if r.status_code != 200:
                    break
                m = re.search(r'<meta[^>]*name=["\']csrf-token["\'][^>]*content=["\']([^"\']+)', r.text)
                if not m:
                    break
                csrf = m.group(1)
                ar = s.post("https://vuotnhanh.com/go/sf",
                            headers={"X-CSRF-TOKEN": csrf, "X-Requested-With": "XMLHttpRequest"},
                            data={"alias": alias}, timeout=10)
                if ar.status_code == 200:
                    try:
                        j = ar.json()
                        if j.get("status") == "success" or j.get("url_redirect"):
                            q.put({"ok": True, "ip": ip, "alias": alias})
                            return
                    except Exception:
                        pass
                break
            except Exception:
                time.sleep(0.3)
        q.put({"ok": False, "ip": ip})

    def run(self, proxies):
        if not proxies:
            print(Grad.hot("❌ Không có proxy để bắn!"))
            return
        print(Grad.hot(f"🚀 BẮN {len(proxies)} PROXY -> {len(self.links)} LINK (1 PROXY = 1 LUỒNG)"))
        q = queue.Queue()
        total = len(proxies)
        done = success = earned = 0
        with ThreadPoolExecutor(max_workers=min(200, total)) as ex:
            fs = [ex.submit(self.click_worker, p, i, total, q)
                  for i, p in enumerate(proxies, 1)]
            for f in as_completed(fs):
                done += 1
                try:
                    r = q.get(timeout=1)
                    if r.get("ok"):
                        success += 1
                        earned += 12
                        with self.lock:
                            self.success_count += 1
                            self.earned += 12
                            if r.get("alias"):
                                self.earned_per_link[r["alias"]] = self.earned_per_link.get(r["alias"], 0) + 12
                        print(Grad.party(f"\n[THÀNH CÔNG] [{r['ip']}] [{r.get('alias','')}] [+12đ]\n"))
                except queue.Empty:
                    pass
                pct = done / total * 100
                sys.stdout.write(f"\r{Grad.by_percent(pct)}[ZKAI] ĐANG BẮN [{pct:5.2f}%] | OK: {success}/{done} | {earned}đ")
                sys.stdout.flush()
        print()
        print(Grad.forest(f"📊 TỔNG: {success} clicks thành công, {earned}đ"))


# ==================== MAIN ====================
def banner():
    print(Grad.hot(r"""
  ____  __    ____  __  _____
 |__  |/ /   / __ \|  \/  _  |
   / /| ' /  / /_/ /| |\/| | |  -- ZKAI FARM DA LINK --
  / /_| . \  / ____/ | |  | | |     upgrade r2: link.txt
 /____/|_|\_\/_/      |_|  |_| |     +190 nguon proxy
"""))
    print(Grad.gold("=== r2.py - doc link.txt, farm nhieu link song song ==="))


def main():
    banner()
    links = load_links()
    if not links:
        print(Grad.hot("❌ Không thấy link.txt! Tạo file với mỗi link 1 dòng."))
        print(Grad.cold("Ví dụ:\nhttps://vuotnhanh.com/GL48\nhttps://vuotnhanh.com/DMxu"))
        return
    print(Grad.gold(f"📄 Nạp {len(links)} link từ link.txt"))
    for i, ln in enumerate(links, 1):
        print(f"   {i}. {ln}  (alias: {extract_alias(ln)})")

    miner = ProxyMiner()
    clicker = Clicker(links)

    cycle = 0
    while True:
        cycle += 1
        print(Grad.neon(f"\n═══════ CYCLE {cycle} ═══════"))
        allp = miner.fetch_all()
        if allp:
            alive = miner.filter_alive()
            if alive:
                print(Grad.neon("🏆 TOP 10 NHANH NHẤT:"))
                for i, p in enumerate(alive[:10], 1):
                    print(f"   {i}. {p['proxy']} [{p['proto']}] {p.get('latency',0)}s")
                clicker.run(alive)
                print(Grad.forest(f"📊 TẠM THỜI: {clicker.success_count} clicks, {clicker.earned}đ"))
                print(Grad.cold("📈 THEO LINK:"))
                for alias, d in clicker.earned_per_link.items():
                    print(f"   {alias}: {d}đ")
                print(Grad.neon("⏳ ĐỢI 10 GIÂY..."))
                time.sleep(10)
                continue
        print(Grad.hot("❌ Tài nguyên bay sạch. Đợi 30s..."))
        time.sleep(30)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(Grad.hot("\n👋 Dừng. Chạy lại là tiếp tục."))
