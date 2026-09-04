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
# proto: http/https -> http:// ; socks4 -> socks4:// ; socks5 -> socks5://
# Giới hạn mỗi nguồn + tổng để giữ tốc độ lọc sống

MAX_PER_SOURCE = 1500
MAX_TOTAL = 60000
MAX_FILTER = 15000  # giới hạn số proxy đem đi lọc sống mỗi cycle (cân bằng theo nguồn)

GITHUB_SOURCES = [
    # ---- tổng hợp lớn ----
    {"name": "zevtyardt ALL", "url": "https://proxylist.zevtyardt.com/get_all?format=text", "proto": "http"},
    {"name": "TheSpeedX HTTP", "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt", "proto": "http"},
    {"name": "TheSpeedX SOCKS4", "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt", "proto": "socks4"},
    {"name": "TheSpeedX SOCKS5", "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt", "proto": "socks5"},
    {"name": "monosans HTTP", "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt", "proto": "http"},
    {"name": "monosans SOCKS4", "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt", "proto": "socks4"},
    {"name": "monosans SOCKS5", "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt", "proto": "socks5"},
    {"name": "monosans ALL", "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/all.txt", "proto": "http"},
    {"name": "jetkai HTTP", "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt", "proto": "http"},
    {"name": "jetkai HTTPS", "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt", "proto": "http"},
    {"name": "jetkai SOCKS4", "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt", "proto": "socks4"},
    {"name": "jetkai SOCKS5", "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt", "proto": "socks5"},
    {"name": "clarketm", "url": "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt", "proto": "http"},
    {"name": "roosterkid ALL", "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/ALL.txt", "proto": "http"},
    {"name": "roosterkid HTTPS", "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS.txt", "proto": "http"},
    {"name": "roosterkid SOCKS4", "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4.txt", "proto": "socks4"},
    {"name": "roosterkid SOCKS5", "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5.txt", "proto": "socks5"},
    {"name": "proxifly ALL", "url": "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.txt", "proto": "http"},
    {"name": "proxifly HTTP", "url": "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt", "proto": "http"},
    {"name": "proxifly SOCKS4", "url": "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks4/data.txt", "proto": "socks4"},
    {"name": "proxifly SOCKS5", "url": "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt", "proto": "socks5"},
    {"name": "mzyui ALL", "url": "https://raw.githubusercontent.com/mzyui/proxy-list/main/all.txt", "proto": "http"},
    {"name": "mzyui HTTP", "url": "https://raw.githubusercontent.com/mzyui/proxy-list/main/http.txt", "proto": "http"},
    {"name": "mzyui SOCKS4", "url": "https://raw.githubusercontent.com/mzyui/proxy-list/main/socks4.txt", "proto": "socks4"},
    {"name": "mzyui SOCKS5", "url": "https://raw.githubusercontent.com/mzyui/proxy-list/main/socks5.txt", "proto": "socks5"},
    {"name": "Thordata ALL", "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/all.txt", "proto": "http"},
    {"name": "Thordata HTTP", "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/http.txt", "proto": "http"},
    {"name": "Thordata SOCKS4", "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/socks4.txt", "proto": "socks4"},
    {"name": "Thordata SOCKS5", "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/socks5.txt", "proto": "socks5"},
    {"name": "zevtyardt HTTP", "url": "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/http.txt", "proto": "http"},
    {"name": "zevtyardt SOCKS4", "url": "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks4.txt", "proto": "socks4"},
    {"name": "zevtyardt SOCKS5", "url": "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks5.txt", "proto": "socks5"},
    # ---- trong r.txt còn thiếu ----
    {"name": "hookzof SOCKS5", "url": "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt", "proto": "socks5"},
    {"name": "Anonym0usWork HTTP", "url": "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/http_proxies.txt", "proto": "http"},
    {"name": "Anonym0usWork SOCKS4", "url": "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks4_proxies.txt", "proto": "socks4"},
    {"name": "Anonym0usWork SOCKS5", "url": "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks5_proxies.txt", "proto": "socks5"},
    {"name": "VPSLab http_all", "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_all.txt", "proto": "http"},
    {"name": "VPSLab http_ssl", "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_ssl.txt", "proto": "http"},
    {"name": "VPSLab socks4", "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks4_all.txt", "proto": "socks4"},
    {"name": "VPSLab socks5", "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks5_all.txt", "proto": "socks5"},
    {"name": "ProxyScraper http", "url": "https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/http.txt", "proto": "http"},
    {"name": "ProxyScraper sock4", "url": "https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/sock4.txt", "proto": "socks4"},
    {"name": "ProxyScraper sock5", "url": "https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/sock5.txt", "proto": "socks5"},
    {"name": "pscrape-free HTTP", "url": "https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/protocols/http/data.txt", "proto": "http"},
    {"name": "pscrape-free SOCKS4", "url": "https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/protocols/socks4/data.txt", "proto": "socks4"},
    {"name": "pscrape-free SOCKS5", "url": "https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/protocols/socks5/data.txt", "proto": "socks5"},
    {"name": "pscrape-free ALL", "url": "https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/all/data.txt", "proto": "http"},
    {"name": "hproxy ALL", "url": "https://raw.githubusercontent.com/hproxy-com/free-proxy-list/main/all.txt", "proto": "http"},
    {"name": "openproxylist HTTP", "url": "https://openproxylist.xyz/http.txt", "proto": "http"},
    {"name": "openproxylist SOCKS4", "url": "https://openproxylist.xyz/socks4.txt", "proto": "socks4"},
    {"name": "openproxylist SOCKS5", "url": "https://openproxylist.xyz/socks5.txt", "proto": "socks5"},
    {"name": "iplocate HTTP", "url": "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/http.txt", "proto": "http"},
    {"name": "iplocate SOCKS4", "url": "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/socks4.txt", "proto": "socks4"},
    {"name": "iplocate SOCKS5", "url": "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/socks5.txt", "proto": "socks5"},
    {"name": "ShiftyTR HTTP", "url": "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt", "proto": "http"},
    {"name": "ShiftyTR HTTPS", "url": "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt", "proto": "http"},
    {"name": "ShiftyTR SOCKS4", "url": "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt", "proto": "socks4"},
    {"name": "ShiftyTR SOCKS5", "url": "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt", "proto": "socks5"},
    {"name": "fyvri HTTP", "url": "https://raw.githubusercontent.com/fyvri/fresh-proxy-list/main/http.txt", "proto": "http"},
    {"name": "fyvri SOCKS4", "url": "https://raw.githubusercontent.com/fyvri/fresh-proxy-list/main/socks4.txt", "proto": "socks4"},
    {"name": "fyvri SOCKS5", "url": "https://raw.githubusercontent.com/fyvri/fresh-proxy-list/main/socks5.txt", "proto": "socks5"},
    {"name": "vmheaven HTTP", "url": "https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/main/http.txt", "proto": "http"},
    {"name": "vmheaven SOCKS4", "url": "https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/main/socks4.txt", "proto": "socks4"},
    {"name": "vmheaven SOCKS5", "url": "https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/main/socks5.txt", "proto": "socks5"},
    {"name": "databay ALL", "url": "https://cdn.jsdelivr.net/gh/databay-labs/free-proxy-list@main/proxies/all.txt", "proto": "http"},
    {"name": "sunny9577", "url": "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt", "proto": "http"},
    {"name": "KangProxy HTTP", "url": "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/http/http.txt", "proto": "http"},
    {"name": "KangProxy SOCKS4", "url": "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/socks4/socks4.txt", "proto": "socks4"},
    {"name": "KangProxy SOCKS5", "url": "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/socks5/socks5.txt", "proto": "socks5"},
    # ---- nguồn thêm mới ----
    {"name": "zloi-user HTTP", "url": "https://raw.githubusercontent.com/zloi-user/hideip.me/main/http.txt", "proto": "http"},
    {"name": "zloi-user HTTPS", "url": "https://raw.githubusercontent.com/zloi-user/hideip.me/main/https.txt", "proto": "http"},
    {"name": "zloi-user SOCKS4", "url": "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks4.txt", "proto": "socks4"},
    {"name": "zloi-user SOCKS5", "url": "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks5.txt", "proto": "socks5"},
    {"name": "mmpx12 HTTP", "url": "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt", "proto": "http"},
    {"name": "mmpx12 HTTPS", "url": "https://raw.githubusercontent.com/mmpx12/proxy-list/master/https.txt", "proto": "http"},
    {"name": "mmpx12 SOCKS4", "url": "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks4.txt", "proto": "socks4"},
    {"name": "mmpx12 SOCKS5", "url": "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt", "proto": "socks5"},
    {"name": "UptimerBot HTTP", "url": "https://raw.githubusercontent.com/UptimerBot/proxy-list/main/proxies/http.txt", "proto": "http"},
    {"name": "AshiqAmir HTTP", "url": "https://raw.githubusercontent.com/AshiqAmir/Proxy-List/main/http.txt", "proto": "http"},
    {"name": "saisuiu HTTP", "url": "https://raw.githubusercontent.com/saisuiu/uiu/master/http.txt", "proto": "http"},
    {"name": "ErcinDedeoglu", "url": "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies.txt", "proto": "http"},
    {"name": "proxy4parsing", "url": "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/proxies.txt", "proto": "http"},
    {"name": "prxchk HTTP", "url": "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt", "proto": "http"},
    {"name": "yuceltoluyag", "url": "https://raw.githubusercontent.com/yuceltoluyag/GoodProxy/main/proxies.txt", "proto": "http"},
    {"name": "B4RC0DE HTTP", "url": "https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/HTTP.txt", "proto": "http"},
    {"name": "Lux-AiR HTTP", "url": "https://raw.githubusercontent.com/Lux-AiR/ProxyList/main/http.txt", "proto": "http"},
    {"name": "topicality", "url": "https://raw.githubusercontent.com/topicality/proxy-list/main/proxy-list/data.txt", "proto": "http"},
    {"name": "mahmoudgalal2", "url": "https://raw.githubusercontent.com/mahmoudgalal2/proxy-list/master/proxylist.txt", "proto": "http"},
    {"name": "rwvch HTTP", "url": "https://raw.githubusercontent.com/rwvch/Proxy/master/proxy.txt", "proto": "http"},
    {"name": "webhooksite HTTP", "url": "https://raw.githubusercontent.com/webhooksite/proxy-list/main/http.txt", "proto": "http"},
    {"name": "rickwang888 HTTP", "url": "https://raw.githubusercontent.com/rickwang888/proxylist/main/http.txt", "proto": "http"},
    {"name": "Staawik HTTP", "url": "https://raw.githubusercontent.com/Staawik/ProxyList/main/http.txt", "proto": "http"},
]

API_SOURCES = [
    {"name": "ProxyScrape v2 ALL", "url": "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=all&ssl=all&anonymity=all", "proto": "http"},
    {"name": "ProxyScrape v2 HTTP", "url": "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all", "proto": "http"},
    {"name": "ProxyScrape v2 SOCKS4", "url": "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks4&timeout=10000&country=all&ssl=all&anonymity=all", "proto": "socks4"},
    {"name": "ProxyScrape v2 SOCKS5", "url": "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=10000&country=all&ssl=all&anonymity=all", "proto": "socks5"},
    {"name": "ProxyScrape v2 HTTP elite", "url": "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=yes&anonymity=elite", "proto": "http"},
    {"name": "ProxyScrape v2 getproxies", "url": "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all", "proto": "http"},
    {"name": "ProxyScrape v3 ALL", "url": "https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&timeout=10000&protocol=all", "proto": "http"},
    {"name": "ProxyScrape v3 HTTP", "url": "https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&timeout=10000&protocol=http", "proto": "http"},
    {"name": "ProxyScrape v3 SOCKS4", "url": "https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&timeout=10000&protocol=socks4", "proto": "socks4"},
    {"name": "ProxyScrape v3 SOCKS5", "url": "https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&timeout=10000&protocol=socks5", "proto": "socks5"},
    {"name": "ProxyScrape v4", "url": "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=ipport&format=text", "proto": "http"},
    {"name": "Geonode HTTP p1", "url": "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps", "proto": "http"},
    {"name": "Geonode SOCKS p1", "url": "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=socks4%2Csocks5", "proto": "socks5"},
    {"name": "Geonode HTTP p2", "url": "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps", "proto": "http"},
    {"name": "Geonode HTTP p3", "url": "https://proxylist.geonode.com/api/proxy-list?limit=500&page=3&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps", "proto": "http"},
    {"name": "ProxyList.download HTTP", "url": "https://www.proxy-list.download/api/v1/get?type=http", "proto": "http"},
    {"name": "ProxyList.download HTTPS", "url": "https://www.proxy-list.download/api/v1/get?type=https", "proto": "http"},
    {"name": "ProxyList.download SOCKS4", "url": "https://www.proxy-list.download/api/v1/get?type=socks4", "proto": "socks4"},
    {"name": "ProxyList.download SOCKS5", "url": "https://www.proxy-list.download/api/v1/get?type=socks5", "proto": "socks5"},
    {"name": "ProxyList.download v2 hs", "url": "https://www.proxy-list.download/api/v2/get?l=en&t=hs", "proto": "http"},
    {"name": "HProxy", "url": "https://hproxy.com/api/proxy-list?format=txt", "proto": "http"},
    {"name": "CoolProxy", "url": "https://cool-proxy.net/proxies.json", "proto": "http"},
    {"name": "PubProxy random", "url": "http://pubproxy.com/api/proxy?limit=20&format=txt", "proto": "http"},
    {"name": "Databay API", "url": "https://databay.com/api/v1/proxy-list?format=txt&limit=1000", "proto": "http"},
]

HTML_SOURCES = [
    {"name": "free-proxy-list.net", "url": "https://free-proxy-list.net/", "proto": "http"},
    {"name": "sslproxies.org", "url": "https://www.sslproxies.org/", "proto": "http"},
    {"name": "us-proxy.org", "url": "https://www.us-proxy.org/", "proto": "http"},
    {"name": "socks-proxy.net", "url": "https://www.socks-proxy.net/", "proto": "socks5"},
    {"name": "spys.one HTTP", "url": "https://spys.one/en/http-proxy-list/", "proto": "http"},
    {"name": "spys.one SOCKS", "url": "https://spys.one/en/socks-proxy-list/", "proto": "socks5"},
    {"name": "hidemy.name HTTP", "url": "https://hidemy.name/en/proxy-list/?type=hs", "proto": "http"},
    {"name": "hidemy.name SOCKS", "url": "https://hidemy.name/en/proxy-list/?type=45", "proto": "socks5"},
    {"name": "proxynova", "url": "https://www.proxynova.com/proxy-server-list/", "proto": "http"},
    {"name": "proxylistplus HTTP1", "url": "https://list.proxylistplus.com/Fresh-HTTP-Proxy-List-1", "proto": "http"},
    {"name": "proxylistplus HTTP2", "url": "https://list.proxylistplus.com/Fresh-HTTP-Proxy-List-2", "proto": "http"},
    {"name": "proxylistplus HTTP3", "url": "https://list.proxylistplus.com/Fresh-HTTP-Proxy-List-3", "proto": "http"},
    {"name": "proxylistplus SSL", "url": "https://list.proxylistplus.com/SSL-List-1", "proto": "http"},
    {"name": "proxylistplus SOCKS", "url": "https://list.proxylistplus.com/SOCKS-List-1", "proto": "socks5"},
    {"name": "free-proxy.cz", "url": "http://free-proxy.cz/en/proxylist/country/all/http/ping/all", "proto": "http"},
    {"name": "proxydb", "url": "https://proxydb.net/?protocol=http&protocol=https&protocol=socks4&protocol=socks5", "proto": "http"},
    # ---- nguồn VN trong r.txt ----
    {"name": "VN ProxyNova", "url": "https://www.proxynova.com/proxy-server-list/country-vn/", "proto": "http"},
    {"name": "VN Spys", "url": "https://spys.one/free-proxy-list/VN/", "proto": "http"},
    {"name": "VN HideMyName", "url": "https://hidemy.name/en/proxy-list/?country=VN", "proto": "http"},
    {"name": "VN FreeProxy", "url": "https://www.freeproxy.world/?country=VN&type=http&page=1", "proto": "http"},
    {"name": "VN ProxyScrape", "url": "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country=VN&ssl=all&anonymity=all", "proto": "http"},
    {"name": "VN Geonode", "url": "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&country=VN", "proto": "http"},
    {"name": "VN ProxyList.download", "url": "https://www.proxy-list.download/api/v1/get?type=http&country=VN", "proto": "http"},
]

# ---- theo quốc gia (ProxyScrape + Geonode cho 34 nước) ----
_COUNTRIES = ["US", "CN", "ID", "BR", "IN", "RU", "DE", "FR", "GB", "JP", "KR",
              "TH", "MY", "SG", "PH", "BD", "PK", "TR", "UA", "MX", "AR", "CL",
              "CO", "ZA", "EG", "NG", "IT", "ES", "NL", "PL", "CA", "AU", "TW", "VN"]
COUNTRY_SOURCES = []
for _c in _COUNTRIES:
    COUNTRY_SOURCES.append({"name": f"ProxyScrape {_c}", "url": f"https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=10000&country={_c}&ssl=all&anonymity=all", "proto": "http"})
    COUNTRY_SOURCES.append({"name": f"Geonode {_c}", "url": f"https://proxylist.geonode.com/api/proxy-list?limit=300&page=1&sort_by=lastChecked&sort_type=desc&country={_c}", "proto": "http"})



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
        self.sources = GITHUB_SOURCES + API_SOURCES + HTML_SOURCES + COUNTRY_SOURCES
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

            # chuẩn hoá + lọc (giới hạn mỗi nguồn + tổng để giữ tốc độ)
            added = 0
            for p in proxies_raw:
                if added >= MAX_PER_SOURCE:
                    break
                p = p.strip()
                if "://" in p:
                    p = p.split("://")[-1]
                p = re.split(r"[\s,]+", p)[0]
                if not re.match(r"^\d{1,3}(\.\d{1,3}){3}:\d{1,5}$", p):
                    continue
                ip, port = p.rsplit(":", 1)
                if not (0 < int(port) < 65536):
                    continue
                with self.lock:
                    if len(self.all_proxies) >= MAX_TOTAL:
                        break
                    if ip in self.used_ips:
                        continue
                    self.used_ips.add(ip)
                    self.all_proxies.append({
                        "proxy": p,
                        "ip": ip,
                        "proto": source["proto"],
                        "source": source["name"],
                    })
                    added += 1
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
        with ThreadPoolExecutor(max_workers=min(120, self.total_sources)) as ex:
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
        pool = self.all_proxies
        # lấy mẫu cân bằng theo nguồn nếu quá nhiều -> vẫn đa dạng, lọc nhanh
        if len(pool) > MAX_FILTER:
            by_src = {}
            for p in pool:
                by_src.setdefault(p["source"], []).append(p)
            per = max(1, MAX_FILTER // len(by_src))
            pool = []
            for src, items in by_src.items():
                pool.extend(items[:per])
            random.shuffle(pool)
            pool = pool[:MAX_FILTER]
            print(Grad.gold(f"⚖️ Lấy mẫu {len(pool)} proxy cân bằng từ {len(by_src)} nguồn để lọc"))
        print(Grad.cold(f"🔍 CHECK {len(pool)} PROXY (1 PROXY = 1 LUỒNG)"))
        results = []
        total = len(pool)
        with ThreadPoolExecutor(max_workers=300) as ex:
            fs = [ex.submit(self.test_worker, p, i, total, results)
                  for i, p in enumerate(pool, 1)]
            fs = [ex.submit(self.test_worker, p, i, total, results)
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
 /____/|_|\_\/_/      |_|  |_| |     +130 nguon proxy
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
