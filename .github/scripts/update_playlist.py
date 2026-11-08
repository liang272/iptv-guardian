# -*- coding: utf-8 -*-
"""GitHub Actions 零运维方案：只替换失效频道的地址，固定 URL 永远不变。

运行逻辑：
  state.json（已提交到仓库）保存每个频道的当前地址 + 备用地址池 + 质量统计；
  每次运行复检所有频道（最短周期由 workflow 的 cron 控制）；
  某个频道失效时，对备用池并发探测并按多维评分挑「最优」地址单独替换，且换后即时复验；
  空闲时按轮转抽样对健康频道做「择优升级」，逐步把频道切到加载更快、更稳的源；
  最后重新生成 docs/live.m3u —— 文件名/URL 不变，播放器永远订阅同一个地址。

候选池来源（全部需为已获合法授权或版权方主动公开的源）：
  sources.json 里可配任意多个上游，支持 enabled / weight / merge_only；
  sources.d/ 目录下的 .m3u / .txt 会被自动扫描并入（放置你自己有权使用的源）；
  merge_only 的上游只补充已有频道的备用地址，不新增频道（用于大型聚合列表）。

探测结果分三态（重要）：
  ok=True   可用（已通过「播放列表有效 + 首分片有数据」的真实播放链路验证）
  ok=False  明确失效（HTTP 4xx/5xx、返回的不是播放列表、分片报错等）
  ok=None   不确定：连接超时/拒绝/DNS 失败。GitHub Actions 节点在海外，
            访问国内运营商内网源普遍不通，这不等于用户端失效，
            因此不判定失效、不换源、不降权、不删除频道。

选源评分（越高越优）：加载延迟、首字节、首分片耗时、画质、历史成功率、
连续失败、不确定次数、最近成功时间、上游权重。换源时只从通过验证的源里选最高分。

输出：
  docs/live.m3u     全部频道，确认失效的排到末尾并标记「待修复」（主订阅地址，URL 不变）
  docs/healthy.m3u  只含未确认失效的频道（纯净版，可选订阅）
  docs/status.json  巡检统计、质量概览与换源日志

为避免高频周期产生大量无意义提交：
  只有「新增频道 / 备用池变化 / 换源 / 健康状态翻转 / 台名映射变化」才重写播放列表；
  质量统计与状态看板按 STATUS_INTERVAL 节流落盘（默认 30 分钟）。

仅使用 Python 标准库，无需安装依赖。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SOURCES = os.path.join(ROOT, 'sources.json')
SOURCES_DIR = os.path.join(ROOT, 'sources.d')
STATE = os.path.join(ROOT, 'state.json')
OUT_M3U = os.path.join(ROOT, 'docs', 'live.m3u')
OUT_HEALTHY = os.path.join(ROOT, 'docs', 'healthy.m3u')
OUT_STATUS = os.path.join(ROOT, 'docs', 'status.json')
NAMES = os.path.join(ROOT, 'names_zh.json')

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')
TIMEOUT = float(os.environ.get('PROBE_TIMEOUT', '5'))
FETCH_TIMEOUT = float(os.environ.get('FETCH_TIMEOUT', '10'))
WORKERS = int(os.environ.get('PROBE_WORKERS', '24'))
STALE = float(os.environ.get('STALE_SECONDS', '0'))  # 0 = 每轮全量复检
STATUS_INTERVAL = float(os.environ.get('STATUS_INTERVAL', '1800'))
MAX_CANDIDATES = int(os.environ.get('MAX_CANDIDATES', '20'))
NETWORK_CACHING = int(os.environ.get('NETWORK_CACHING', '1000'))
# 持续择优：每轮抽样多少健康频道做候选池探测，单轮最多升级几个
OPTIMIZE_SAMPLE = int(os.environ.get('OPTIMIZE_SAMPLE', '24'))
OPTIMIZE_MAX = int(os.environ.get('OPTIMIZE_MAX', '8'))
OPTIMIZE_GAIN = float(os.environ.get('OPTIMIZE_GAIN', '20'))
MAX_POOL_PROBE = int(os.environ.get('MAX_POOL_PROBE', '6'))
ORDER = ['大陆', '中国香港', '中国澳门', '中国台湾', '华语', '其他']

# 全量中文台名映射（names_zh.json）；缺失时用兜底规则生成中文名
try:
    _NAMES_RAW = open(NAMES, 'rb').read()
    NAME_ZH = json.loads(_NAMES_RAW.decode('utf-8'))
    NAMES_HASH = hashlib.md5(_NAMES_RAW).hexdigest()
except Exception:
    NAME_ZH, NAMES_HASH = {}, ''

_SUBS = [
    (r'\bSatellite TV\b', '卫视'), (r'\bNews Channel\b', '新闻频道'),
    (r'\bMovie Channel\b', '电影频道'), (r'\bSports\b', '体育'),
    (r'\bChildren\'?s?\b', '少儿'), (r'\bComprehensive\b', '综合'),
    (r'\bInternational\b', '国际'), (r'\bEntertainment\b', '娱乐'),
    (r'\bInformation\b', '资讯'), (r'\bDocumentary\b', '纪录'),
    (r'\bEconomy\b', '经济'), (r'\bEducation\b', '教育'),
    (r'\bScience\b', '科教'), (r'\bCulture\b', '文化'),
    (r'\bVariety\b', '综艺'), (r'\bTravel\b', '旅游'),
    (r'\bShopping\b', '购物'), (r'\bCartoon\b', '卡通'),
    (r'\bPublic\b', '公共'), (r'\bLife\b', '生活'),
    (r'\bNews\b', '新闻'), (r'\bMovie\b', '电影'),
    (r'\bChannel\b', '频道'), (r'\bTV\b', '电视台'),
]

ATTR_RE = re.compile(r'''([A-Za-z0-9_.:-]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s,]+))''')


def norm(s: str) -> str:
    s = (s or '').lower()
    s = re.sub(r'\[[^\]]*\]|\([^)]*\)', '', s)
    s = re.sub(r'\b(hd|fhd|uhd|sd|4k|8k|2160p|1080p|720p|576i|480p)\b', '', s)
    return re.sub(r'[^0-9a-z\u4e00-\u9fff]', '', s)


def key_of(tvg_id: str, name: str) -> str:
    tid = (tvg_id or '').split('@')[0].lower()
    k = re.sub(r'[^0-9a-z]', '', tid)
    return k or ('n' + hashlib.md5(norm(name).encode()).hexdigest()[:12])


def parse(text: str, group: str) -> list:
    out, pending = [], None

    def flush():
        nonlocal pending
        if pending and pending.get('url', '').startswith(('http://', 'https://')):
            out.append(pending)
        pending = None

    for raw in (text or '').lstrip('\ufeff').splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith('#EXTINF'):
            flush()
            q, cut = None, len(line)
            for i, ch in enumerate(line):
                if ch in ('"', "'"):
                    if q is None:
                        q = ch
                    elif q == ch:
                        q = None
                elif ch == ',' and q is None:
                    cut = i
                    break
            attrs = {}
            for m in ATTR_RE.finditer(line[:cut]):
                attrs[m.group(1).lower()] = next((g for g in m.groups()[1:] if g is not None), '')
            headers = {}
            if attrs.get('http-user-agent'):
                headers['User-Agent'] = attrs['http-user-agent']
            pending = {
                'name': line[cut + 1:].strip() or attrs.get('tvg-name', '未命名'),
                'tvg_id': attrs.get('tvg-id', ''),
                'logo': attrs.get('tvg-logo', ''),
                'group': group or attrs.get('group-title', '其他'),
                'headers': headers, 'url': '',
            }
        elif line.startswith('#EXTVLCOPT:http-user-agent=') and pending:
            pending['headers']['User-Agent'] = line.split('=', 1)[1]
        elif line.startswith('#EXTVLCOPT:http-referrer=') and pending:
            pending['headers']['Referer'] = line.split('=', 1)[1]
        elif line.startswith('#'):
            continue
        elif pending and not pending['url']:
            pending['url'] = line
            flush()
    flush()
    if out:
        return out
    res = []
    for line in (text or '').splitlines():
        line = line.strip()
        if not line or line.startswith('#') or not re.search(r'https?://', line):
            continue
        if ',' in line and re.search(r'https?://', line.split(',', 1)[1]):
            name, url = [x.strip() for x in line.split(',', 1)]
        else:
            name, url = line.rsplit('/', 1)[-1][:40], line
        if url.startswith(('http://', 'https://')):
            res.append({'name': name, 'tvg_id': '', 'logo': '', 'group': group or '其他',
                        'headers': {}, 'url': url})
    return res


def fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as r:
        return r.read().decode('utf-8', 'replace')


def _res(ok, reason, t0, **kw):
    d = {'ok': ok, 'reason': reason, 'latency_ms': int((time.time() - t0) * 1000),
         'ttfb_ms': 0, 'seg_ms': 0, 'quality': 0}
    d.update(kw)
    return d


def quality_of(text: str, url: str) -> int:
    """从 #EXT-X-STREAM-INF 的 RESOLUTION/BANDWIDTH 或 URL 标记推断画质 0~4。"""
    head = text[:6000]
    m = re.search(r'RESOLUTION\s*=\s*(\d{3,4})[xX\u00d7](\d{3,4})', head)
    h = int(m.group(2)) if m else 0
    if not h:
        m2 = re.search(r'(2160|1080|720|576|480)[pi]?', (head + ' ' + url).lower())
        h = int(m2.group(1)) if m2 else 0
    if h >= 1080:
        return 4
    if h >= 720:
        return 3
    if h >= 576:
        return 2
    if h > 0:
        return 1
    bw = re.search(r'BANDWIDTH\s*=\s*(\d+)', head)
    if bw and int(bw.group(1)) >= 3000000:
        return 3
    return 0


def probe(item) -> dict:
    """真实播放链路探测：拉播放列表 -> 校验 #EXTM3U -> 拉首分片并读数据。

    返回 ok(True/False/None) + 延迟/首字节/首分片耗时/画质，作为选源评分依据。
    None 表示「网络不可达，无法判定」——常见于海外节点访问国内内网源，不判失效。
    """
    url = item['url']
    uh = item.get('headers') or {}
    hdr = {'User-Agent': UA, 'Range': 'bytes=0-32768', 'Accept': '*/*',
           'Connection': 'close'}
    hdr.update(uh)
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers=hdr)
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            if r.status >= 400:
                return _res(False, 'HTTP %d' % r.status, t0)
            data = r.read(32768)
            ttfb = int((time.time() - t0) * 1000)
            final = r.geturl()
    except urllib.error.HTTPError as ex:
        # 明确的 HTTP 4xx/5xx：属于确认失效，不是网络不可达
        return _res(False, 'HTTP %d' % ex.code, t0)
    except Exception as ex:
        return _res(None, '网络不可达: %s' % type(ex).__name__, t0)
    latency = int((time.time() - t0) * 1000)
    text = data.decode('utf-8', 'replace')
    quality = quality_of(text, url)
    if text.lstrip().startswith('#EXTM3U') or '.m3u8' in url.lower():
        if '#EXTM3U' not in text:
            return _res(False, '无效播放列表', t0, ttfb_ms=ttfb, quality=quality)
        seg = next((l.strip() for l in text.splitlines()
                    if l.strip() and not l.strip().startswith('#')), '')
        seg_ms = 0
        if seg:
            ts = time.time()
            try:
                h2 = {'User-Agent': UA, 'Range': 'bytes=0-1024',
                      'Connection': 'close'}
                for k, v in uh.items():
                    if k.lower() in ('user-agent', 'referer'):
                        h2[k] = v
                req2 = urllib.request.Request(urllib.parse.urljoin(final, seg), headers=h2)
                with urllib.request.urlopen(req2, timeout=TIMEOUT) as r2:
                    if r2.status >= 400:
                        return _res(False, '分片 HTTP 错误', t0, ttfb_ms=ttfb,
                                    quality=quality)
                    if not r2.read(512):
                        return _res(None, '分片无数据（不确定）', t0, ttfb_ms=ttfb,
                                    quality=quality)
                seg_ms = int((time.time() - ts) * 1000)
            except urllib.error.HTTPError:
                return _res(False, '分片 HTTP 错误', t0, ttfb_ms=ttfb, quality=quality)
            except Exception:
                return _res(None, '分片不可达（不确定）', t0, ttfb_ms=ttfb,
                            quality=quality)
        return _res(True, '', t0, ttfb_ms=ttfb, seg_ms=seg_ms, quality=quality,
                    latency_ms=latency)
    if text.lstrip().startswith('<') or len(data) < 128:
        return _res(False, '非视频数据', t0, ttfb_ms=ttfb)
    return _res(True, '', t0, ttfb_ms=ttfb, quality=quality, latency_ms=latency)


def probe_many(items) -> dict:
    """并发探测并按 URL 去重，返回 {url: result}。"""
    uniq = {}
    for it in items:
        uniq.setdefault(it['url'], it)
    out = {}
    if not uniq:
        return out
    with ThreadPoolExecutor(max_workers=min(WORKERS, len(uniq))) as ex:
        futs = {ex.submit(probe, it): u for u, it in uniq.items()}
        for f in as_completed(futs):
            u = futs[f]
            try:
                out[u] = f.result()
            except Exception as ex:
                out[u] = _res(None, str(ex)[:60], time.time())
    return out


def score_of(c: dict, r: dict, weight: float = 5.0, now: float = 0) -> float:
    """候选源综合评分：加载越快、画质越好、历史越稳，分越高。"""
    ok = r.get('ok')
    if ok is False:
        return -1000.0
    lat = r.get('latency_ms') or 99999
    ttfb = r.get('ttfb_ms') or lat
    seg = r.get('seg_ms') or 0
    s = 100.0
    s -= min(40.0, lat / 25.0)                     # 加载延迟（核心）
    s -= min(15.0, ttfb / 60.0)                    # 首字节
    s -= min(15.0, seg / 60.0)                     # 首分片耗时
    s -= min(30.0, c.get('consec_fail', 0) * 10)   # 连续失败
    tot = c.get('ok', 0) + c.get('fail', 0)
    if tot:
        s -= min(25.0, 25.0 * c.get('fail', 0) / tot)   # 历史失败率
    s -= min(12.0, c.get('unknown', 0) * 1.5)     # 不确定次数
    if c.get('last_ok'):
        s -= min(12.0, max(0.0, (now - c['last_ok']) / 3600.0))  # 陈旧
    s += min(12.0, (r.get('quality') or 0) * 3.0)  # 画质
    s += (weight - 5.0) * 1.5                      # 上游权重
    if ok is None:
        s -= 25.0                                  # 不确定降权，但仍可作为备选
    else:
        s += 5.0                                   # 明确通过验证
    return s


def update_stats(c: dict, r: dict):
    """把本轮探测结果累计到候选的历史统计里。"""
    ok = r.get('ok')
    if ok is True:
        c['ok'] = c.get('ok', 0) + 1
        c['consec_fail'] = 0
        c['last_ok'] = time.time()
        lat = r.get('latency_ms') or 0
        c['lat_ema'] = int((c.get('lat_ema') or lat) * 0.6 + lat * 0.4)
        c['quality'] = max(c.get('quality', 0), r.get('quality') or 0)
    elif ok is False:
        c['fail'] = c.get('fail', 0) + 1
        c['consec_fail'] = c.get('consec_fail', 0) + 1
    else:
        c['unknown'] = c.get('unknown', 0) + 1


def load_json(path, default):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def fallback_zh(name: str) -> str:
    """未收录频道：去掉画质/标记，把常见英文词换成中文，保证输出仍是中文名。"""
    s = re.sub(r'\[[^\]]*\]|\([^)]*\)', '', name or '')
    s = re.sub(r'\b(HD|SD|FHD|UHD|4K|8K|\d+p|\d+i)\b', '', s, flags=re.I)
    for pat, zh in _SUBS:
        s = re.sub(pat, zh, s, flags=re.I)
    s = re.sub(r'\s+', ' ', s).strip(' -·|,')
    if not re.search(r'[\u4e00-\u9fff]', s):
        s = '海外频道·' + (s or '未命名')
    return s or '未命名频道'


def zh_name(c) -> str:
    k = key_of(c.get('tvg_id', ''), c.get('name', ''))
    return NAME_ZH.get(k) or fallback_zh(c.get('name', ''))


def render(chans, updated_iso, healthy_only=False):
    lines = ['#EXTM3U',
             f'# 自动维护：{updated_iso}',
             '# 固定地址，后台只替换失效频道的源，订阅 URL 永不改变']
    if healthy_only:
        lines.append('# 纯净版：只保留未确认失效的频道')
    for c in chans:
        if not c.get('active'):
            continue
        bad = not c.get('healthy', True)
        if healthy_only and bad:
            continue
        group = ('待修复·' + c['group']) if bad else c['group']
        name = zh_name(c)
        attrs = [f'tvg-id="{c.get("tvg_id") or c["key"]}"',
                 f'tvg-name="{name}"']
        if c.get('logo'):
            attrs.append(f'tvg-logo="{c["logo"]}"')
        attrs.append(f'group-title="{group}"')
        lines.append(f'#EXTINF:-1 {" ".join(attrs)},{name}')
        # 播放器侧加载优化：断流自动重连、缩短起播缓冲、透传鉴权头
        lines.append('#EXTVLCOPT:http-reconnect=true')
        lines.append(f'#EXTVLCOPT:network-caching={NETWORK_CACHING}')
        h = c.get('active_headers') or {}
        if h.get('User-Agent'):
            lines.append('#EXTVLCOPT:http-user-agent=' + h['User-Agent'])
        if h.get('Referer'):
            lines.append('#EXTVLCOPT:http-referrer=' + h['Referer'])
        lines.append(c['active'])
    return '\r\n'.join(lines) + '\r\n'


def extra_sources() -> list:
    """自动扫描 sources.d/ 下的自定义源（放置你自己有权使用的订阅）。"""
    out = []
    if not os.path.isdir(SOURCES_DIR):
        return out
    for fn in sorted(os.listdir(SOURCES_DIR)):
        if fn.lower().endswith(('.m3u', '.m3u8', '.txt')):
            out.append({'name': '自定义·' + fn, 'group': '',
                        'url': 'file:sources.d/' + fn,
                        'weight': float(os.environ.get('LOCAL_WEIGHT', '6')),
                        'enabled': True})
    return out


def main():
    src_cfg = load_json(SOURCES, {'sources': []})
    sources = [s for s in (src_cfg.get('sources') or []) if s.get('enabled', True)]
    sources += extra_sources()
    state = load_json(STATE, {'channels': {}, 'log': []})
    channels = state.setdefault('channels', {})
    now = time.time()
    added = pool_added = 0
    health_before = {k: bool(c.get('healthy', True)) for k, c in channels.items()}

    def pull(src):
        u = src.get('url', '')
        # 可编辑的仓库内本地源：url 写成 "custom.m3u" 或 "file:custom.m3u"
        if u.startswith('file:') or not u.startswith(('http://', 'https://')):
            rel = u.split(':', 1)[-1] if u.startswith('file:') else u
            try:
                with open(os.path.join(ROOT, rel.lstrip('/')), 'r', encoding='utf-8') as f:
                    return src, f.read()
            except Exception as ex:
                print(f"[warn] 本地源 {src.get('name')} 读取失败: {type(ex).__name__}")
                return src, None
        try:
            return src, fetch(u)
        except Exception as ex:
            print(f"[warn] 上游 {src.get('name')} 拉取失败: {type(ex).__name__}")
            return src, None

    # 1) 并行拉取所有上游，把新地址补进备用池（不改动当前生效地址）
    with ThreadPoolExecutor(max_workers=max(1, min(10, len(sources) or 1))) as ex:
        pulled = list(ex.map(pull, sources))
    for src, text in pulled:
        if not text:
            continue
        w = float(src.get('weight', 5))
        for e in parse(text, src.get('group', '')):
            k = key_of(e['tvg_id'], e['name'])
            ch = channels.get(k)
            if not ch:
                if src.get('merge_only'):
                    continue  # 大型聚合列表只补候选，避免频道数失控
                ch = {'key': k, 'name': e['name'], 'tvg_id': e['tvg_id'],
                      'logo': e['logo'], 'group': src.get('group') or e['group'],
                      'active': e['url'], 'active_headers': e['headers'],
                      'candidates': [], 'switches': 0, 'last_check': 0,
                      'last_ok': 0, 'healthy': True, 'prev_active': ''}
                channels[k] = ch
                added += 1
            if not ch.get('active'):
                ch['active'] = e['url']
                ch['active_headers'] = e['headers']
            urls = {c['url'] for c in ch['candidates']}
            # 候选池已满时不再追加：否则被裁掉的地址每轮都会被上游重新加回，
            # 造成 5 分钟周期里无意义的反复增删与刷屏提交。
            if (e['url'] not in urls and
                    (len(ch['candidates']) < MAX_CANDIDATES
                     or e['url'] == ch['active'])):
                ch['candidates'].append({'url': e['url'], 'headers': e['headers'],
                                         'weight': w, 'ok': 0, 'fail': 0,
                                         'unknown': 0, 'consec_fail': 0,
                                         'lat_ema': 0, 'quality': 0, 'last_ok': 0})
                pool_added += 1
            else:
                for c in ch['candidates']:
                    if c['url'] == e['url']:
                        c['weight'] = max(c.get('weight', 5), w)
                        if e['headers']:
                            c['headers'] = e['headers']
                        break
            if len(ch['candidates']) > MAX_CANDIDATES:
                act = ch['active']
                ranked = sorted(ch['candidates'],
                                key=lambda c: (c['url'] == act,
                                               c.get('ok', 0) - c.get('fail', 0),
                                               -c.get('lat_ema', 99999)),
                                reverse=True)
                ch['candidates'] = ranked[:MAX_CANDIDATES]

    def item_of(c):
        return {'url': c['url'], 'headers': c.get('headers') or {}}

    def find_cand(ch, url):
        return next((c for c in ch['candidates'] if c['url'] == url), None)

    # 2) 每轮全量复检 active（URL 去重后并发探测，明显缩短单轮耗时）
    todo = [ch for ch in channels.values()
            if now - ch.get('last_check', 0) > STALE or not ch.get('healthy', True)]
    print(f"频道总数 {len(channels)}，本次需复检 {len(todo)} 个")
    act_items = [{'url': ch['active'],
                  'headers': (ch.get('active_headers')
                              or (find_cand(ch, ch['active']) or {}).get('headers')
                              or {})} for ch in todo]
    url_res = probe_many(act_items)
    results = {ch['key']: url_res.get(ch['active'], _res(None, '未探测', now))
               for ch in todo}

    dead = [k for k, r in results.items() if r['ok'] is False]
    unknown = [k for k, r in results.items() if r['ok'] is None]
    print(f"确认失效 {len(dead)} 个，网络不确定 {len(unknown)} 个（不判失效、保留原地址）")

    # 更新 active 对应候选的统计
    for k, r in results.items():
        ch = channels[k]
        c = find_cand(ch, ch['active'])
        if c:
            update_stats(c, r)

    healed = optimized = rollback = 0

    def switch_to(ch, cand, label):
        nonlocal healed, optimized
        prev = ch['active']
        ch['active'] = cand['url']
        ch['active_headers'] = cand.get('headers') or {}
        ch['prev_active'] = prev
        ch['healthy'] = True
        ch['last_ok'] = now
        ch['last_check'] = now
        ch['switches'] = ch.get('switches', 0) + 1
        if label == 'heal':
            healed += 1
            msg = f"[{zh_name(ch)}] 单独替换失效地址（第 {ch['switches']} 次，评分择优）"
        else:
            optimized += 1
            msg = (f"[{zh_name(ch)}] 择优升级到更快/更稳的源"
                   f"（第 {ch['switches']} 次）")
        state.setdefault('log', []).append({'t': now, 'msg': msg})
        return True

    # 3) 对确认失效的频道：探测备用池 -> 按评分选最优 -> 换后即时复验
    for k in dead:
        ch = channels[k]
        pool = [c for c in ch['candidates']
                if c['url'] != ch['active']][:MAX_POOL_PROBE]
        chosen = None
        if pool:
            cres = probe_many([item_of(c) for c in pool])
            for c in pool:
                update_stats(c, cres.get(c['url'], _res(None, '未探测', now)))
            ranked = sorted(
                pool,
                key=lambda c: score_of(c, cres.get(c['url'], {}),
                                       c.get('weight', 5), now),
                reverse=True)
            for cand in ranked:
                r = cres.get(cand['url'], {})
                if r.get('ok') is False:
                    break  # 后面的分数更低，无需再看
                # 换源后即时复验，确保新地址真的能播
                verify = probe(item_of(cand))
                if verify['ok'] is not False:
                    chosen = cand
                    break
        if chosen:
            switch_to(ch, chosen, 'heal')
            continue
        # 所有候选都不可用：尝试回滚到上一个已验证可用的地址
        prev = ch.get('prev_active')
        if prev and prev != ch['active']:
            prev_cand = find_cand(ch, prev) or {'url': prev, 'headers': {}}
            pv = probe(item_of(prev_cand))
            if pv['ok'] is not False:
                switch_to(ch, prev_cand, 'heal')
                rollback += 1
                state['log'][-1]['msg'] += '（回滚历史可用源）'
                continue
        ch['healthy'] = False
        ch['last_check'] = now
        state.setdefault('log', []).append(
            {'t': now, 'msg': f"[{zh_name(ch)}] 备用池 {len(pool)} 个地址均不可用，标记待修复"})

    for k, r in results.items():
        ch = channels[k]
        if r['ok'] is None:
            ch['last_check'] = now   # 探测节点不可达，保留原地址与健康状态
        elif r['ok'] is True:
            ch['healthy'] = True
            ch['last_check'] = now
            ch['last_ok'] = now

    # 4) 持续择优：轮转抽样健康频道，若备用池里有明显更优的源则主动升级
    healthy_keys = [k for k, ch in channels.items()
                    if ch.get('healthy', True) and len(ch['candidates']) > 1
                    and results.get(k, {}).get('ok') is True]
    healthy_keys.sort()
    cursor = int(state.get('opt_cursor', 0))
    sample = []
    if healthy_keys and OPTIMIZE_SAMPLE > 0:
        for i in range(min(OPTIMIZE_SAMPLE, len(healthy_keys))):
            sample.append(healthy_keys[(cursor + i) % len(healthy_keys)])
        state['opt_cursor'] = (cursor + len(sample)) % len(healthy_keys)
    for k in sample:
        if optimized >= OPTIMIZE_MAX:
            break
        ch = channels[k]
        cur_res = results.get(k, {})
        cur_cand = find_cand(ch, ch['active']) or {}
        cur_score = score_of(cur_cand, cur_res, cur_cand.get('weight', 5), now)
        pool = [c for c in ch['candidates']
                if c['url'] != ch['active']][:MAX_POOL_PROBE]
        if not pool:
            continue
        cres = probe_many([item_of(c) for c in pool])
        for c in pool:
            update_stats(c, cres.get(c['url'], _res(None, '未探测', now)))
        best, best_score = None, cur_score + OPTIMIZE_GAIN
        for c in pool:
            r = cres.get(c['url'], {})
            if r.get('ok') is not True:   # 择优只接受明确验证通过的源
                continue
            sc = score_of(c, r, c.get('weight', 5), now)
            if sc > best_score:
                best, best_score = c, sc
        if best:
            switch_to(ch, best, 'optimize')

    state['log'] = state.get('log', [])[-100:]
    updated_iso = datetime.now(timezone.utc).isoformat()

    # 5) 幂等写入：只有实质变化才重写播放列表；统计与看板按节流落盘
    flipped = any(bool(c.get('healthy', True)) != health_before.get(k, True)
                  for k, c in channels.items())
    names_changed = state.get('names_hash') != NAMES_HASH
    dirty = bool(added or pool_added or healed or optimized or rollback
                 or flipped or names_changed)
    first_run = not (os.path.exists(OUT_M3U) and os.path.exists(STATE))
    write_all = dirty or first_run

    os.makedirs(os.path.dirname(OUT_M3U), exist_ok=True)
    chans = sorted(channels.values(),
                   key=lambda c: (0 if c.get('healthy', True) else 1,
                                  ORDER.index(c['group']) if c['group'] in ORDER else 99,
                                  c['name']))
    if write_all:
        state['updated'] = updated_iso
        state['names_hash'] = NAMES_HASH
        with open(OUT_M3U, 'w', encoding='utf-8', newline='') as f:
            f.write(render(chans, updated_iso))
        with open(OUT_HEALTHY, 'w', encoding='utf-8', newline='') as f:
            f.write(render(chans, updated_iso, healthy_only=True))

    prev = load_json(OUT_STATUS, {})
    prev_ts = float(prev.get('_ts', 0) or 0)
    status_due = (not os.path.exists(OUT_STATUS) or dirty or first_run
                  or now - prev_ts >= STATUS_INTERVAL)
    # state.json 与看板同节拍落盘，避免每 5 分钟刷一次提交
    if write_all or status_due:
        with open(STATE, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=1)
    if write_all:
        print(f"已更新 docs/live.m3u（{len(chans)} 频道）与 docs/healthy.m3u")
    else:
        print("本轮无实质变化：订阅文件内容保持不变，不产生提交")

    if status_due:
        healthy = sum(1 for c in channels.values() if c.get('healthy'))
        lats = [r['latency_ms'] for r in results.values() if r['ok'] is True]
        lats.sort()
        total_cand = sum(len(c['candidates']) for c in channels.values())
        multi = sum(1 for c in channels.values() if len(c['candidates']) > 1)
        with open(OUT_STATUS, 'w', encoding='utf-8') as f:
            json.dump({'channels': len(channels), 'checked': len(todo),
                       'healthy': healthy, 'dead': len(dead), 'unknown': len(unknown),
                       'healed': healed, 'optimized': optimized, 'rollback': rollback,
                       'pending_fix': sum(1 for c in channels.values()
                                          if not c.get('healthy')),
                       'candidates_total': total_cand,
                       'multi_candidate_channels': multi,
                       'avg_candidates': round(total_cand / max(1, len(channels)), 2),
                       'median_latency_ms': lats[len(lats) // 2] if lats else 0,
                       'note': 'unknown=探测节点网络不可达，不等于用户端失效；此类频道保留原地址',
                       'scoring': '换源按延迟/首分片/画质/历史成功率/上游权重综合评分选最优，换后即时复验',
                       'interval': '每 5 分钟（GitHub Actions 允许的最短周期）',
                       'updated': updated_iso,
                       'log': state['log'][-20:][::-1], '_ts': now},
                      f, ensure_ascii=False, indent=2)
    print(f"完成：新增 {added} 频道，新增备用 {pool_added}，复检 {len(todo)}，"
          f"确认失效 {len(dead)}，不确定 {len(unknown)}，换源 {healed}，"
          f"择优升级 {optimized}，回滚 {rollback}，实质变化={dirty}")


if __name__ == '__main__':
    main()
