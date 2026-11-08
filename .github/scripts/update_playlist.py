# -*- coding: utf-8 -*-
"""GitHub Actions 零运维方案：只替换失效频道的地址，固定 URL 永远不变。

运行逻辑：
  state.json（已提交到仓库）保存每个频道的当前地址 + 备用地址池；
  每次运行复检所有频道（最短周期由 workflow 的 cron 控制）；
  某个频道失效时，从它的备用池里挑一个可用地址单独替换；
  最后重新生成 docs/live.m3u —— 文件名/URL 不变，播放器永远订阅同一个地址。

为避免高频周期产生大量无意义提交：
  只有「新增频道 / 备用池变化 / 换源 / 健康状态翻转」才写 state.json 与 live.m3u；
  status.json 按 STATUS_INTERVAL 节流写入（默认 30 分钟）；
  其余轮次只做检测，不改动仓库内容。

仅使用 Python 标准库，无需安装依赖。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SOURCES = os.path.join(ROOT, 'sources.json')
STATE = os.path.join(ROOT, 'state.json')
OUT_M3U = os.path.join(ROOT, 'docs', 'live.m3u')
OUT_STATUS = os.path.join(ROOT, 'docs', 'status.json')

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')
TIMEOUT = float(os.environ.get('PROBE_TIMEOUT', '6'))
FETCH_TIMEOUT = float(os.environ.get('FETCH_TIMEOUT', '10'))
WORKERS = int(os.environ.get('PROBE_WORKERS', '16'))
STALE = float(os.environ.get('STALE_SECONDS', '0'))  # 0 = 每轮全量复检
STATUS_INTERVAL = float(os.environ.get('STATUS_INTERVAL', '1800'))
MAX_CANDIDATES = int(os.environ.get('MAX_CANDIDATES', '10'))

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
            # 找引号外的第一个逗号
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
    # 兼容 `频道名,URL` 的 TXT
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


def probe(item) -> dict:
    """返回 {'ok':bool,'latency_ms':int,'reason':str}。"""
    url = item['url']
    hdr = {'User-Agent': UA, 'Range': 'bytes=0-65535', 'Accept': '*/*'}
    hdr.update(item.get('headers') or {})
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers=hdr)
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            if r.status >= 400:
                return {'ok': False, 'reason': f'HTTP {r.status}',
                        'latency_ms': int((time.time() - t0) * 1000)}
            data = r.read(65536)
            final = r.geturl()
    except Exception as ex:
        return {'ok': False, 'reason': f'{type(ex).__name__}: {str(ex)[:60]}',
                'latency_ms': int((time.time() - t0) * 1000)}
    latency = int((time.time() - t0) * 1000)
    text = data.decode('utf-8', 'replace')
    if text.lstrip().startswith('#EXTM3U') or '.m3u8' in url.lower():
        if '#EXTM3U' not in text:
            return {'ok': False, 'reason': '无效播放列表', 'latency_ms': latency}
        # 复检第一个分片，排除「能连上但没画面」的假活源
        seg = next((l.strip() for l in text.splitlines()
                    if l.strip() and not l.strip().startswith('#')), '')
        if seg:
            try:
                req2 = urllib.request.Request(urllib.parse.urljoin(final, seg),
                                              headers={'User-Agent': UA,
                                                       'Range': 'bytes=0-1024'})
                with urllib.request.urlopen(req2, timeout=TIMEOUT) as r2:
                    if r2.status >= 400 or not r2.read(512):
                        return {'ok': False, 'reason': '分片不可读', 'latency_ms': latency}
            except Exception:
                return {'ok': False, 'reason': '分片不可读', 'latency_ms': latency}
        return {'ok': True, 'latency_ms': latency, 'reason': ''}
    if text.lstrip().startswith('<') or len(data) < 128:
        return {'ok': False, 'reason': '非视频数据', 'latency_ms': latency}
    return {'ok': True, 'latency_ms': latency, 'reason': ''}


def load_json(path, default):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def main():
    sources = load_json(SOURCES, {'sources': []})['sources']
    state = load_json(STATE, {'channels': {}, 'log': []})
    channels = state.setdefault('channels', {})
    now = time.time()
    added = 0
    pool_added = 0
    health_before = {k: bool(c.get('healthy', True)) for k, c in channels.items()}

    def pull(src):
        try:
            return src, fetch(src['url'])
        except Exception as ex:
            print(f"[warn] 上游 {src.get('name')} 拉取失败: {type(ex).__name__}")
            return src, None

    # 1) 并行拉取所有上游，把新地址补进各频道的备用池（不改动当前生效地址）
    with ThreadPoolExecutor(max_workers=max(1, min(8, len(sources) or 1))) as ex:
        pulled = list(ex.map(pull, sources))
    for src, text in pulled:
        if not text:
            continue
        for e in parse(text, src.get('group', '')):
            k = key_of(e['tvg_id'], e['name'])
            ch = channels.get(k)
            if not ch:
                ch = {'key': k, 'name': e['name'], 'tvg_id': e['tvg_id'],
                      'logo': e['logo'], 'group': src.get('group') or e['group'],
                      'active': e['url'], 'candidates': [], 'switches': 0,
                      'last_check': 0, 'last_ok': 0, 'healthy': True}
                channels[k] = ch
                added += 1
            if not ch.get('active'):
                ch['active'] = e['url']
            urls = {c['url'] for c in ch['candidates']}
            if e['url'] not in urls:
                ch['candidates'].append({'url': e['url'], 'headers': e['headers'],
                                         'fails': 0})
                pool_added += 1
            if len(ch['candidates']) > MAX_CANDIDATES:
                keep = [c for c in ch['candidates'] if c['url'] == ch['active']]
                ch['candidates'] = (keep + [c for c in ch['candidates']
                                            if c['url'] != ch['active']][:MAX_CANDIDATES - 1])

    # 2) 复检频道：STALE=0 时每轮全量检测（最短检测周期）
    todo = [ch for ch in channels.values()
            if now - ch.get('last_check', 0) > STALE or not ch.get('healthy', True)]
    print(f"频道总数 {len(channels)}，本次需复检 {len(todo)} 个")
    results = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(probe, {'url': ch['active'],
                                  'headers': next((c['headers'] for c in ch['candidates']
                                                   if c['url'] == ch['active']), {})}): ch['key']
                for ch in todo}
        for f in as_completed(futs):
            try:
                results[futs[f]] = f.result()
            except Exception as ex:
                results[futs[f]] = {'ok': False, 'reason': str(ex)[:60], 'latency_ms': 0}

    dead = [k for k, r in results.items() if not r['ok']]
    print(f"确认失效 {len(dead)} 个频道，开始单独换源")

    # 3) 对失效频道：从备用池找出一个可用地址，逐个替换（其它频道完全不动）
    healed = 0
    for k in dead:
        ch = channels[k]
        pool = [c for c in ch['candidates'] if c['url'] != ch['active']]
        if not pool:
            ch['healthy'] = False
            ch['last_check'] = now
            state.setdefault('log', []).append(
                {'t': now, 'msg': f"[{ch['name']}] 失效但无备用地址"})
            continue
        with ThreadPoolExecutor(max_workers=min(6, len(pool))) as ex:
            futs = {ex.submit(probe, c): c for c in pool}
            cand_res = {}
            for f in as_completed(futs):
                try:
                    cand_res[futs[f]['url']] = f.result()
                except Exception:
                    cand_res[futs[f]['url']] = {'ok': False}
        ok = [c for c in pool if cand_res[c['url']]['ok']]
        ok.sort(key=lambda c: cand_res[c['url']]['latency_ms'])
        if ok:
            ch['active'] = ok[0]['url']
            ch['healthy'] = True
            ch['last_ok'] = now
            ch['switches'] = ch.get('switches', 0) + 1
            healed += 1
            state.setdefault('log', []).append(
                {'t': now, 'msg': f"[{ch['name']}] 已单独替换失效地址（第 {ch['switches']} 次）"})
        else:
            ch['healthy'] = False
            state.setdefault('log', []).append(
                {'t': now, 'msg': f"[{ch['name']}] 备用池 {len(pool)} 个地址均不可用"})
        ch['last_check'] = now

    for k, r in results.items():
        if r['ok']:
            ch = channels[k]
            ch['healthy'] = True
            ch['last_check'] = now
            ch['last_ok'] = now

    state['log'] = state.get('log', [])[-100:]
    updated_iso = datetime.now(timezone.utc).isoformat()

    # 4) 幂等写入：只有实质变化才更新 M3U 与 state，避免高频周期刷屏提交
    flipped = any(bool(c.get('healthy', True)) != health_before.get(k, True)
                  for k, c in channels.items())
    dirty = bool(added or pool_added or healed or flipped)
    first_run = not (os.path.exists(OUT_M3U) and os.path.exists(STATE))
    write_playlist = dirty or first_run

    os.makedirs(os.path.dirname(OUT_M3U), exist_ok=True)
    if write_playlist:
        state['updated'] = updated_iso
        order = ['大陆', '中国香港', '中国澳门', '中国台湾', '华语', '其他']
        chans = sorted(channels.values(),
                       key=lambda c: (order.index(c['group']) if c['group'] in order else 99,
                                      c['name']))
        lines = ['#EXTM3U',
                 f'# 自动维护：{updated_iso}',
                 '# 固定地址，后台只替换失效频道的源，订阅 URL 永不改变']
        for c in chans:
            if not c.get('active'):
                continue
            attrs = [f'tvg-id="{c.get("tvg_id") or c["key"]}"',
                     f'tvg-name="{c["name"]}"']
            if c.get('logo'):
                attrs.append(f'tvg-logo="{c["logo"]}"')
            attrs.append(f'group-title="{c["group"]}"')
            lines.append(f'#EXTINF:-1 {" ".join(attrs)},{c["name"]}')
            lines.append(c['active'])
        with open(OUT_M3U, 'w', encoding='utf-8', newline='') as f:
            f.write('\r\n'.join(lines) + '\r\n')
        with open(STATE, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=1)
        print(f"已更新 docs/live.m3u（{len(chans)} 频道）")
    else:
        print("本轮无实质变化：订阅文件内容保持不变，不产生提交")

    # status.json 按间隔节流，保证看板有心跳但不刷屏
    prev = load_json(OUT_STATUS, {})
    prev_ts = float(prev.get('_ts', 0) or 0)
    status_due = (not os.path.exists(OUT_STATUS) or dirty or first_run
                  or now - prev_ts >= STATUS_INTERVAL)
    if status_due:
        with open(OUT_STATUS, 'w', encoding='utf-8') as f:
            json.dump({'channels': len(channels), 'checked': len(todo),
                       'dead': len(dead), 'healed': healed,
                       'healthy': sum(1 for c in channels.values() if c.get('healthy')),
                       'interval': '每 5 分钟（GitHub Actions 允许的最短周期）',
                       'updated': updated_iso,
                       'log': state['log'][-20:][::-1], '_ts': now},
                      f, ensure_ascii=False, indent=2)
    print(f"完成：新增 {added} 频道，新增备用 {pool_added}，复检 {len(todo)}，"
          f"失效 {len(dead)}，已单独换源 {healed}，实质变化={dirty}")


if __name__ == '__main__':
    main()
