# IPTV Guardian · 方案 A（GitHub Actions 零运维）

一条**永远不变**的 M3U 订阅地址，后台自动检测失效频道并只替换失效的那几个地址。

## 订阅地址

```
https://liang272.github.io/iptv-guardian/live.m3u
```

播放器只需填一次。状态看板：`https://liang272.github.io/iptv-guardian/status.json`

> 占位符由部署脚本自动替换为真实账号与仓库名，以本文件显示的地址为准。

## 检测周期（已选最短）

| 项 | 值 |
|---|---|
| 调度 cron | `*/5 * * * *` —— GitHub Actions 允许的**最短间隔：5 分钟** |
| 复检范围 | 每轮**全量复检**所有频道（`STALE_SECONDS=0`） |
| 实际间隔 | 5~15 分钟；高峰期 GitHub 可能排队或跳过某次调度 |
| 单次任务上限 | 8 分钟，超过会自动取消（防止排队堆积） |
| 提交策略 | 幂等：无失效/无新增时不写文件、不产生 commit；状态看板每 30 分钟一次心跳 |

GitHub 不支持低于 5 分钟的定时调度，也没有「秒级」方案。想做到「观看中源挂掉几秒内切换」，
只能用方案 B（自建服务 + 播放时代理）。

## 为什么周期与仓库可见性有关（重要）

| 仓库可见性 | GitHub Pages | Actions 分钟 | 5 分钟周期 |
|---|---|---|---|
| **Public**（Free 账户） | ✅ 可用 | ✅ 免费不限量 | ✅ 可持续 |
| Private（Free 账户） | ❌ 不支持 | 2000 分钟/月 | ❌ 约 7 天耗尽 |
| Private（Pro/Team） | ✅ 站点仍公开 | 3000 分钟/月 | ❌ 约 10 天耗尽 |

5 分钟一次约 8640 次/月，即使每次只按 1 分钟计费也远超私有仓库免费额度。
因此**要用最短周期，仓库必须设为 Public**（这也正是脚本默认部署方式）。
仓库里只有公开上游 URL、生成的播放列表和状态日志，不含任何凭据或私密数据。

如果必须保持私有，请同时改两处：

```yaml
# .github/workflows/playlist.yml
- cron: '*/30 * * * *'      # 改回 30 分钟
  env:
    STALE_SECONDS: '1800'   # 与周期一致
```

并把仓库可见性改为 Private；但 Free 账户下 Pages 会失效，订阅地址将无法访问。

## 文件结构

```
.github/workflows/playlist.yml     定时任务（5 分钟）
.github/scripts/update_playlist.py 检测 / 换源 / 生成（仅标准库）
sources.json                       上游源清单
state.json                         每个频道的当前地址与备用池（由脚本维护）
docs/live.m3u                      固定订阅文件（由脚本生成）
docs/status.json                   巡检状态看板（由脚本生成）
```

## 日常操作

- **立即检测一次**：`Actions → 自动维护 IPTV 播放列表 → Run workflow`
- **加源 / 加频道**：编辑 `sources.json` 追加一条，push 后会自动触发一次检测
  ```json
  {"name": "自有授权源", "group": "大陆", "url": "https://your-domain/authorized.m3u"}
  ```
  支持标准 M3U 与 `频道名,URL` 的 TXT。**建议配 ≥2 个上游**，否则很多频道没有备用地址可换。
- **改周期**：改 `playlist.yml` 的 cron（GitHub 最短 5 分钟）
- **看换源记录**：`docs/status.json` 的 `log` 字段，或仓库 commit 历史

## 注意事项

- Pages 站点本身是**公开可访问**的（即使仓库私有也是如此）。请不要公开分享订阅地址。
- GitHub Actions 定时任务在仓库 **60 天无任何活动**时会被自动禁用；本方案每 30 分钟有一次状态心跳提交，可避免触发。
- 上游探测频率较高，若某个上游开始限流，请把该源从 `sources.json` 移除或改用方案 B。
- 本工具不提供、不托管、不缓存任何节目内容，仅维护地址列表。请只接入已获合法授权或版权方主动公开的直播源，
  用于个人学习与家庭自建，遵守所在地广播电视与网络视听相关规定；中国香港、中国澳门、中国台湾地区频道的接收传播另有监管要求。
