# taosx 对抗审查报告

> **日期**：2026-09-22
> **基线**：main 分支 commit `9807c43`（工作区干净）
> **方法**：agent team 对抗审查——Wave 1 五名独立审查员（4 维度审查 + 1 红队攻击）→ Wave 2 两名挑战者（challenger）对全部 30 条发现逐条对抗反驳 → Lead 汇总裁决
> **团队**：`taosx-adversarial-review`（reviewer-config / reviewer-client / reviewer-datapath / reviewer-tests / red-team / challenger-a / challenger-b，全部只读，未修改任何源码）
> **过程产物**：`.omc/artifacts/taosx-review/*.md`（5 份原始报告 + 2 份裁决报告，位于工作区 `.omc/`）

---

## 1. 执行摘要

对 taosx v0.1.4（~7490 行 Rust，TDengine 异步客户端）完成全维度对抗审查。**整体结论：代码质量良好，安全姿态评级「强」**——生产代码零 unwrap/expect/panic/unsafe（`#![forbid(unsafe_code)]` + lint 守卫），SQL 注入防护经红队 10+ 组 payload 穷举推演全部被正确防御，凭据脱敏、TLS 强制、重定向禁用、响应体三重限额等 18 项防御点经挑战者抽查 14 处全部属实。

对抗验证的价值得到充分体现：**30 条原始发现中 4 条被推翻**（含 1 条 P0 与 1 条 P1——两者均建立在「tungstenite 默认无帧上限」的错误事实前提上，经查依赖源码证伪），6 条被降级。最终确认：**P0×1、P1×5、P2×19**。

唯一 P0：`WriteBatcher::close()` 竞态窗口导致并发 `push()` 数据**静默永久丢失**。

### 门禁证据（Lead 本机执行 + challenger-b 复跑）

| 门禁 | 命令 | 结果 |
|------|------|------|
| 格式 | `cargo fmt --all -- --check` | ✅ exit 0 |
| 静态分析 | `cargo clippy --workspace --all-targets --all-features -- -D warnings` | ✅ exit 0，零告警 |
| 测试 | `cargo test --workspace --all-features` | ✅ 全绿（67 内联 + 51 集成，2 个 live 用例按设计 ignored） |

---

## 2. 确认发现（经对抗验证）

### 2.1 P0（1 条）

#### P0-1 `WriteBatcher::close()` 竞态：并发 push 数据静默永久丢失

- **位置**：`src/batcher.rs:181-192`
- **机制**：`close()` 在 L179 `take(buffer)` 后于 L181 释放锁，随后无锁执行 `flush_batch().await`（网络 I/O）；成功返回后 L185 重新获取锁，**仅检查 `failed_pending`，不检查 buffer 是否被重新填充**，直接 L192 `closed = true`。窗口期内并发 `push()`（`closed==false && pending==None`，必然入队成功）写入的数据无任何后续消费路径——无 flush、无报错、无 metrics。
- **触发条件**：`Arc<WriteBatcher>` 多 task 共享（batcher 的设计使用场景），一个 task `close()` 期间另一 task `push()`。窗口宽度 = flush 网络 RTT。
- **测试盲区**：现有 `full_success_flush_then_close` 等用例只测单线程顺序路径，无 close 并发 push 用例（grep 确认 0 命中）。
- **裁决记录**：⚖️ 挑战者定级分歧——challenger-a 判 P0 维持（数据丢失无恢复路径）；red-team/challenger-b 判 P2（窗口窄、需特定并发模式、close 文档未声明「须无并发 push」前置条件）。**Lead 依宪法 P-5「保守结论优先」裁决为 P0**，分歧双方理由均已记录。
- **修复建议**：`close()` 重获锁后检查 buffer 非空则循环再排空；或引入 `closing` 标志使窗口内 push 直接拒绝（语义更保守）。同时在文档声明关闭语义，并补并发回归测试。

### 2.2 P1（5 条）

| # | 位置 | 问题 | 裁决 |
|---|------|------|------|
| P1-1 | `src/config.rs:300-308` | `validate()` 将 timeout/acquire_timeout/close_timeout 的 4 个校验条件合并为一条模糊错误消息，无字段级定位（违反 R-ERR-005）；同模块其他字段均有独立字段名+范围消息，唯 timeout 组合并 | 确认，P1 维持 |
| P1-2 | `src/error.rs:162` | `TaosError::with_message` 对 `Io` 变体静默丢弃调用方上下文消息（其余 9 个变体均替换），文档承诺「替换错误消息」与实现不一致；现有测试未覆盖 Io 变体 | 确认，P1 维持 |
| P1-3 | `src/client/pool.rs:293` | `detect_precision` 中 database 名置于**单引号字符串字面量**位置（`WHERE name='{database}'`），安全性仅依赖 `validate_ident`（语义是「合法标识符」而非「安全字面量」）。当前白名单严格无漏洞，但未来若放宽 validate_ident（如支持 Unicode），此处转义假设即被破坏——防御深度不足的脆断耦合 | 确认，P1 维持 |
| P1-4 | `src/client/pool.rs:44-53` | 多主机 failover **成功路径**零测试：现有全失败用例（`ping_unreachable.rs:49-53` 单 host）对「首个 Err 即 return」这类回归**零检测力**（两种实现均返回 Err，测试仍绿）。AGENTS.md 宣称的故障转移能力无成功路径验证 | 确认，P1 维持 |
| P1-5 | `src/native.rs:96-118` | WS 成功路径（Text/Binary 帧解析、非数据帧 fail-closed）仅被 `#[ignore]` 的 live 用例行使，CI 从不执行；全仓无 WS 层离线 mock。双传输一等公民的成功分支回归门禁无感知。`tokio-tungstenite` 已是生产依赖，accept 侧 mock 无需新增依赖 | 确认，P1 维持 |

### 2.3 P2（19 条，按域分组）

**配置与错误（6）**

| 位置 | 问题 | 备注 |
|------|------|------|
| `src/config/parse.rs:21` + `config.rs:300-302` | `de_millis` 对 u64::MAX 饱和为 `Duration::MAX`，timeout/acquire_timeout 无上界校验 → 配置「永不超时」可通过校验 | 原 P1，降级（需荒谬输入值，实际风险极低） |
| `src/config.rs:317-318` | `write_max_attempts` 仅查 `==0` 无上界，为全部数值字段中唯一无 HARD_MAX_* 者 | 确认 |
| `src/config.rs:306-308` | 错误消息硬编码「30 秒」，与 `HARD_MAX_CLOSE_TIMEOUT` 常量（L95）存在漂移风险 | 确认 |
| `src/config/parse.rs:69-77` | `env_parsed` 将空字符串视为「取值非法」而非「未设置」，与同模块 `env_non_empty`/`env_trimmed`（空→None）语义不一致 | 确认 |
| `src/config.rs:242-248` | TOML password 为非字符串类型时，错误消息「禁止非空 password 字段」未揭示类型问题 | 确认 |
| `src/error.rs`（Io 变体） | 与 P1-2 同源的文档/测试补全项 | 并入 P1-2 修复 |

**客户端（4）**

| 位置 | 问题 | 备注 |
|------|------|------|
| `src/client/sql.rs:177-178` | `escape_str` 未转义 NUL（`\0`）。Rust→HTTP 传输链 NUL 原样保留；TDengine C 层解析器是否 strlen 截断**未经证实**——定性为防御性加固而非已确认漏洞 | 原 P1，降级（agent 间矛盾裁决见 §4） |
| `src/client.rs:56` | `build_http_client` 中 `std::fs::read`（TLS CA 文件）在 async 可达路径上同步阻塞，违反 R-RT-010；实际风险低（小文件+本地路径） | 确认 |
| `src/client/response.rs:174-181` | `json_cell_to_string` 对 JSON Number 用 `to_string()` 可能科学记数法/精度损失；当前 TDengine 以 string 返回数值单元格故不受影响，属前瞻性风险 | 确认 |
| `src/client/pool.rs:399-408` | HTTP 非成功响应丢弃正文（返回「响应正文已省略」），与成功但 code!=0 路径（正确提取截断 desc）行为不一致，丢失诊断信息；已有 256 字符截断机制可复用 | 确认 |

**数据路径（5）**

| 位置 | 问题 | 备注 |
|------|------|------|
| `src/native.rs:55-62` | `exec_sql_ws` 的 WS 帧上限（库默认 16MiB/64MiB）不随 `max_response_bytes` 配置联动，与 REST 路径限额策略不一致；可显式改用 `connect_async_with_config` 对齐 | 原 P0「无上限 OOM」被推翻后的设计一致性残余（见 §3） |
| `src/batcher.rs:139-144` | `push()` 触发 flush 后若 future 被取消，已 take 的 batch 随栈帧 drop 丢失；与模块文档「非 exactly-once」一致，但 cancel 语义未显式标注 | 原 P1，降级（文档补全项） |
| `src/native.rs:107` | `let _ = socket.close(None).await` 静默吞 WS 关闭错误，无 debug 日志，排障信号丢失（响应已获取，不影响正确性） | 原 P1，降级 |
| `src/retry.rs:145` | `run()` 的 fallback 分支经控制流分析证明不可达（循环末次迭代必走 L129 return），属 harmless 死代码 | 原 P1，降级 |
| `src/batcher.rs:114` | buffer 初始容量 `max_rows.min(1024)` 硬截断，max_rows>1024 时多次重分配；性能影响微小 | 确认 |

**测试与门禁（6）**

| 位置 | 问题 | 备注 |
|------|------|------|
| `src/client/write.rs:39-51` | `write_batch_idempotent` 零直接测试（grep 仅命中定义处；`http_roundtrip.rs:203` 注释写「幂等写路径」实际调用 `write_batch`，注释误导）。组件级已分层锁定（clamp×3 处、RetryPolicy::run、is_retryable、for_idempotent_write），函数本体 8 行胶水，回归面有限 | 原 P1，降级 |
| `src/batcher.rs:136` | `flush_interval` 时间窗触发分支零覆盖：全部用例设 60s，默认 200ms 时间窗路径（低流量兜底 flush）从未行使 | 确认 |
| tests/ 全仓 | 37 处裸 `.is_err()` 弱断言（0 处同行类型核验）+ src/ 内联 26 处；缓解：部分位置有独立 `matches!` 锁定、metrics 计数补偿断言（R-TEST-002） | 确认 |
| `tests/live_taos.rs:157,208` | `#[ignore]` 有原因与运行手册但缺 owner+期限字段（testing.md §6 精神的扩展适用） | 确认 |
| `tests/api_surface.rs:78` | `black_box(aggregate) > 1_000` 重言式断言（无害，真断言在 L63-69）；`ws_probe_totals` 全仓无数值级断言，probe 计数成功/失败交换回归不会红 | 确认 |
| `.github/workflows/ci.yml:43` vs `AGENTS.md:58` | 门禁文案漂移（`cargo test` vs `cargo test --all-targets`）；CI 缺 doc/deny 且无项目宪章书面暂缓声明（ci.md §1）。单 crate 仓库实际差异仅 benches 编译（已由 clippy 覆盖） | 确认 |

---

## 3. 被推翻的发现（对抗审查核心产出）

| 原主张 | 提出者 | 推翻依据 |
|--------|--------|----------|
| **[P0] `exec_sql_ws` WS 帧无大小上限，恶意节点可发数 GB 帧致 OOM** | reviewer-datapath | 两名挑战者**独立**查依赖源码证伪：`tungstenite 0.29.0` `WebSocketConfig::default()` 为 `max_frame_size=16MiB`、`max_message_size=64MiB`；`connect_async` → `connect_async_with_config(req, None, false)` → `unwrap_or_default()` 生效。超限帧返回 `ProtocolError` → 映射为 `Unavailable`，fail-closed。巧合佐证：64MiB 恰等于项目自身 `HARD_MAX_RESPONSE_BYTES`。残余设计一致性关注降为 P2（§2.3） |
| **[P1] 同上（红队独立提出）** | red-team | 同上，challenger-b 以相同证据独立推翻。红队标注「疑似，需验证」的诚实定级得到回报 |
| **[P1] acquire CAS 循环无 `spin_loop_hint`** | reviewer-client | 审查员自认「低风险、防御性改进而非已确认缺陷」；CAS 循环体 <100ns、外层信号量已限并发、x86_64 上 weak CAS 几乎不伪失败、acquire_timeout 兜底。属性能微优化偏好，非缺陷 |
| **[P1] 非 Text/Binary WS 帧被静默忽略** | reviewer-datapath | tokio-tungstenite 对 Ping/Pong/Close 控制帧在 Stream 层自动响应，不暴露给 `socket.next()`，`Ok(_) => {}` 分支在当前库版本不可达；且现有 fallback（空 body → `Unavailable`）是正确 fail-closed |

**教训沉淀**：涉及第三方库默认行为的缺陷主张，必须核查依赖源码实际版本的默认值，不能依赖记忆或文档印象——本次 2 条高严重度发现均因该前提错误被推翻。

---

## 4. Agent 间矛盾裁决记录

### 4.1 NUL 转义：reviewer-client（P1 缺口）vs red-team（已防御）

- **矛盾**：client 审查员主张 `escape_str` 不处理 `\0`，若 TDengine C 层 strlen 截断则可构造 `\0' OR 1=1` 绕过转义；红队推演表标注 NUL「原样保留在字面量内，被防御」。
- **裁决（challenger-a）**：两方不矛盾——NUL 在 Rust String→HTTP body 链路确实原样保留（红队视角正确）；到达 TDengine C 解析器后是否截断**双方均无源码级证据**（client 审查员自己也承认可利用性取决于服务端内部处理）。
- **结论**：定性为 **P2 防御性加固**（补 `\0` 转义成本极低、收益是消除未证实但理论上存在的攻击面），非已确认可利用漏洞。若后续获得 TDengine 解析器截断证据，应回升 P0。

### 4.2 close() 竞态严重度：challenger-a（P0）vs challenger-b（P2）

- 缺陷本身双方均确认（源码逐行吻合、无消费路径、无观测手段）；分歧仅在严重度。
- **Lead 裁决**：依宪法 P-5「两个 Reviewer 结论矛盾时保守结论优先」，定 **P0**。理由：静默数据丢失类缺陷的后果不可逆；`Arc<WriteBatcher>` 并发使用是设计场景而非误用；「窗口窄」是概率缓解而非后果缓解。challenger-b 的 P2 理由（需并发模式触发、文档未声明前置条件）作为修复优先级排期的参考记录在案。

---

## 5. 红队攻击矩阵摘要（18 项防御确认）

| 攻击维度 | 尝试 | 结果 |
|----------|------|------|
| SQL 注入 | `test' OR 1=1 --`、`test\`、`test\' OR 1=1 --`、`\0`、多字节 Unicode+引号、`*/; DROP--` 等 6+ 组字符串 payload | **全部被防御**（`escape_str` 先 `\` 后 `'` 顺序正确，sql.rs:177-178） |
| 标识符注入 | 反引号/分号/注释符/数字开头/空串/空格/换行/制表符等 10 组 | **全部拒绝**（`validate_ident` 白名单 `[A-Za-z_][A-Za-z0-9_]*` + 192 字节限长） |
| 子表名注入 | tag 值注入 | **被防御**（hex 编码 + 二次 validate，sql.rs:130-144） |
| panic 路径 | 全量扫描 unwrap/expect/panic/unreachable/todo/unsafe/索引/整数溢出 | **生产代码 0 命中**（lint 守卫 lib.rs:68-72；challenger-b 以生产段切分法复验） |
| 资源耗尽 | 连接池/响应体/查询行数/重试风暴 | **被防御**（HARD_MAX_* 全覆盖 + 三重限额 response.rs:78-110 + checked_pow 退避）；唯一例外见 P2 WS 限额不联动 |
| 并发 | 池竞态/锁跨 await/任务泄漏 | **被防御**（CLOSED_BIT+drain+timeout、mutex 不跨 await、生产零 spawn）；例外见 P0-1 |
| 凭据/传输 | 密码泄露/TLS 绕过/重定向劫持 | **被防御**（Debug 脱敏 `"***"`、TOML 拒非空 password、远程强制 TLS+密码含 IPv6 loopback 判定、`redirect::Policy::none()`） |

**残余攻击面提示**：`exec()`/`query()` 接受任意 SQL 是设计使然（低级 API），调用方拼接用户输入时库无法防御——建议在 README/API.md 加大警告（API 契约文档项）。

**整体安全姿态评级：强**（红队评定，challenger 抽查 14/18 处防御点全部属实，无虚报防御）。

---

## 6. 修复优先级路线图

| 优先级 | 项 | 动作 | 预估影响面 |
|--------|-----|------|-----------|
| **立即** | P0-1 close() 竞态 | close() 重获锁后循环排空 buffer（或 closing 标志拒绝窗口内 push）+ 并发回归测试 + 文档声明关闭语义 | `src/batcher.rs` 单文件 |
| **下个迭代** | P1-1/P1-2 | 拆分 timeout 校验为独立字段消息；Io 变体支持上下文消息（或更新文档+补测试） | config.rs / error.rs |
| **下个迭代** | P1-3 | `detect_precision` 的 database 字面量改用 `escape_str` 或专用字面量校验，与 validate_ident 解耦 | pool.rs 单点 |
| **下个迭代** | P1-4/P1-5 | 补 failover 成功路径 mock 测试；补 WS 层离线 mock 三分支测试（accept_async，无需新依赖） | tests/ |
| **随触达修复** | P2×19 | 按 §2.3 分域排期；NUL 转义加固（sql.rs 一行）建议提前顺手做 | 分散 |
| **文档项** | exec/query 注入警告、cancel 语义、close 前置条件、CI 文案对齐、#[ignore] 补 owner | 纯文档 | docs/AGENTS.md/README |

**验证要求**（agent-quality-gates）：每项修复须附 `cargo fmt --check` + `cargo clippy -D warnings` + `cargo test` 全绿证据；P0-1 修复必须附带能复现竞态的失败测试先行（红→绿）。

---

## 7. 审查方法与独立性声明

- **角色分离**（宪法 C-3）：审查员、红队、挑战者、Lead 四类角色独立；审查员之间零文件重叠分组；红队与维度审查员互不通信保证独立视角（NUL 矛盾的双方即为独立产出）。
- **对抗验证**：全部 30 条原始发现经挑战者逐条反驳（读原码核实 file:line、依赖源码核查、grep 复算、cargo test 复跑），不采信任何报告自述。
- **只读约束**（REV-F01）：全部 7 个 agent 未修改 taosx 仓库任何源码/测试/配置；本报告是本次审查在 taosx 仓库内的唯一新增文件。
- **局限**：live 路径（真实 TDengine 6030/6041）未行使；TDengine 服务端 C 解析器行为（NUL 截断）无源码级证据；行号以基线 commit `9807c43` 为准。

---

*报告由 team lead 汇总撰写；原始发现与裁决细节见 `.omc/artifacts/taosx-review/`（reviewer-{config,client,datapath,tests}.md、red-team.md、challenger-{a,b}.md）。*
