# CONTRIBUTING.md — 贡献指南（taosx）

本文件面向贡献者，汇总本地门禁与提交约定。
AI Agent 的工作约定另见 [`AGENTS.md`](./AGENTS.md)；术语与领域语言见 [`CONTEXT.md`](./CONTEXT.md)。

## 开发流程

- 本仓库是**独立的单 crate 仓库**，不依赖 `xhyper.rs` 主工程及其内部 crate（`kernel` /
  `contracts` 等），只使用 crates.io 公开依赖。
- substantial 变更走 feature branch → PR → review → merge，**禁止直接 push `main`**。
- `main` 已启用分支保护：要求 PR + 必需检查 `fmt / clippy / test`，
  `required_approving_review_count = 0`（单人也能合并），禁止强推与删除。
- 合并方式固定为 **create a merge commit**。注意仓库设置是
  `merge_commit_title = MERGE_MESSAGE` + `merge_commit_message = PR_TITLE`，因此
  `gh pr merge` 必须显式传 `--subject` 与 `--body`，否则会产出通用
  `Merge pull request #N from …` 标题。
- 提交信息遵循 Conventional Commits（`feat:` / `fix:` / `docs:` / `ci:` / `chore:` /
  `refactor:`），描述用简体中文。

## 本地门禁（P0 三件套）

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

无需 `--all-features`：本 crate 没有可切换 feature。

元数据完整性门禁（**不发布 crates.io**，此命令只校验打包元数据）：

```bash
cargo package --no-verify --allow-dirty
```

`--allow-dirty` 用于工作区存在未提交改动时；`cargo package` 会打印 `Packaged N files`，
可用于确认新文档已进入打包白名单。

热路径基准（离线，无需 TDengine 服务）：

```bash
cargo bench --bench hot_path -- --quick
```

## 复用口径（不发布 crates.io）

- 本 crate **不发布到 crates.io**，仅以 GitHub 源码 / git 依赖形式复用。
- 文档与元数据中不得出现「可独立发布」「可直接 `cargo publish`」等表述，
  也不得放置 crates.io / docs.rs 徽章与外链。
- `Cargo.toml` 的 `documentation` 指向 `https://github.com/bytechainx/taosx#readme`。
- 消费方引入方式（README「安装」小节为准）：

  ```toml
  [dependencies]
  taosx = { git = "https://github.com/bytechainx/taosx" }
  ```

## 开发约定

- 注释、文档、错误消息使用**简体中文**；标识符保持英文。
- 错误类型：`thiserror` 枚举 + `#[non_exhaustive]` + `pub type TaosResult<T>` 别名。
- 不在库代码里裸 `unwrap()`（`[lints.clippy]` 已 `deny` `unwrap_used` / `expect_used` /
  `panic`）。
- 所有 `pub` 项必须有中文 `///` 文档（`src/lib.rs` 还 `forbid(unsafe_code)`、
  `deny(missing_docs)` / `deny(unreachable_pub)`）。
- **SQL 注入防护**：所有进入 SQL 文本的调用方输入必须经过白名单标识符校验或转义；
  `build_insert_sql_chunks` 是构造 INSERT 的唯一入口，新增写入路径必须复用它，
  禁止手工拼接 SQL。
- **资源上界**：批量行数 / 字节、in-flight 并发、查询行数、响应字节、关闭超时在构建期
  `validate` fail-fast，不得静默 clamp，不得引入运行期无界增长。
- 凭据（user / password）只能从环境变量或构建器注入，`Debug` 输出脱敏，禁止写入 TOML 明文。
- 远程端点强制 HTTPS / WSS；明文 HTTP / WS 仅允许 loopback 开发端点。
- async 代码使用 tokio，禁止阻塞 I/O。
- MSRV 为 `1.85`，edition 2021（与 `Cargo.toml` 声明一致，不要使用更新的语言特性）。

## 提交前自检清单

- [ ] `cargo fmt --all -- --check` 通过
- [ ] `cargo clippy --all-targets -- -D warnings` 通过
- [ ] `cargo test --all-targets` 通过
- [ ] `cargo package --no-verify --allow-dirty` 通过
- [ ] 新增 `pub` 项都有中文 `///` 文档
- [ ] 文档中无「可独立发布」/ crates.io / docs.rs 表述
- [ ] 新增写入路径复用 `build_insert_sql_chunks`，未手工拼接 SQL
- [ ] 新增资源参数已在构建期校验并受 `HARD_MAX_*` 约束
- [ ] 未引入内部 crate 依赖（零内部耦合）
