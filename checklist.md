# taosx checklist

本文件只复述本仓已有门禁，不新增义务。命令差异以 [README.md](README.md)「门禁」（若有）与 `.github/workflows/ci.yml` 为准。`[x]` 表示本轮已在 crate 根目录跑过且退出码为 0。

来源：本仓 `.github/workflows/ci.yml`；`cargo package` 取工作区 P0 默认

## P0

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo test --workspace --all-features`
- [ ] `cargo package --no-verify`

## 说明

- 从 crate 根目录运行；执行前核对工作树归属。
- `cargo test` 含 doctest；不要用会漏 doctest 的 `--all-targets` 替代（本仓 README / CI 已写明变体的除外）。
- 不执行 `cargo publish`。README / rustdoc / 元数据不承诺 crates.io 或 docs.rs。
- 工作区级检查在元仓库执行，见工作区 `README.md`「质量门禁」。
- 机器全绿不代替已登记的人审，也不产生合入权。
