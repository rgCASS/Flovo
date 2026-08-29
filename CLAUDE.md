# Flovo 项目上下文

## 当前状态

- 仓库骨架已初始化。
- 许可证：Apache-2.0，版权归属 `rgCASS` 组织。
- 分支结构：`main` 为稳定基线，`dev` 用于后续开发；功能分支从 `dev` 创建。
- 当前任务：FL-01（仓库骨架初始化）。

## 文件说明

- `README.md`：项目名称、定位与后续文档占位。
- `LICENSE`：Apache-2.0 官方标准全文。
- `.gitignore`：环境变量、日志、Rust 构建产物、密钥、编辑器和系统文件，以及本地任务元数据。

## 后续任务指引

- FL-02：在 `dev` 分支上建立项目基础配置与开发环境。
- FL-03：完善 README，包括使用方式、架构与贡献说明。
- FL-04：继续实现后续功能，并从 `dev` 创建对应功能分支。

## 依赖安装

当前骨架不包含 Rust crate 或运行时依赖。开始 FL-02 后，请按项目配置安装 Rust stable 工具链，并使用 Cargo 管理依赖。

## 验证命令与结果

执行以下命令进行仓库基线检查：

```bash
git log --oneline -1
git branch -r
git status
git check-ignore target/ .env .env.local logs/example.log secret.pem secret.key id_rsa
rg -F "Flovo — a JSON-driven async streaming workflow engine in Rust" README.md
```

预期结果：

- `git log --oneline -1` 显示 `chore: init flovo repository`。
- `git branch -r` 显示 `origin/main` 与 `origin/dev`。
- `git status` 显示工作区干净，且本地 `main` 与 `dev` 指向同一提交并与远端同步。
- `git check-ignore` 为所有列出的本地文件打印匹配路径。
- `rg` 返回 README 中的项目定位句。

FL-01 本地文件检查结果：上述忽略规则、许可证全文和 README 定位句已通过静态检查；远端分支与提交同步状态需在完成 GitHub 推送后复核。

## 异常处理

- SSH 推送权限失败：确认 `ssh -T git@github.com` 可认证，检查当前 SSH key 是否具备 `rgCASS/Flovo` 写权限，并核对 `origin` 地址。
- 远端分支已存在且历史不一致：先执行 `git fetch origin` 检查差异，禁止直接强制推送；根据远端历史决定合并或请仓库维护者清理远端。
- `git check-ignore` 无输出：确认命令在仓库根目录执行，并检查 `.gitignore` 文件名和规则是否被其他配置覆盖。
