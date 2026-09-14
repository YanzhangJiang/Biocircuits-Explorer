# 瘦身验收报告 — `slim/single-user`

- 基线：`master` @ `8b0c6a9`（2026-09-14 single-user pivot 之后）
- 瘦身分支：`slim/single-user`，代码提交 8 个（`92bf7a9`…`85c801c`，每阶段一个，可逐个回滚）+ 本报告 1 个提交
- 研究栈备份：`research/ro-field` @ `8b0c6a9`（与基线完全相同，P5–P8 验证数值栈完整保留）
- 执行日期：2026-09-15（夜间自主执行）
- 度量口径：`git ls-files | xargs wc -l`（跟踪内容行数，含 JSON/锁文件）；测试数来自各套件自身汇总

## 1. 总量对比

| 指标 | 瘦身前 | 瘦身后 | 变化 |
|---|---:|---:|---:|
| 跟踪文件数 | 710 | 509 | −201 (−28%) |
| 跟踪行数 | 430,328 | 317,499 | −112,829 (−26%) |
| Julia 行数 (`.jl`) | 151,156 | 69,584 | −54% |
| Python 行数 | 26,825 | 21,413 | −20% |
| Markdown 行数 | 11,079 | 2,041 | −82% |
| YAML 行数 | 1,615 + 473 | 200 | −90% |
| HTTP 路由数 | 51 | 48 | −3 |
| Julia Manifest 数 | 7 | 3 | −4 |
| Julia 项目环境 | webapp / webapp_hpc / packaging / Bnc_julia | webapp / packaging / Bnc_julia | −1 |
| 引擎副本 | 2（Bnc_julia, Bnc_julia_headless） | 1 | −1 |
| CI 任务 | 5（含 Julia×2 矩阵、macOS×2、HPC、Docker workflow） | 4（Julia 1.12 单跑、macOS 单 runner） | Docker/HPC 删除 |
| 引擎测试文件 | 28 | 4 | −24 |
| 后端测试文件 | 90 | 78 | −12 |
| 仓库级 Python 测试文件 | 27 | 14 | −13 |
| knowledge/ 文件 | 37（11,757 行） | 8（1,648 行） | −86% 行 |
| `git diff --shortstat` | | | 249 files, +410 / −113,243 |

按目录（跟踪行数）：

| 目录 | 前 | 后 |
|---|---:|---:|
| Bnc_julia/src | 57,897 | 17,467 |
| Bnc_julia/test | 18,980 | 1,594 |
| webapp/src | 46,600 | 31,690 |
| webapp/test | 36,997 | 30,563 |
| webapp/scripts | 24,305 | 18,076 |
| webapp/public | 100,207 | 97,165 |
| knowledge | 11,757 | 1,648 |
| tests | 6,264 | 3,562 |
| schemas | 6,050 | 4,307 |
| deploy / slurm / webapp_hpc / Bnc_julia_headless | 1,033 / 675 / 4,028 / 45 | 0 / 0 / 0 / 0 |
| frontend-swift | 17,190 | 17,096 |

## 2. 逐阶段去除清单

| 提交 | 阶段 | 去除内容 | 规模 |
|---|---|---|---:|
| `92bf7a9` | 1 部署/云残留 | `deploy/`（Dockerfile、docker-compose+nginx+TLS、EC2 deploy.sh、rollback、image_reference、aws-runtime.env）、`slurm/`（9 个 sbatch/README）、`webapp_hpc/`、`Bnc_julia_headless/`、`.dockerignore`、Docker CI workflow、headless golden smoke、`Manifest-v1.10.toml`×2、`tests/test_build_image.py`、`tests/test_rewrite_rollback_config.py`；CI 收成 Julia 1.12 + 单 macOS runner；`set_version.sh` 及其测试去掉 HPC 项目 | 34 files, −9,469 |
| `8f6349c` | 2 服务层 | `observability.jl`（Prometheus 计数器/直方图/JSON 请求日志）、`/metrics` 路由、`X-Request-Id` 生成/回显、`X-Forwarded-For` client IP、路由层指标打点；裸 `/api/*` 兼容别名与 `legacy_alias` 字段（仅保留 `/api/v1`）；两槽并发闸 `SyncCapacityExceeded`/429/Retry-After（保留 422 工作预算）；EC2/auth 残留注释；对应测试（Prometheus、Request-ID、别名）删除，其余测试统一迁到 `/api/v1` | 17 files, −851 |
| `6870939` | 3 Helper 鉴权 | `chat_api.py`：bearer token（≥32）、nonce 长度规则、`/identity` 握手、preflight 头白名单、`ALLOW_UNAUTHENTICATED_LOOPBACK` 开关全部移除，只剩回环绑定 + 精确 Origin（POST 必须带 Origin，GET /health 允许本机无 Origin 探针）；Swift 侧删 per-launch bearer 生成/注入/`authenticatedRequest`/identity 探针；JS 客户端不再发 Authorization；`start.sh` 去掉 dev-only 标志 | 9 files, −258 |
| `f68b5a3` | 4 研究栈 | 引擎 `Bnc_julia/src/rop/ro_*.jl`×23 + 23 个 contract 测试 + `multi_input_ro_field_contract.jl`；后端 `webapp/src/ro_field_*.jl`×11 + 11 个 contract 测试；`schemas/ro-field*.json`×4 + `tests/test_ro_field*.py`×3 + fixtures；`/api/v1/ro_field`、`/ro_field/differential` 路由；`compute_ro_field` 作业类型（jobs.jl 中 12 处分支）；atlas SQLite 0.4/0.5 迁移与 RO 表/签名函数（~1,700 行）；`ro-field-demo.html` + 2 个 JS；campaign 合并/报告脚本；`rop_exports.jl` 245 行 RO 导出 | 103 files, −84,683 |
| `93ad7ff`+`85c801c` | 6 前端 | 与 `index.html` 字节相同的 `index.zh.html`（假 i18n）及 `sync-i18n`/`check-i18n-sync` 脚本与 CI 步骤（语言切换改用 `?lang=zh`）；第二套死 UI `classic.html` + `app.js`（仍调用已下线的裸 `/api/`）；落地页/zh 文案中的 Cloud Compute / Cognito / AWS Batch / 云端集群 宣传；相关测试断言 | 8 files, −1,264 |
| `baf5de7` | 7 流程层 | `knowledge/`：catalogs（modules/contracts/artifacts.yaml）、manifest.yaml、context-packs、11 个 module cards、architecture、api/schemas/workflow-execution/frontend-design 契约页、research、819 行 status、ADR 0003(AWS Batch)/0005/0006(RO-field)/template；替换为 ≤60 行 status 与短 README。`verify_repository.py` 650→425 行：删 Markdown 链接/heading 校验、CI 工具链×Docker 版本对齐、catalog/artifact 校验、notebook 输出检查、PyYAML 依赖；删 `tests/test_verify_repository.py`（测试“检查器”的测试）。README 269→80 行、PROJECT_SUMMARY 271→36 行。`.gitignore` 删 Cognito/AWS/deploy 条目和 tests/ 的“先全部忽略再逐个放行”陷阱 | 36 files, −10,862 |
| `00d0194` | 4b 研究脚本 | `webapp/scripts/synth/`（kd 优化器/CEGIS/S2 基准，已被 ParameterPlacer 取代）、latent-atlas 标签生成（gen_*_labels.jl、gen_splits、gen_phenotype_shards、merge_phenotype_shards、gen_curve_packets）、基准/性能脚本（bench_retrieve_verify、run_benchmark、profile_warmstart、heavy_profile）、一次性 demo/估算脚本（demo_*、est_d4、enumerate_count、extract_*、atlas_read、step5_analog）、`export_reference_facts.jl`、`design_chat.py`（旧 CLI 闭环）、`behavior_search.py`、`exact_search_baseline.py`、`nl_fidelity.py`、`compute_behavior_complexity_index.py` | 51 files, −5,877 |

## 3. 舍弃的过度工程（对应最初诊断）

| 诊断项 | 处置 |
|---|---|
| 为已取消的公网多租户 SaaS 保留的部署栈（EC2/nginx/TLS/compose/rollback） | 全部删除 |
| Slurm/HPC 双环境 + 第二份引擎 | 删除（引擎已有 `BNC_HEADLESS` 开关，无需副本） |
| Julia 1.10+1.12 双 Manifest、双矩阵 CI、双 macOS runner | 单版本、单 runner |
| Prometheus、JSON 请求日志、request-id、X-Forwarded-For | 删除 |
| 裸 `/api/*` “永久兼容别名” + sunset 计数 | 删除，仅 `/api/v1` |
| 429 两槽并发闸 | 删除（保留 422 工作预算与同步请求上下文） |
| loopback helper 的 bearer + nonce + /identity + preflight 白名单 | 降为回环 + 精确 Origin |
| P5–P8 验证数值研究栈住在产品仓库 | 移出产品分支，完整保留在 `research/ro-field` |
| 假 i18n（cp + diff -q 守护恒等） | 删除 |
| 第二套死 UI（classic） | 删除 |
| 落地页宣传 Cloud Compute / Cognito | 改为本地事实 |
| 7 级 authority order / 5 种 evidence state / 三套 catalog / 1000 词 evidence_boundary / 819 行 status | 压缩到 8 个文件 1,648 行 |
| docs-about-docs（verify_repository 的 Markdown 校验 + 测试检查器的测试） | 删除 |
| 退役研究脚本（synth、latent-atlas、benchmark、demo） | 删除 |

## 4. 保留但曾被点名的项（及原因）

| 项 | 原因 |
|---|---|
| `jobs.jl` 的 128 锁分片、LRU 时钟、manifest 协议、`user_sub` 匿名归属字段 | 与 8 个作业契约测试深度耦合，改动收益低风险高；`user_sub` 已固定为 anonymous |
| 前端 7×7 端口族、生命周期五元组 ticket、6 个 restore-only v1 节点 | 承担旧工作区文件迁移与刷新一致性，删除会级联到 workspace-v2 迁移测试 |
| Swift 侧 Workspace v2 解码器、手工镜像菜单 | 保留并简化了鉴权；菜单改造涉及 UI 行为，留作后续 |
| `benchmarks/rop_shape_control`（37k 行结果 JSON） | 是 `rop_shape_cat_benchmark.jl` 测试的冻结输入 |
| `tools/migration_parity`、`atlas_specs/`、atlas SQLite 维护脚本 | 引擎上游同步与 atlas 存储格式维护仍需要 |
| `__precompile__(false)` | 试过去掉：overlay 重定义上游方法，Julia 报 “Method overwriting is not permitted during precompilation”，已回退 |
| Reader（`webapp/scripts/reader`） | Design Agent 依赖，且有 CI 测试 |

## 5. 基础功能验收

### 5.1 真实 HTTP 冒烟（`start.sh` 启动 Julia 服务 + Design Chat helper，34 个调用）

同一脚本、同一请求体，瘦身前后各跑一次：

- 状态码一致：**33/34**。唯一差异 `GET /health`（无 Origin）403→200，是阶段 3 的预期改变（本机探针不再需要 bearer）。
- 规范化响应体一致：28/34；6 处差异全部良性：`index.html`（落地页文案改动）、`rop_cloud`/`rop_polyhedron`（随机采样点）、`job_status`（时间戳/路径已归一化后仍含运行相关字段；前后均为 `query_atlas`→`failed`，作业管线行为一致）、`chat_turn`（前后同为“LLM backend error 400”，无 key 情况一致）、`chat_noorigin`（同上预期改变）。

覆盖的核心功能：build_model → find_vertices → build_graph(qk/siso) → siso_paths/polyhedra/trajectory → behavior_families → rop_cloud → parameter_scan_1d/2d → rop_polyhedron → vertex_detail → SBML 导出/导入 → NetworkIR 校验 → placer_menu → design_search → 本地异步 job 提交/查询 → debug_logs → 错误路径（坏 JSON 400、未知路由 404）→ Design Chat health/turn/Origin 拒绝。

### 5.2 测试套件

| 套件 | 基线 | 瘦身后（最终 HEAD） |
|---|---|---|
| webapp Julia（`webapp/test/runtests.jl`） | 4,912 pass / 0 fail / 1 broken（预存 `@test_broken`） | 3,836 pass / 0 fail / 1 broken（同一预存项） |
| 引擎 golden-value（`BindingAndCatalysis golden-value suite`） | 327/327 | 327/327 |
| 引擎全套（`Bnc_julia/test/runtests.jl`） | 5,743 pass（含 RO contract 约 5,000） | 455 pass / 0 fail（RO contract 约 5,288 条随研究栈移出；golden 327/327 不变） |
| phenotyper | 35/35 | 35/35 |
| JS lint | 0 警告 | 0 警告 |
| JS 单元（`test:js`，49 文件） | 全过 | 全过 |
| Playwright e2e（28 项） | 空载未测；负载下 3 失败（超时/瞬时资源错误） | 空载单 worker **28/28**；负载下同样出现 2 个超时（偶发，与基线相同类型） |
| Python agent（chat_api / design_agent / target_compile） | 14+45+14 OK | 10+45+14 OK（删除的是 bearer/identity 用例） |
| 仓库级 Python（`tests/`） | 96 OK | 43 OK（删除的是 Docker/rollback/RO schema/verify_repository 自测） |
| `verify_repository.py --check` | PASS | PASS |
| Reader no-fabrication | 15/15 | 15/15 |
| packaging 运行时契约 | 5/5 | 5/5 |
| 版本资源发现 | 3/3 | 3/3 |
| macOS 单元测试（xcodebuild） | 未在基线跑 | **63 passed / 0 failed** |

测试数量下降均对应被删功能的测试，无“先禁用再删”的情况；每阶段提交时对应套件均为绿。

## 6. 发现并修复的回归

1. 删除 `classic.html` 后，`input_validation_contract.jl` 有一段读取该文件校验 cloud-samples 上限的断言——随 classic UI 一起删除（`85c801c`）。
2. 测试批量迁移到 `/api/v1` 时，三处 `haskey(API_ROUTES, "/api/…")`（内部路径）和两处“裸路径应 404”的断言被误改，已逐条恢复。

## 7. 未做 / 建议后续

- `jobs.jl` 简化（单锁、去 `user_sub`）与前端端口族/ticket 收敛：需要专门一轮，建议在合并后单独做。
- Swift 菜单改由 web 端节点清单驱动（消除手工镜像）。
- `benchmarks/` 的 37k 行结果 JSON 可移到 Git LFS 或压缩。
- e2e 在 CPU 满载时会超时（基线同样如此），CI 上建议 `--workers=1` 或提高超时。

## 8. 合并前操作建议

```bash
git checkout master
git merge --ff-only slim/single-user      # 8 个提交，线性
# research/ro-field 分支保留，不合并
```

本报告文件 `SLIM_REPORT.md` 可在合并前删除或移入 `knowledge/decisions/`。
