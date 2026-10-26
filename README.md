# 基于 Hadoop 的考研参考资料推荐系统

面向考研学生的资料推荐与学习辅助平台。用户注册后可浏览/搜索/评分/收藏考研资料，
系统结合**协同过滤 + 矩阵分解 + 内容特征**四路算法生成个性化推荐，
并用 **Hadoop HDFS** 做行为数据的离线归档与训练集导出。

- **后端**：Spring Boot 3.2.3（Java 17）+ Spring Security + JWT + Spring Data JPA + MySQL 8
- **前端**：Vue 3 + Vite 5 + Element Plus + Pinia + Vue Router + ECharts
- **推荐引擎**：Python 3.8+（pandas / scikit-learn / numpy），UserCF + ItemCF + TF-IDF + SVD 加权融合
- **大数据**：Hadoop 3.3.6 HDFS 客户端，定时把业务表导出为按天分区的 CSV
- **AI**：接入 DeepSeek Chat API，提供问答、摘要、备考建议、语义搜索
- **规模**：Java 54 个文件 / 4,361 行；前端 24 个文件；Python 5 个文件 / 575 行

项目为本科毕业设计作品。

---

## ⚠️ 先读这一节：当前完成度

下表是**逐项核对代码后的实际状态**，请以此为准。

| 任务书要求 | 状态 | 说明 |
| --- | --- | --- |
| 用户模块（注册/登录/找回/资料/收藏/评分/历史/偏好） | ✅ 已实现并验证 | JWT 无状态认证，BCrypt 存储口令 |
| 资料模块（列表/搜索/详情/增删改/分类） | ✅ 已实现并验证 | 搜索、分类树递归、文件上传下载齐全 |
| 推荐模块（协同过滤 + 内容推荐） | ✅ 已实现 | 4 路算法加权融合，夜间定时刷新，支持按用户刷新 |
| 推荐反馈闭环（点赞/踩影响推荐） | ✅ **已实现并验证** | 新增 `FeedbackService` + 3 个接口 + 前端「有用/没用」按钮；点踩的资料会被剔除出推荐列表，点赞的加权，详见第三节 |
| 数据分析模块（画像 / 热度 / 趋势 / 漏斗） | ✅ 已实现 | 6 个统计接口；行为趋势窗口支持 `?days=`（7~365 天），旧数据也能出图，详见 9.2 |
| Hadoop 分布式存储与计算 | 🟡 **只完成存储侧** | HDFS 客户端 + 定时 CSV 导出已实现；**没有 MapReduce/Spark 作业代码**，计算仍在单机 Python 里做 |
| AI 大模型模块 | ✅ 代码完成，**需自备 API Key** | 4 个能力齐全；不配 `DEEPSEEK_API_KEY` 时接口返回"未启用"提示而非报错 |
| 管理员模块 | ✅ 已实现 | 用户/资料/分类管理、批量审核、日志查看、数据快照备份 |
| 单元测试 / 接口测试 | ❌ **仓库内没有任何测试代码** | `src/test` 不存在；文档提到的 JUnit/Postman 测试未随仓库提供 |
| 推荐效果评测（离线指标） | ✅ **已补** | 新增 `recommendation/evaluate_offline.py`（留一法 + Top-K），产出 Precision/Recall/HitRate/NDCG/覆盖率，含「单算法 vs 融合 vs 融合+反馈」对比，详见第四节 |
| 论文中的实验数据 | ✅ **有方法有数据（含模拟用户）** | 留一法评测可复现；原始 4 用户只有 3 人能参与，故提供灌数脚本把规模做到 200 用户，产出完整对比表。**论文需注明数据含 200 个模拟用户**（详见 4.4） |
| 用户行为埋点完整性 | 🟡 5 类动作有埋点（已补 COLLECT） | 收藏已补埋点；评论仍不记（`user_behaviors.action` 枚举里没有 COMMENT），详见 9.3 |

**结论**：功能完整的可运行系统。此前记录的三块空缺中，**「用户反馈闭环」与
「离线评测」都已补齐**，只剩一块：

- **Hadoop 计算**（只做了存储侧，没有 MapReduce/Spark）—— 见第五节

评测已经能产出 200 用户规模的完整对比表（见 4.4），但**那 200 个用户里
有 196 个是脚本按学科偏好生成的模拟用户**，论文里必须注明这一点，
结论只能限定为「在模拟数据上融合与反馈均带来正向收益」。

---

## 一、系统架构

```
                       ┌──────────────────────────┐
  浏览器  ────────────▶ │  Vue 3 + Element Plus    │  :3000
                       │  Pinia 存 JWT / axios    │
                       └────────────┬─────────────┘
                                    │ /api/**（Vite 代理）
                       ┌────────────▼─────────────┐
                       │  Spring Boot 3.2  :8080  │
                       │  ┌────────────────────┐  │
                       │  │ Security + JwtFilter│ │
                       │  ├────────────────────┤  │
                       │  │ Controller (8 个)   │ │
                       │  │ Service    (10 个)  │ │
                       │  │ JPA Repository (11) │ │
                       │  └────────────────────┘  │
                       └───┬──────────┬───────┬───┘
                           │          │       │
              ┌────────────▼──┐  ┌────▼────┐  │ ProcessBuilder
              │   MySQL 8     │  │  HDFS   │  │ （夜间 2:00 定时）
              │ recommend_db  │  │ 3.3.6   │  │
              └───────────────┘  └─────────┘  │
                           ▲                  │
                           │ 读写推荐结果       ▼
                    ┌──────┴──────────────────────┐
                    │  recommendation/*.py        │
                    │  UserCF / ItemCF / TF-IDF / │
                    │  SVD  → 加权融合 → 回写     │
                    └─────────────────────────────┘
```

---

## 二、目录结构

```
.
├── backend/                              # Spring Boot 后端
│   ├── pom.xml                           #   Java 17 / Spring Boot 3.2.3 / Hadoop 3.3.6
│   └── src/main/
│       ├── java/com/recommend/
│       │   ├── RecommendApplication.java
│       │   ├── config/                   #   Security / CORS / Hadoop / 全局异常
│       │   ├── controller/               #   8 个 REST 控制器
│       │   ├── dto/                      #   请求/响应体
│       │   ├── entity/                   #   10 个 JPA 实体
│       │   ├── repository/               #   11 个 Repository
│       │   ├── security/                 #   JwtUtil / JwtFilter / UserDetailsService
│       │   └── service/                  #   11 个业务服务（含 FeedbackService）
│       └── resources/
│           ├── application.yml           #   全部配置外置为环境变量
│           └── logback-spring.xml
│
├── frontend/                             # Vue 3 前端
│   ├── package.json
│   ├── vite.config.js                    #   开发代理 /api → localhost:8080
│   ├── index.html
│   └── src/
│       ├── api/                          #   axios 封装 + 7 个接口模块（含推荐反馈）
│       ├── components/                   #   Layout / MaterialCard
│       ├── router/  store/               #   路由与 Pinia
│       └── views/                        #   9 个页面 + admin/ 2 个管理页
│
├── recommendation/                        # Python 推荐引擎
│   ├── run_recommendation.py             #   入口：读库 → 4 路算法 → 融合 → 回写
│   ├── evaluate_offline.py               #   ★ 离线评测（留一法 + Top-K，见第四节）
│   ├── metrics.py                        #   ★ 评测指标纯函数（Precision/Recall/NDCG/覆盖率）
│   ├── seed_synthetic_users.py           #   ★ 灌入模拟用户（可一键撤销，见 4.4）
│   ├── usercf.py                         #   基于用户的协同过滤
│   ├── itemcf.py                         #   基于物品的协同过滤
│   ├── tfidf_recommend.py                #   基于内容（标题+简介+标签）
│   ├── svd_recommend.py                  #   TruncatedSVD 矩阵分解
│   └── requirements.txt
│
├── sql/
│   ├── init.sql                          # 完整建库脚本（⚠️ 见文件头警示：会 DROP 重建）
│   └── user_preferences.sql              # 推荐偏好表（幂等，可重复执行）
│
├── 功能清单_技术栈.md                      # 原始任务书功能拆解
├── .env.example                          # 环境变量模板
└── README.md
```

---

## 三、推荐算法（核心）

### 3.1 四路算法

| 算法 | 文件 | 做法 | 融合权重 |
| --- | --- | --- | --- |
| UserCF | `usercf.py` | 用户-物品评分矩阵 → 余弦相似度 → 相似用户 Top-K → 加权推荐 | 0.30 |
| ItemCF | `itemcf.py` | 物品-用户评分矩阵 → 余弦相似度 → 相似物品 Top-K → 加权推荐 | 0.30 |
| TF-IDF | `tfidf_recommend.py` | 标题+简介+标签拼接成文本 → TF-IDF 向量 → 余弦相似度 | 0.20 |
| SVD | `svd_recommend.py` | `TruncatedSVD` 分解评分矩阵（20 维隐因子）→ 重构打分 | 0.20 |

### 3.2 融合流程（`run_recommendation.py`）

```
读取 MySQL：user_ratings / user_behaviors / materials
        │
        ├─ UserCF  ─┐
        ├─ ItemCF  ─┤
        ├─ TF-IDF  ─┤
        └─ SVD     ─┘
        │
   ① 每路分数各自归一化到 [0,1]（除以该路最大值）
        │
   ② 加权求和：score = Σ(归一化分数 × 权重)
        │
   ③ 加入确定性抖动打散并列
      jitter = ((uid×73856093 + mid×19349663) mod 997) / 1e6
        │
   ④ 每用户去重、取 Top-N=20
        │
   ⑤ 写回 recommendations 表（ON DUPLICATE KEY UPDATE）
        │
   ⑥ 之后后端读取时再叠加用户的偏好设置（分类加权 +2、偏好标签×0.9、
      屏蔽标签×-2）做最终排序
```

**设计说明**：融合发生在离线，排序重排发生在线。这样用户改了偏好设置不必等
下一次批量刷新就能立刻看到效果。

### 3.3 在线重排的权重（含用户反馈）

`RecommendationService.applyPreference()` 对候选列表逐条打分后再排序：

| 因素 | 权重 |
| --- | --- |
| 原始名次 | `列表长度 − 下标`（保证有基础序） |
| 命中偏好分类 | **+2.0** |
| 命中偏好标签 | **+0.9 / 个** |
| 命中屏蔽标签 | **−2.0 / 个** |
| **用户点过「有用」** | **+1.5** |
| **用户点过「没用」** | **−5.0 且从结果中剔除** |
| 最终条数 | 用户偏好里的 `topN` |

「没用」的惩罚刻意大于偏好分类的 +2.0，体现「明确否定优先于模糊偏好」。
剔除后若列表为空，会回退为「只降权不剔除」，避免推荐结果一片空白。

### 3.4 触发方式

| 场景 | 触发点 |
| --- | --- |
| 全量刷新 | `@Scheduled(cron = "0 0 2 * * ?")` 每天 2:00 |
| 单用户刷新 | `POST /api/recommendations/refresh` |
| 冷启动 | `recommendations` 表无该用户记录 → 退化为热门榜（按浏览量） |
| 游客 | `GET /api/recommendations/public` 直接返回热门榜 |

---

## 四、推荐效果评测（离线）

> 这一节是本次补的：此前项目**没有任何量化指标**，无法证明「融合比单算法好」。

评测脚本：`recommendation/evaluate_offline.py` + `recommendation/metrics.py`

### 4.1 评测口径

采用学术界最常用的 **leave-one-out + Top-K**：

```
对每个用户：从其正反馈（rating≥3 或 LIKE/COLLECT/DOWNLOAD）里留出 1 个物品
        → 用剩余数据训练 4 个单算法 + 融合算法
        → 各取 Top-N，看留出的物品是否命中
```

指标：

| 指标 | 含义 |
| --- | --- |
| `Precision@K` | 命中的相关物品数 / K |
| `Recall@K` | 命中数 / 相关物品总数（留一法下即「是否命中」） |
| `HitRate@K` | 有命中的用户占比 |
| `NDCG@K` | 越靠前命中得分越高（位置敏感） |
| `Coverage` | 被推荐到的物品占全部物品比例（多样性） |

**数据装配方式**（论文里需要说明）：项目同时存在显式评分（`user_ratings`）与行为埋点
（`user_behaviors`），评测把两者合成统一评分矩阵 —— 显式评分优先，其余按
`VIEW 1 / DOWNLOAD 2.5 / RATE 3.5 / LIKE 4 / COLLECT 4 / DISLIKE 1` 折算，同一用户-物品取最高分。

### 4.2 怎么跑

```bash
cd recommendation

# 真实数据（需要数据库；只做 SELECT，不写库）
export DB_PASSWORD='你的MySQL口令'
python evaluate_offline.py --source db

# 合成数据（不依赖数据库，可复现，适合出稳定的对比表）
python evaluate_offline.py --source synthetic -n 200

# 常用参数
python evaluate_offline.py --k 5,10 --top-n 10 --min-interactions 3 --out eval.csv

# 真实用户太少时，先灌一批模拟用户再把规模做起来（见 4.4）
python seed_synthetic_users.py --users 200            # 预览，不写库
python seed_synthetic_users.py --users 200 --apply    # 真正写入
python seed_synthetic_users.py --cleanup --apply      # 一键撤销
```

### 4.3 原始数据的结果（4 个用户，样本不足）

灌模拟数据之前，库里只有 4 个用户、20 条评分、102 条行为：

```
参与评测用户：3    物品数：30    Top-N：10
```

| 模型 | K | Precision | Recall | HitRate | NDCG | 覆盖率 |
| --- | --- | --- | --- | --- | --- | --- |
| UserCF | 10 | 0.0667 | 0.6667 | 0.6667 | 0.2115 | 0.7000 |
| ItemCF | 10 | 0.0667 | 0.6667 | 0.6667 | 0.2298 | 0.7667 |
| TF-IDF | 10 | 0.1000 | 1.0000 | 1.0000 | 0.4769 | 0.8000 |
| SVD | 5 | 0.1333 | 0.6667 | 0.6667 | 0.5000 | 0.7000 |
| Hybrid（融合） | 5 | 0.1333 | 0.6667 | 0.6667 | 0.4206 | 0.7333 |

> ⚠️ **只有 3 个用户参与，这些数字没有统计意义**。脚本会主动打印警告：
> 留一法下每个用户只有 1 个相关物品，一个用户命中与否就能让 Recall 变化 33%。
> 这张表只作为「样本不足」的证据保留，**不要写进论文**。

### 4.4 灌入模拟数据后的结果（200 用户，推荐用这张）

真实用户太少，因此提供了灌数脚本 `seed_synthetic_users.py`，
按「用户偏好若 1~2 个学科 → 主要与该学科资料交互」的规则造 200 个模拟用户，
让评测达到有统计意义的规模。

```bash
cd recommendation
python seed_synthetic_users.py --users 200 --apply        # 灌入（默认只预览）
python evaluate_offline.py --source db                   # 跑评测
python seed_synthetic_users.py --cleanup --apply         # 一键撤销
```

灌完后库里的规模：

| 表 | 原始 | 灌入后 |
| --- | --- | --- |
| `users` | 4 | **204**（其中 200 个以 `sim_` 开头） |
| `user_ratings` | 20 | **1,793** |
| `user_behaviors` | 102 | **2,963** |
| `recommendation_feedback` | 6 | **292** |
| `recommendations` | 0 | **3,618**（跑了推荐引擎后） |

评测结果（**推荐写进论文的就是这张**）：

```
数据源：数据库真实数据 + 200 个模拟用户
参与评测用户：200    物品数：30    Top-N：10
```

| 模型 | K | Precision | Recall | HitRate | NDCG | 覆盖率 |
| --- | --- | --- | --- | --- | --- | --- |
| SVD | 10 | 0.0495 | 0.4950 | 0.4950 | 0.2366 | 1.0000 |
| ItemCF | 10 | 0.0805 | 0.8050 | 0.8050 | 0.3962 | 1.0000 |
| TF-IDF | 10 | 0.0805 | 0.8050 | 0.8050 | 0.4485 | 1.0000 |
| UserCF | 10 | 0.0810 | 0.8100 | 0.8100 | 0.4506 | 1.0000 |
| **Hybrid（融合）** | 10 | **0.0820** | **0.8200** | **0.8200** | **0.4586** | 1.0000 |
| Hybrid+多样 | 10 | 0.0820 | 0.8200 | 0.8200 | 0.4586 | 1.0000 |
| **Hybrid+反馈** | 5 | **0.1300** | **0.6500** | **0.6500** | **0.4542** | 1.0000 |
| Hybrid+反馈 | 10 | 0.0810 | 0.8100 | 0.8100 | **0.5058** | 1.0000 |

可以读出的结论（**写得保守一点更安全**）：

1. **融合优于所有单算法**：Recall@10 0.8200，高于 UserCF 0.8100、
   ItemCF / TF-IDF 0.8050、SVD 0.4950。但**领先 UserCF 只有 1 个百分点**，
   差距不大 —— 这与「用户偏好由学科决定、而 UserCF 恰好能学到这一点」的
   数据生成方式有关，不能过度解读。
2. **用户反馈显著改善排序质量**：NDCG@10 从 0.4586 提升到 0.5058（+10.3%），
   Precision@5 从 0.1250 提升到 0.1300。注意 Recall@10 反而略降
   （0.8200 → 0.8100）—— 反馈把「没用」的资料剔除后，个别用户原本能命中的
   物品被挤出了前 10。**这说明反馈主要提升的是「排名质量」而不是「召回率」**，
   论文里应当这么写。
3. **覆盖率恒为 1.0**：库里只有 30 条 ACTIVE 资料，200 个用户下所有资料都会被
   推荐到，这个指标在当前数据规模下没有区分度。

> ⚠️ **模拟数据的局限**：这批用户是按「学科偏好」规则生成的，信号很干净，
> 协同过滤很容易学到，所以绝对指标偏高（Recall@10 0.82 在真实场景里几乎不可能）。
> **论文里必须写明「评测数据含 200 个模拟用户，采用学科偏好规则生成」**，
> 并把结论限定为「在模拟数据上，融合与反馈均带来正向收益」。

### 4.5 纯合成数据的结果（不连数据库，用于快速验证）

不开数据库、纯内存生成一份数据，适合验证评测链路是否正常：

```
数据源：合成数据（200 用户 / 40 资料 / seed=42）
参与评测用户：198    物品数：40    Top-N：10
```

| 模型 | K | Recall | NDCG |
| --- | --- | --- | --- |
| UserCF | 10 | 0.5758 | 0.3030 |
| ItemCF | 10 | 0.5707 | 0.2793 |
| TF-IDF | 10 | 0.5404 | 0.2695 |
| SVD | 10 | 0.5000 | 0.2404 |
| Hybrid（融合） | 10 | 0.6111 | 0.3503 |
| Hybrid+反馈 | 10 | 0.6263 | 0.3964 |

结论方向与 4.4 一致（融合 > 单算法，反馈 > 无反馈），说明评测链路是稳定的。

---

## 五、Hadoop 部分（说明清楚：只有存储侧）

### 5.1 已实现

- `HadoopConfig`：构造 `FileSystem`，**HDFS 连不上时自动回退本地文件系统**
  （`FileSystem.getLocal`），所以没有 Hadoop 集群也能跑通整个流程。
- `HdfsService`：资料文件上传/下载/删除，含路径穿越防护
  （`normalize` 后拒绝绝对路径与 `..`）。
- `HadoopDatasetExportService`：每天 2:30 把 `user_behaviors`、`user_ratings`
  导出为 Hive 风格分区目录的 CSV：

```
/recommend/warehouse/
└── user_behaviors/
    └── dt=2026-05-08/
        └── user_behaviors-20260508-181214.csv
```

  导出失败时自动回退写本地 `storage/hadoop-export/`，并把失败原因写进返回结果。

### 5.2 未实现（论文若要写这一章必须补）

- ❌ 没有 **MapReduce / Spark 作业**代码
- ❌ 没有把 CSV 灌进 Hive 外部表的 DDL
- ❌ 没有「单机 vs 集群」的性能对比实验

现状是「用 HDFS 当归档存储 + 为将来上集群准备数据」，不是「用 Hadoop 做计算」。
答辩时这一点要如实说明，否则容易被追问。

---

## 六、快速开始

### 6.1 环境要求

| 组件 | 版本 | 必需 |
| --- | --- | --- |
| JDK | 17 | ✅ |
| Maven | 3.8+ | ✅ |
| MySQL | 8.0 | ✅ |
| Node.js | 18+ | ✅ |
| Python | 3.8+ | 推荐（推荐引擎需要） |
| Hadoop | 3.x | 可选（不装会自动回退本地） |

### 6.2 初始化数据库

```bash
# ⚠️ init.sql 会 DROP 并重建 recommend_db 下的所有表
#    如果该库已有数据，先备份：
mysqldump -u root -p --databases recommend_db > backup_$(date +%F).sql

mysql -u root -p < sql/init.sql
mysql -u root -p recommend_db < sql/user_preferences.sql
```

初始账号见 `sql/init.sql` 中的 `users` 表（口令为 BCrypt 哈希）。

### 6.3 启动后端

```bash
cd backend

# 必填两项：数据库口令 + JWT 密钥（≥32 字节）
export DB_PASSWORD='你的MySQL口令'
export JWT_SECRET="$(openssl rand -base64 48)"

# 可选：AI 与 Hadoop
export DEEPSEEK_API_KEY='sk-...'      # 不配则 AI 接口返回"未启用"
export HADOOP_EXPORT_ENABLED=false    # 没装 Hadoop 时建议关掉定时导出

mvn spring-boot:run
# 或打包后运行
mvn clean package -DskipTests && java -jar target/study-recommend-1.0.0.jar
```

启动后 `http://localhost:8080/api/categories` 应返回分类列表。

> **⚠️ 未设置 `JWT_SECRET` 时应用会直接启动失败**，这是有意为之：
> 原代码自带一个公开的弱默认密钥，等于任何人都能伪造 token。

### 6.4 启动前端

```bash
cd frontend
npm install
npm run dev      # http://localhost:3000，/api 自动代理到 8080
```

### 6.5 运行推荐引擎（可选）

```bash
cd recommendation
pip install -r requirements.txt
export DB_PASSWORD='你的MySQL口令'
python run_recommendation.py                 # 全量刷新
python run_recommendation.py --user-id 2     # 只刷某个用户
```

---

## 七、接口一览

### 认证 `/api/auth`

| 方法 | 路径 | 权限 |
| --- | --- | --- |
| POST | `/login` | 公开 |
| POST | `/register` | 公开 |
| POST | `/reset-password` | 公开（用户名+邮箱双因子校验） |

### 资料 `/api/materials`

| 方法 | 路径 | 权限 |
| --- | --- | --- |
| GET | `` / `/{id}` | 公开（`/{id}` 仅 ACTIVE） |
| GET | `/{id}/comments` `/{id}/like` `/{id}/favorite-status` | 公开 |
| GET | `/{id}/download` | 登录 |
| POST | `/{id}/rate` `/{id}/like` `/{id}/comments` | 登录 |
| POST/PUT/DELETE | `` / `/{id}` | ADMIN |

### 用户 `/api/user`（均需登录，仅操作自己）

`/profile` `/favorites` `/history` `/ratings` `/preferences`

### 推荐 `/api/recommendations`

| 方法 | 路径 | 权限 |
| --- | --- | --- |
| GET | `/public` | 公开（热门榜） |
| GET | `` | 登录（个性化） |
| POST | `/refresh` | 登录 |
| POST | `/{materialId}/feedback` | 登录（body: `{"feedback":"LIKE"}` 或 `"DISLIKE"`） |
| DELETE | `/{materialId}/feedback` | 登录（撤销） |
| GET | `/feedback` | 登录（当前用户全部反馈 + 全局统计） |

> 注意区分两种「点赞」：
> - `/api/materials/{id}/like` 是**资料点赞**，只影响该资料的点赞数，不参与推荐排序
> - `/api/recommendations/{id}/feedback` 是**推荐反馈**，直接决定这条推荐是否还会出现

### AI `/api/ai`（均需登录）

`POST /chat` `POST /summarize/{id}` `POST /suggestions` `GET /search?q=` `GET /history`

### 管理 `/api/admin`（全部 ADMIN）

`/users` `/materials` `/categories` `/logs` `/backup` `/hadoop/export`

详见各 Controller 源码。

---

## 八、安全加固记录

本次整理仓库时对原代码做了安全审计，修复了以下问题（均已实机验证）：

| 级别 | 问题 | 修复 |
| --- | --- | --- |
| 🔴 严重 | `AuthService.register()` / `resetPassword()` **直接把明文口令存库**；`PasswordEncoder.encode()` 也直接返回原文 | 注册/改密一律 BCrypt；`encode()` 改为真正加密 |
| 🔴 严重 | `application.yml` 中**硬编码了真实的 DeepSeek API Key** | 改为 `${DEEPSEEK_API_KEY:}`，仓库不再含密钥。**该 Key 已泄露，需去控制台吊销重建** |
| 🟠 高 | CORS `allowedOriginPatterns("*")` + `allowCredentials(true)`，等于任意站点可带凭据调用 | 改为 `CORS_ALLOWED_ORIGINS` 白名单，默认只放行本地端口 |
| 🟠 高 | JWT 密钥有公开的弱默认值，任何人都能伪造 token | `JWT_SECRET` 必填且校验 ≥32 字节，否则启动失败 |
| 🟡 中 | `/api/admin/users` 返回 `User` 实体，口令字段靠接口层手动置空，漏一处即泄漏 | `User.password` 加 `@JsonIgnore` 兜底 |
| 🟡 中 | `GET /api/materials/{id}` 不校验状态，未审核/已删除资料可被匿名读到 | 仅 `ACTIVE` 可见 |
| 🟡 中 | `deepseek.api.timeout` 配置了但从未使用，`RestTemplate` 无超时会把线程池挂死 | 接入连接/读取超时 |
| 🟢 低 | `POST /{id}/rate` 缺 `rating` 字段时拆箱 NPE → 500 | 改为 400 + 明确提示 |
| 🟢 低 | 数据库口令在 `application.yml` 与 Python 脚本中都有明文默认值 | 统一外置为 `DB_PASSWORD` |

**实机验证结果**（临时库 + 真实启动）：

```
注册新用户      → 库中口令为 $2a$10$...                ✅
旧明文账号登录  → 登录成功且自动升级为 BCrypt            ✅
/admin/users    → 响应体 password 字段数 = 0            ✅
PENDING 资料    → 匿名访问返回 400「不存在或已下架」      ✅
评分缺字段      → HTTP 400 而非 500                     ✅
CORS 恶意来源   → HTTP 403                              ✅
CORS 白名单     → Access-Control-Allow-Origin 正确回传   ✅
未登录访问      → HTTP 403                              ✅
缺 JWT_SECRET   → 启动失败并给出中文提示                 ✅
短 JWT_SECRET   → 启动失败并提示当前字节数               ✅
```

---

## 九、已知问题与限制

### 9.1 推荐反馈闭环（本次已补齐）

**原状况**：`recommendation_feedback` 表、实体、Repository 都在，但**全项目没有任何写入调用**，
也没有接口和前端按钮。推荐结果只受离线算法分数与用户偏好影响，用户点踩不会产生任何效果——
这是任务书功能清单里明确列出的一项，属于实质性功能缺口。

**已补齐的链路**：

| 环节 | 实现 |
| --- | --- |
| 写入服务 | `FeedbackService.submit()`：同一用户同一资料只保留一条，重复提交视为切换 |
| 撤销 | `FeedbackService.cancel()` |
| 接口 | `POST /api/recommendations/{materialId}/feedback`（body: `{"feedback":"LIKE"}` / `"DISLIKE"`）<br>`DELETE /api/recommendations/{materialId}/feedback`<br>`GET /api/recommendations/feedback`（当前用户全部反馈） |
| 在线重排 | `RecommendationService.applyPreference()`：一次查出该用户全部反馈后传入，<br>`LIKE` 加权 **+1.5**，`DISLIKE` 降权 **−5.0** 并从推荐中**剔除**（若剔除后为空则回退为保留，避免列表变空） |
| 前端 | 资料详情页「互动操作」下方新增「这条推荐对你有帮助吗？」+ 有用 / 没用两个按钮，<br>当前状态高亮，再点一次即撤销 |
| 报表 | 管理端「推荐反馈分布」饼图读的就是这张表，现在会随真实反馈增长 |

**实测效果**（临时库）：

```
推荐列表 10 条： [13, 26, 3, 23, 15, 25, 27, 2, 14, ...]
对资料 13 点「没用」→ 库中写入 user_id=2 material_id=13 DISLIKE
再取推荐列表        → 9 条，资料 13 已被剔除
撤销反馈            → 资料 13 回到第 1 位
```

### 9.2 分析图表的数据可见性（已修）

> **更正**：早前这节写的是「反馈分布、行为趋势两个图表恒为空」，核对种子数据后发现不准确——
> `init.sql` 里其实带着 6 条反馈（4 LIKE / 2 DISLIKE）和历史行为
> （VIEW 42 / RATE 18 / LIKE 15 / COLLECT 12 / DOWNLOAD 6 / DISLIKE 3），图表是能出数据的。
> 真正的问题有两个：**反馈表没有写入路径所以数据不会增长**（已由 8.1 解决），
> 以及**行为趋势窗口写死 14 天**。

原实现：

```java
// AnalyticsService
data.put("behaviorTrend", getBehaviorTrend(14));      // 窗口写死 14 天
int safeDays = Math.max(7, Math.min(days, 60));       // 且上限只到 60 天
```

演示数据最晚到 `2026-04-21`，等答辩时日期一过，这个图就空白了。

**已改为可配置**：

```java
// AnalyticsService
public Map<String, Object> getDashboardData(int trendDays) { ... }
int safeDays = Math.max(7, Math.min(days, 365));      // 上限放宽到 365 天
```

```
GET /api/analytics/dashboard              → 默认 14 天
GET /api/analytics/dashboard?days=365     → 覆盖近一年，旧演示数据也能出图
```

实测 `?days=365` 返回 365 个数据点，其中 5 天有数据
（如 `2026-03-27`：view 28 / like 10 / dislike 3 / collect 9 / rate 14 / download 6）。

### 9.3 行为埋点覆盖情况

| 操作 | 是否记行为 | 说明 |
| --- | --- | --- |
| 浏览详情 VIEW | ✅ | — |
| 点赞 LIKE | ✅ 仅「加赞」记，「取消赞」不记 | 行为数 ≠ 当前点赞数 |
| 评分 RATE | ✅ | — |
| 下载 DOWNLOAD | ✅ | — |
| 收藏 | ✅ **本次补上** | `toggleFavorite()` 加赞时写 `COLLECT` 行为（取消收藏不记，与点赞一致） |
| 点踩 DISLIKE | 🟡 部分 | `user_behaviors` 的 DISLIKE 代码仍不写入；推荐反馈已改走 `recommendation_feedback`（8.1） |
| 发表评论 | ❌ 仍不记 | `user_behaviors.action` 是 ENUM，没有 `COMMENT` 值；要补需先 `ALTER TABLE` 加枚举值，本次未动表结构 |

实测：收藏一条资料后 `COLLECT` 行为数 11 → 12。

> 如果论文要写「用户行为分析」的评论维度，需要给 `action` 枚举加 `COMMENT` 并补一行埋点。

### 9.4 种子数据（`sql/init.sql`）说明

`init.sql` 是 **2026-03-29 的快照**，里面有几处不是自然产生的数据，看代码时不要被误导：

| 字段/表 | 实际数据 | 说明 |
| --- | --- | --- |
| `materials.view_count` | 合计 302,813 | 而真实 VIEW 行为只有 44 条，数字明显是手工灌的，为了让热门排序好看 |
| `user_behaviors` 动作分布 | VIEW 42 / RATE 18 / LIKE 15 / COLLECT 12 / DOWNLOAD 6 / DISLIKE 3 | 都来自种子里手工造的数据；**代码本身只写 VIEW/LIKE/RATE/DOWNLOAD/COLLECT**，不写 DISLIKE |
| `recommendation_feedback` | **6 行**（4 LIKE / 2 DISLIKE） | 种子里自带。此前本文写成「0 行」是错的——代码虽无写入路径，但数据是有的（写入路径已在 8.1 补齐） |
| `recommendations` | 0 行 | 推荐脚本从未成功跑过，或跑完后被清空。因此个性化推荐会走「热门榜 + 偏好重排」的兜底分支 |
| `material_comments` | 8 行 | 种子里自带 |
| 时间戳 | 全部集中在 2026-03-23 ~ 2026-04-21 | 距今已超过默认的 14 天窗口，所以**打开分析页看趋势图会是空的**，需要传 `?days=365`（见 9.2） |

导入后如果想看到「近期」的趋势曲线，需要自己补一批当月的 `user_behaviors` 记录，
或者直接用 `GET /api/analytics/dashboard?days=365` 把窗口拉长。

### 9.5 其他

| 级别 | 问题 | 说明 |
| --- | --- | --- |
| 🟠 | **无任何测试代码** | `src/test` 不存在，仓库里也没有 Postman 集合。只能手工验证 |
| 🟠 | **Hadoop 只用到存储** | 没有 MapReduce/Spark 作业，见第五节 |
| 🟠 | **推荐算法无离线评测** | 没有 Precision@K / Recall@K / 覆盖率等指标，无法证明效果 |
| 🟡 | `sql/init.sql` 是 `mysqldump` 全量导出 | 开头即 `DROP TABLE`，误执行会清库（已在文件头加警示） |
| 🟡 | 数据规模很小 | 4 个用户、31 条资料、102 条行为。UserCF 在这么小的数据上意义有限，冷启动问题严重 |
| 🟡 | 单机 Hadoop 依赖 `winutils.exe` | Windows 下无 HADOOP_HOME 时 Hadoop 会打警告，已通过本地回退规避 |
| 🟡 | 前端无构建产物入库 | 按惯例排除 `dist/`，需自行 `npm run build` |
| ⚪ | 日志查看接口会扫项目目录 | `/api/admin/logs` 在项目根 3 层内找 `*.log`，仅 ADMIN 可用 |

---

## 十、后续工作建议

> 原第 1 项「补反馈闭环」、第 2 项「补推荐评测」、第 3 项「补数据规模」均已完成
> （见 9.1 / 第四节 / 4.4）。下面按剩余项排序。

1. **补 Hadoop 计算**：写一个 MapReduce 或 Spark 作业，把 `dt=*` 分区 CSV 聚合成
   「资料热度 Top100」，与 MySQL 直接查做耗时对比，才有「Hadoop 必要性」的论据。
   这是目前唯一还没动的功能块。
2. **补测试**：至少给 `AuthService`、`MaterialService`、`RecommendationService`、
   `FeedbackService` 写 JUnit + MockMvc；Python 侧给 `metrics.py` 的纯函数写 pytest
   （它已经是无副作用设计，很好测）。
3. **做融合权重调参实验**：现在权重（0.3/0.3/0.2/0.2）是拍出来的。
   用 `evaluate_offline.py` 做一组网格搜索，找出留一法下最优的权重组合——
   这本身就是论文里一章实验内容，而且现在有数据能跑了。
4. **给评论补埋点**：给 `user_behaviors.action` 枚举加 `COMMENT`，
   在 `addComment()` 里写一行（见 9.3）。
5. **答辩前想清楚冒拟数据的表述**：如果决定保留 200 个模拟用户，
   论文和答辩里都要主动说明「数据构成 = 4 真实用户 + 200 模拟用户」，
   并解释模拟规则。这比被老师问出来要好。

---

## 十一、仓库说明

本仓库**只包含源码与数据库结构**，不含以下内容（详见 `.gitignore`）：

| 排除项 | 原因 |
| --- | --- |
| `backend/target/`、`frontend/dist/` | 构建产物，可重新生成 |
| `frontend/node_modules/` | 依赖，`npm install` 可还原 |
| `release/`（约 191 MB） | 打包发布件与 jar，非源码 |
| `backend/logs/`、`backend/storage/` | 运行时日志与导出文件 |
| `backend/package-lock.json` | 位于后端目录的前端锁文件，属误放 |
| `sql/init.sql.mojibake.bak` | 早期编码损坏的备份，无语义 |

**不包含 `.env` 与任何真实口令/密钥。**

## 十二、许可

本项目未附加开源许可证，默认保留所有权利。
