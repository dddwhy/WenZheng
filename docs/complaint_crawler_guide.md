# 组织机构投诉数据爬取系统使用指南

本文档介绍如何使用自动化爬取系统来获取组织机构的投诉数据并存储到数据库中。

## 系统概述

该系统可以：
1. 从数据库中读取组织机构信息
2. 根据组织机构ID抓取相应的投诉数据
3. 将投诉数据存储到数据库中
4. 支持定时执行任务，实现自动化数据采集

## 前提条件

使用本系统前，请确保：
1. 已安装所有依赖库：`pip install -r requirements.txt`
2. PostgreSQL数据库已正确配置
3. 组织机构数据已经爬取并存入数据库

## 数据库配置

系统使用PostgreSQL数据库存储数据。请确保在 `config/config.yaml` 中正确配置数据库连接信息：

```yaml
database:
  postgres:
    host: localhost
    port: 5432
    database: gov_complaint_data
    user: your_username
    password: "your_password"
```

## 使用方法

### 测试数据库连接

首先，您应该测试数据库连接是否正常：

```bash
# 测试数据库连接和表结构
python src/auto_complaint_crawler.py --test-db

# 使用单独的测试脚本进行更详细的测试
python src/test_db_storage.py --verify-only

# 测试处理特定样本文件并存储到数据库
python src/test_db_storage.py --sample data/complaints/example.json --create-table
```

### 直接执行爬取任务

您可以使用`auto_complaint_crawler.py`脚本直接执行爬取任务：

```bash
# 爬取所有区县级(level=3)组织机构的投诉数据
python src/auto_complaint_crawler.py --level 3

# 爬取指定类型的组织机构投诉数据
python src/auto_complaint_crawler.py --types DEPARTMENT,AREA

# 限制爬取数量（例如只爬取10个组织机构）
python src/auto_complaint_crawler.py --level 3 --limit 10

# 每个组织机构只爬取前5页数据
python src/auto_complaint_crawler.py --level 3 --pages 5

# 设置每页数据量（默认为20）
python src/auto_complaint_crawler.py --level 3 --page-size 30

# 指定保存目录
python src/auto_complaint_crawler.py --level 3 --save-dir data/my_complaints
```

### 设置定时爬取任务

您可以使用`schedule_crawler.py`脚本设置定时爬取任务：

```bash
# 每天凌晨2:30执行爬取任务
python src/schedule_crawler.py --time "02:30" --level 3

# 每12小时执行一次爬取任务
python src/schedule_crawler.py --interval 12 --level 3

# 设置任务并立即执行一次
python src/schedule_crawler.py --interval 24 --level 3 --now
```

### 作为后台服务运行

在生产环境中，您可能希望将爬虫作为后台服务运行。在Linux系统上，您可以使用以下命令：

```bash
# 使用nohup在后台运行定时任务
nohup python src/schedule_crawler.py --interval 24 --level 3 > logs/crawler_nohup.log 2>&1 &

# 或者使用screen
screen -S crawler
python src/schedule_crawler.py --interval 24 --level 3
# 按Ctrl+A,D分离screen会话
```

## 数据存储和表结构

爬取的数据会经过处理后存储到PostgreSQL数据库中。主要涉及以下表：

### 组织机构表 (organizations)

存储组织机构信息的表，包含以下主要字段：
- `id`: 主键
- `org_id`: 原始组织ID
- `name`: 组织名称
- `parent_id`: 父级组织ID
- `path`: 组织路径
- `type`: 组织类型
- `level`: 组织级别

### 投诉表 (complaints)

存储投诉信息的表，包含以下主要字段：
- `id`: 主键
- `complaint_id`: 投诉ID（唯一）
- `title`: 投诉标题
- `content`: 投诉内容
- `created_at`: 创建时间
- `updated_at`: 更新时间
- `status`: 处理状态
- `reply_status`: 回复状态
- `organization_id`: 组织ID
- `organization_name`: 组织名称
- `category`: 投诉分类
- `source`: 来源
- `raw_data`: 原始数据（JSONB格式）

## 故障排除

### 数据库连接问题

如果遇到数据库连接问题，请检查：
1. 数据库服务是否正常运行
2. 数据库配置是否正确（用户名、密码、主机、端口等）
3. 防火墙是否允许连接

可以使用以下命令测试数据库连接：
```bash
python src/test_db_storage.py --verify-only
```

### API请求问题

如果遇到API请求问题，请检查：
1. API接口是否可访问
2. 请求参数是否正确
3. 是否需要设置代理
4. IP是否被限制（可能需要降低请求频率）

### 数据存储问题

如果数据爬取成功但存储失败，请检查：
1. 表结构是否正确
2. 数据格式是否符合预期
3. 日志中是否有详细错误信息

使用以下命令测试数据存储功能：
```bash
python src/test_db_storage.py --sample 您的样本文件.json --create-table
```

## 日志和监控

系统会自动记录日志到`logs`目录。您可以通过查看日志文件来监控爬虫运行状态：

```bash
# 查看最新的日志
tail -f logs/crawler.log
```

## 扩展功能

您可以通过修改代码扩展以下功能：
1. 添加邮件或消息推送通知
2. 实现爬取失败的自动重试机制
3. 增加更多数据分析功能
4. 优化爬取策略，提高效率和成功率 