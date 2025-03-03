# 政府机构数据抓取项目

## 项目说明

这是一个用于抓取和存储政府机构数据的项目。主要功能是从API获取省级、市州和区县政府机构的组织结构数据，并将其存储到PostgreSQL数据库中。

## 环境准备

1. 安装 PostgreSQL 数据库
2. 安装 Python 3.8+
3. 安装依赖包：`pip install -r requirements.txt`

## 项目结构

```
├── config/
│   └── config.yaml       # 配置文件
├── src/
│   ├── crawlers/         # 爬虫模块
│   ├── db/               # 数据库操作模块
│   │   └── init_db.py    # 数据库初始化脚本
│   ├── models/           # 数据模型
│   ├── services/         # 业务逻辑服务
│   ├── utils/            # 工具函数
│   └── sample_data.py    # 示例数据抓取脚本
├── data/
│   └── samples/          # 保存的示例数据
└── logs/                 # 日志目录
```

## 使用方法

### 第一步：初始化数据库

运行以下命令初始化数据库、用户和表结构：

```
python -m src.db.init_db
```

### 第二步：抓取数据

运行示例数据抓取脚本：

```
python -m src.sample_data
```

抓取的数据将保存在 `./data/samples/` 目录下，采用JSON格式存储。

## 数据分析

抓取数据后，可以分析JSON结构，然后进行后续的数据库设计和数据存储操作。 