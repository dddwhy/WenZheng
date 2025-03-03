"""
数据库表结构定义
"""

# 组织机构表结构定义SQL
ORGANIZATION_SCHEMA = """
-- 组织机构表
CREATE TABLE IF NOT EXISTS organizations (
    id SERIAL PRIMARY KEY,                   -- 自增主键
    org_id INTEGER UNIQUE NOT NULL,          -- 机构原始ID
    name VARCHAR(255) NOT NULL,              -- 机构名称
    parent_id INTEGER,                       -- 父级机构ID
    path TEXT,                               -- 机构路径，以字符串形式存储，如 "1.2.3"
    "type" VARCHAR(50),                      -- 机构类型，如PG, CITY, AREA等
    ext JSONB,                               -- 扩展信息，使用PostgreSQL的JSONB类型
    has_children BOOLEAN DEFAULT FALSE,      -- 是否有子机构
    level INTEGER,                           -- 层级（省=1，市=2，区县=3，等）
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 创建普通索引
CREATE INDEX IF NOT EXISTS idx_organizations_path ON organizations (path);
CREATE INDEX IF NOT EXISTS idx_organizations_parent_id ON organizations (parent_id);
CREATE INDEX IF NOT EXISTS idx_organizations_type ON organizations ("type");
"""

# 线程（投诉）表结构定义SQL
THREAD_SCHEMA = """
-- 线程（投诉）表
CREATE TABLE IF NOT EXISTS complaints (
    id SERIAL PRIMARY KEY,                           -- 自增主键
    thread_id VARCHAR(255) UNIQUE NOT NULL,          -- 原始投诉ID
    title VARCHAR(500),                              -- 投诉标题
    content TEXT,                                    -- 投诉内容
    
    -- 组织机构相关
    assign_organization_id INTEGER,                  -- 分配的组织机构ID
    chosen_organization_id INTEGER,                  -- 选择的组织机构ID
    organization_name VARCHAR(255),                  -- 组织机构名称
    
    -- 处理状态相关
    handle_status VARCHAR(50),                       -- 处理状态 (HANDLING, NOT_HANDLE等)
    handle_status_real VARCHAR(50),                  -- 真实处理状态
    reply_status VARCHAR(50),                        -- 回复状态 (REPLIED, NOT_REPLY等)
    
    -- 时间相关
    created_at TIMESTAMP WITHOUT TIME ZONE,          -- 创建时间
    assign_at TIMESTAMP WITHOUT TIME ZONE,           -- 分配时间
    handle_at TIMESTAMP WITHOUT TIME ZONE,           -- 处理时间
    reply_at TIMESTAMP WITHOUT TIME ZONE,            -- 回复时间
    done_at TIMESTAMP WITHOUT TIME ZONE,             -- 办结时间
    deadline TIMESTAMP WITHOUT TIME ZONE,            -- 处理截止日期
    updated_at TIMESTAMP WITHOUT TIME ZONE,          -- 更新时间
    delete_at TIMESTAMP WITHOUT TIME ZONE,           -- 删除时间(0表示未删除)
    
    -- 标志字段
    expire_flag BOOLEAN,                             -- 是否过期
    warn_flag BOOLEAN DEFAULT FALSE,                 -- 是否警告
    apply_postpone_flag BOOLEAN DEFAULT FALSE,       -- 是否申请延期
    apply_satisfaction_flag BOOLEAN DEFAULT FALSE,   -- 是否申请满意度评价
    apply_transfer_flag BOOLEAN DEFAULT FALSE,       -- 是否申请转办
    can_feedback_flag BOOLEAN DEFAULT FALSE,         -- 是否可以反馈
    has_video INTEGER DEFAULT -1,                    -- 是否包含视频(-1:否, 1:是)
    satisfaction INTEGER DEFAULT -1,                 -- 满意度评价(-1:未评价)
    info_hidden INTEGER DEFAULT 0,                   -- 是否隐藏用户信息(0:否, 1:是)
    
    -- 来源相关
    source VARCHAR(50),                              -- 来源(WAP, PC, WECHAT_MP等)
    ip VARCHAR(50),                                  -- IP地址
    
    -- 用户相关
    username VARCHAR(255),                           -- 用户名
    passport_id VARCHAR(255),                        -- 通行证ID
    wechat_uid VARCHAR(255),                         -- 微信用户ID
    
    -- 其他字段
    area_id INTEGER DEFAULT -1,                      -- 区域ID
    field_id INTEGER,                                -- 领域ID
    field_name VARCHAR(255),                         -- 领域名称
    sort_id INTEGER,                                 -- 分类ID
    sort_name VARCHAR(255),                          -- 分类名称
    visible_status VARCHAR(50) DEFAULT 'ENABLED',    -- 可见状态
    updator VARCHAR(255),                            -- 更新者
    link VARCHAR(500),                               -- 链接
    category VARCHAR(100),                           -- 分类（人工分类，如：安置房问题、物业管理等）
    
    -- 附件和扩展信息
    attaches JSONB,                                  -- 附件(图片URL等)
    ext JSONB,                                       -- 扩展信息
    
    -- 全文搜索
    search_vector TSVECTOR,                          -- 全文搜索向量
    
    -- 外键关联
    FOREIGN KEY (assign_organization_id) REFERENCES organizations(org_id)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_complaints_assign_organization_id ON complaints (assign_organization_id);
CREATE INDEX IF NOT EXISTS idx_complaints_created_at ON complaints (created_at);
CREATE INDEX IF NOT EXISTS idx_complaints_reply_status ON complaints (reply_status);
CREATE INDEX IF NOT EXISTS idx_complaints_handle_status ON complaints (handle_status);
CREATE INDEX IF NOT EXISTS idx_complaints_category ON complaints (category);
CREATE INDEX IF NOT EXISTS idx_complaints_source ON complaints (source);
CREATE INDEX IF NOT EXISTS idx_complaints_satisfaction ON complaints (satisfaction);
CREATE INDEX IF NOT EXISTS idx_complaints_deadline ON complaints (deadline);

-- 创建全文搜索索引
CREATE INDEX IF NOT EXISTS idx_complaints_search_vector ON complaints USING GIN(search_vector);

-- 创建更新触发器函数
CREATE OR REPLACE FUNCTION complaints_search_vector_update() RETURNS TRIGGER AS $$
BEGIN
  NEW.search_vector = to_tsvector('simple', 
    COALESCE(NEW.title, '') || ' ' || 
    COALESCE(NEW.content, '') || ' ' || 
    COALESCE(NEW.category, '') || ' ' || 
    COALESCE(NEW.organization_name, '')
  );
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

-- 创建触发器
DROP TRIGGER IF EXISTS complaints_search_vector_update ON complaints;
CREATE TRIGGER complaints_search_vector_update
BEFORE INSERT OR UPDATE OF title, content, category, organization_name
ON complaints
FOR EACH ROW
EXECUTE PROCEDURE complaints_search_vector_update();
"""

# 爬取任务表结构定义
CRAWL_TASK_SCHEMA = """
-- 爬取任务表
CREATE TABLE IF NOT EXISTS crawl_tasks (
    id SERIAL PRIMARY KEY,                           -- 自增主键
    task_type VARCHAR(50) NOT NULL,                  -- 任务类型(ORGANIZATION, COMPLAINT等)
    target_id VARCHAR(255),                          -- 目标ID（如组织机构ID）
    status VARCHAR(50) DEFAULT 'PENDING',            -- 任务状态(PENDING, RUNNING, COMPLETED, FAILED)
    parameters JSONB,                                -- 任务参数
    result JSONB,                                    -- 任务结果
    error_message TEXT,                              -- 错误信息
    start_time TIMESTAMP WITHOUT TIME ZONE,          -- 开始时间
    end_time TIMESTAMP WITHOUT TIME ZONE,            -- 结束时间
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_crawl_tasks_task_type ON crawl_tasks (task_type);
CREATE INDEX IF NOT EXISTS idx_crawl_tasks_target_id ON crawl_tasks (target_id);
CREATE INDEX IF NOT EXISTS idx_crawl_tasks_status ON crawl_tasks (status);
CREATE INDEX IF NOT EXISTS idx_crawl_tasks_created_at ON crawl_tasks (created_at);
"""

# 统计报表表结构定义
STATISTICS_SCHEMA = """
-- 统计报表表
CREATE TABLE IF NOT EXISTS statistics (
    id SERIAL PRIMARY KEY,                           -- 自增主键
    report_type VARCHAR(50) NOT NULL,                -- 报表类型(DAILY, WEEKLY, MONTHLY等)
    report_date DATE NOT NULL,                       -- 报表日期
    organization_id INTEGER,                         -- 组织机构ID
    metrics JSONB NOT NULL,                          -- 统计指标(新增投诉数、已处理数、满意度等)
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 外键关联
    FOREIGN KEY (organization_id) REFERENCES organizations(org_id)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_statistics_report_type ON statistics (report_type);
CREATE INDEX IF NOT EXISTS idx_statistics_report_date ON statistics (report_date);
CREATE INDEX IF NOT EXISTS idx_statistics_organization_id ON statistics (organization_id);
"""

# 创建全文搜索所需的扩展
EXTENSIONS = """
-- 启用全文搜索扩展
CREATE EXTENSION IF NOT EXISTS pg_trgm;
"""

# 完整的数据库结构定义
DB_SCHEMA = EXTENSIONS + ORGANIZATION_SCHEMA + THREAD_SCHEMA + CRAWL_TASK_SCHEMA + STATISTICS_SCHEMA 