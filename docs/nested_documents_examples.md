# 嵌套文档示例

本文档提供了 TigerDB 中嵌套文档的完整示例，包括简单嵌套和复杂嵌套场景。

## 目录

1. [简单嵌套文档](#简单嵌套文档)
2. [复杂嵌套文档](#复杂嵌套文档)
3. [多层嵌套文档](#多层嵌套文档)
4. [嵌套数组文档](#嵌套数组文档)
5. [混合嵌套文档](#混合嵌套文档)

---

## 简单嵌套文档

### 示例：用户和地址

```json
{
  "user": {
    "id": "user_001",
    "name": "张三",
    "email": "zhangsan@example.com",
    "addresses": [
      {
        "street": "北京市朝阳区",
        "city": "北京",
        "zipcode": "100000",
        "country": "中国"
      },
      {
        "street": "上海市浦东新区",
        "city": "上海",
        "zipcode": "200000",
        "country": "中国"
      }
    ]
  }
}
```

**API 调用示例：**

```bash
# 创建索引
PUT /users

# 创建文档
POST /users/_doc
{
  "id": "user_001",
  "name": "张三",
  "email": "zhangsan@example.com",
  "addresses": [
    {
      "street": "北京市朝阳区",
      "city": "北京",
      "zipcode": "100000",
      "country": "中国"
    },
    {
      "street": "上海市浦东新区",
      "city": "上海",
      "zipcode": "200000",
      "country": "中国"
    }
  ]
}
```

---

## 复杂嵌套文档

### 示例：电商订单系统

```json
{
  "order": {
    "order_id": "ORD-2024-001",
    "customer": {
      "customer_id": "CUST-001",
      "name": "李四",
      "email": "lisi@example.com",
      "phone": "13800138000",
      "address": {
        "street": "广州市天河区",
        "city": "广州",
        "province": "广东",
        "zipcode": "510000",
        "country": "中国"
      }
    },
    "items": [
      {
        "product_id": "PROD-001",
        "product_name": "iPhone 15 Pro",
        "quantity": 1,
        "price": 8999.0,
        "specifications": {
          "color": "深空黑色",
          "storage": "256GB",
          "memory": "8GB"
        },
        "reviews": [
          {
            "review_id": "REV-001",
            "rating": 5,
            "comment": "非常满意",
            "reviewer": {
              "name": "王五",
              "verified": true
            }
          }
        ]
      },
      {
        "product_id": "PROD-002",
        "product_name": "AirPods Pro",
        "quantity": 2,
        "price": 1899.0,
        "specifications": {
          "color": "白色",
          "generation": "2nd Gen"
        }
      }
    ],
    "shipping": {
      "method": "顺丰快递",
      "tracking_number": "SF1234567890",
      "estimated_delivery": "2024-01-15",
      "address": {
        "street": "广州市天河区",
        "city": "广州",
        "province": "广东",
        "zipcode": "510000",
        "country": "中国"
      }
    },
    "payment": {
      "method": "支付宝",
      "transaction_id": "TXN-2024-001",
      "amount": 12697.0,
      "status": "completed",
      "paid_at": "2024-01-10T10:30:00Z"
    },
    "metadata": {
      "created_at": "2024-01-10T10:00:00Z",
      "updated_at": "2024-01-10T10:30:00Z",
      "tags": ["urgent", "vip"],
      "notes": "客户要求加急配送"
    }
  }
}
```

**API 调用示例：**

```bash
# 创建索引
PUT /orders
{
  "mappings": {
    "properties": {
      "order_id": { "type": "keyword" },
      "customer": {
        "type": "object",
        "properties": {
          "customer_id": { "type": "keyword" },
          "name": { "type": "text" },
          "email": { "type": "keyword" },
          "address": {
            "type": "object",
            "properties": {
              "street": { "type": "text" },
              "city": { "type": "keyword" },
              "province": { "type": "keyword" }
            }
          }
        }
      },
      "items": {
        "type": "nested",
        "properties": {
          "product_id": { "type": "keyword" },
          "product_name": { "type": "text" },
          "quantity": { "type": "integer" },
          "price": { "type": "float" },
          "specifications": {
            "type": "object",
            "properties": {
              "color": { "type": "keyword" },
              "storage": { "type": "keyword" }
            }
          },
          "reviews": {
            "type": "nested",
            "properties": {
              "review_id": { "type": "keyword" },
              "rating": { "type": "integer" },
              "comment": { "type": "text" },
              "reviewer": {
                "type": "object",
                "properties": {
                  "name": { "type": "text" },
                  "verified": { "type": "boolean" }
                }
              }
            }
          }
        }
      }
    }
  }
}

# 创建文档
POST /orders/_doc
{
  "order_id": "ORD-2024-001",
  "customer": {
    "customer_id": "CUST-001",
    "name": "李四",
    "email": "lisi@example.com",
    "phone": "13800138000",
    "address": {
      "street": "广州市天河区",
      "city": "广州",
      "province": "广东",
      "zipcode": "510000",
      "country": "中国"
    }
  },
  "items": [
    {
      "product_id": "PROD-001",
      "product_name": "iPhone 15 Pro",
      "quantity": 1,
      "price": 8999.00,
      "specifications": {
        "color": "深空黑色",
        "storage": "256GB",
        "memory": "8GB"
      },
      "reviews": [
        {
          "review_id": "REV-001",
          "rating": 5,
          "comment": "非常满意",
          "reviewer": {
            "name": "王五",
            "verified": true
          }
        }
      ]
    },
    {
      "product_id": "PROD-002",
      "product_name": "AirPods Pro",
      "quantity": 2,
      "price": 1899.00,
      "specifications": {
        "color": "白色",
        "generation": "2nd Gen"
      }
    }
  ],
  "shipping": {
    "method": "顺丰快递",
    "tracking_number": "SF1234567890",
    "estimated_delivery": "2024-01-15",
    "address": {
      "street": "广州市天河区",
      "city": "广州",
      "province": "广东",
      "zipcode": "510000",
      "country": "中国"
    }
  },
  "payment": {
    "method": "支付宝",
    "transaction_id": "TXN-2024-001",
    "amount": 12697.00,
    "status": "completed",
    "paid_at": "2024-01-10T10:30:00Z"
  },
  "metadata": {
    "created_at": "2024-01-10T10:00:00Z",
    "updated_at": "2024-01-10T10:30:00Z",
    "tags": ["urgent", "vip"],
    "notes": "客户要求加急配送"
  }
}
```

---

## 多层嵌套文档

### 示例：组织结构

```json
{
  "organization": {
    "org_id": "ORG-001",
    "name": "科技有限公司",
    "departments": [
      {
        "dept_id": "DEPT-001",
        "name": "研发部",
        "manager": {
          "emp_id": "EMP-001",
          "name": "赵六",
          "email": "zhaoliu@example.com",
          "level": "L5"
        },
        "teams": [
          {
            "team_id": "TEAM-001",
            "name": "前端团队",
            "lead": {
              "emp_id": "EMP-002",
              "name": "孙七",
              "email": "sunqi@example.com"
            },
            "members": [
              {
                "emp_id": "EMP-003",
                "name": "周八",
                "role": "Senior Frontend Engineer",
                "skills": ["React", "TypeScript", "Vue"]
              },
              {
                "emp_id": "EMP-004",
                "name": "吴九",
                "role": "Frontend Engineer",
                "skills": ["React", "JavaScript"]
              }
            ],
            "projects": [
              {
                "project_id": "PROJ-001",
                "name": "电商平台重构",
                "status": "in_progress",
                "deadline": "2024-06-30"
              }
            ]
          },
          {
            "team_id": "TEAM-002",
            "name": "后端团队",
            "lead": {
              "emp_id": "EMP-005",
              "name": "郑十",
              "email": "zhengshi@example.com"
            },
            "members": [
              {
                "emp_id": "EMP-006",
                "name": "钱一",
                "role": "Senior Backend Engineer",
                "skills": ["Go", "Java", "Python"]
              }
            ]
          }
        ]
      }
    ]
  }
}
```

---

## 嵌套数组文档

### 示例：博客文章和评论

```json
{
  "article": {
    "article_id": "ART-001",
    "title": "TigerDB嵌套文档详解",
    "content": "本文详细介绍了TigerDB中嵌套文档的使用方法...",
    "author": {
      "author_id": "AUTH-001",
      "name": "作者一",
      "bio": "资深数据库专家"
    },
    "tags": ["数据库", "搜索引擎", "嵌套文档"],
    "comments": [
      {
        "comment_id": "COMM-001",
        "content": "非常好的文章！",
        "author": {
          "author_id": "AUTH-002",
          "name": "读者一"
        },
        "created_at": "2024-01-10T12:00:00Z",
        "replies": [
          {
            "reply_id": "REPLY-001",
            "content": "谢谢支持！",
            "author": {
              "author_id": "AUTH-001",
              "name": "作者一"
            },
            "created_at": "2024-01-10T12:30:00Z"
          }
        ],
        "likes": [
          {
            "user_id": "USER-001",
            "liked_at": "2024-01-10T13:00:00Z"
          },
          {
            "user_id": "USER-002",
            "liked_at": "2024-01-10T13:05:00Z"
          }
        ]
      },
      {
        "comment_id": "COMM-002",
        "content": "学到了很多",
        "author": {
          "author_id": "AUTH-003",
          "name": "读者二"
        },
        "created_at": "2024-01-10T14:00:00Z",
        "replies": [],
        "likes": []
      }
    ]
  }
}
```

---

## 混合嵌套文档

### 示例：社交媒体帖子

```json
{
  "post": {
    "post_id": "POST-001",
    "content": "今天天气真好！",
    "author": {
      "user_id": "USER-001",
      "username": "user001",
      "display_name": "用户一",
      "avatar": "https://example.com/avatar1.jpg",
      "profile": {
        "bio": "热爱生活",
        "location": "北京",
        "website": "https://example.com"
      }
    },
    "media": [
      {
        "type": "image",
        "url": "https://example.com/image1.jpg",
        "thumbnail": "https://example.com/thumb1.jpg",
        "metadata": {
          "width": 1920,
          "height": 1080,
          "format": "JPEG"
        }
      },
      {
        "type": "video",
        "url": "https://example.com/video1.mp4",
        "thumbnail": "https://example.com/video1_thumb.jpg",
        "metadata": {
          "duration": 120,
          "format": "MP4",
          "resolution": "1080p"
        }
      }
    ],
    "mentions": [
      {
        "user_id": "USER-002",
        "username": "user002",
        "position": [0, 5]
      }
    ],
    "hashtags": [
      {
        "tag": "天气",
        "position": [6, 8]
      },
      {
        "tag": "生活",
        "position": [9, 11]
      }
    ],
    "location": {
      "name": "天安门广场",
      "coordinates": {
        "lat": 39.9042,
        "lon": 116.4074
      },
      "address": "北京市东城区"
    },
    "engagement": {
      "likes": [
        {
          "user_id": "USER-003",
          "liked_at": "2024-01-10T10:00:00Z"
        }
      ],
      "shares": [
        {
          "user_id": "USER-004",
          "shared_at": "2024-01-10T11:00:00Z",
          "platform": "weibo"
        }
      ],
      "comments": [
        {
          "comment_id": "COMM-001",
          "content": "确实很好！",
          "author": {
            "user_id": "USER-005",
            "username": "user005"
          },
          "created_at": "2024-01-10T12:00:00Z",
          "replies": []
        }
      ]
    },
    "metadata": {
      "created_at": "2024-01-10T09:00:00Z",
      "updated_at": "2024-01-10T09:00:00Z",
      "language": "zh-CN",
      "sentiment": "positive"
    }
  }
}
```

---

## 查询嵌套文档

### 查询嵌套字段

```bash
# 查询包含特定嵌套字段的文档
GET /orders/_search
{
  "query": {
    "nested": {
      "path": "items",
      "query": {
        "match": {
          "items.product_name": "iPhone"
        }
      }
    }
  }
}

# 查询嵌套数组中的特定值
GET /users/_search
{
  "query": {
    "nested": {
      "path": "addresses",
      "query": {
        "match": {
          "addresses.city": "北京"
        }
      }
    }
  }
}
```

---

## 批量操作嵌套文档

```bash
POST /_bulk
{"index":{"_index":"orders","_id":"ORD-001"}}
{"order_id":"ORD-001","customer":{"name":"张三"},"items":[{"product_id":"PROD-001"}]}
{"index":{"_index":"orders","_id":"ORD-002"}}
{"order_id":"ORD-002","customer":{"name":"李四"},"items":[{"product_id":"PROD-002"}]}
{"create":{"_index":"users","_id":"USER-001"}}
{"name":"王五","addresses":[{"city":"北京"}]}
```

---

## 注意事项

1. **嵌套深度**：TigerDB 支持多层嵌套，但建议嵌套深度不超过 5 层以保证性能
2. **数组大小**：嵌套数组中的元素数量建议不超过 1000 个
3. **索引性能**：嵌套文档会创建独立的索引条目，大量嵌套文档可能影响索引性能
4. **查询性能**：嵌套查询比普通查询慢，建议合理使用

---

## 最佳实践

1. **合理设计嵌套结构**：避免过度嵌套，保持结构清晰
2. **使用合适的字段类型**：为嵌套字段选择合适的类型（text, keyword, nested 等）
3. **索引优化**：为经常查询的嵌套字段创建索引
4. **批量操作**：使用批量 API 处理大量嵌套文档
