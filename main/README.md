# 主模块说明

该目录负责 API 的基础路由注册与通用行为处理，提供以下核心功能：

- 统一入口（`index.js`）
- 路由分发
- 全局异常兜底
- 通用响应工具（`response.js`）
- KV 读取工具（`kv.js`）
- Referrer 验证工具（`referer.js`）

## 路由清单

当前 `routes`：

- `GET /`：返回 `404 API Not Found`
- `GET /hello`：返回示例响应
- `GET /random-img`：随机图片主接口（详见 `../random-img/README.md`）
- `random-img-count`：不在代码中写死，路径从 `path-config` 的 `RANDOM_IMG_COUNT_PATH` 动态读取

## 入口行为

`index.js` 采用以下处理流程：

1. 从请求 URL 提取 `pathname`
2. 先在普通 `routes` 中查找处理器
3. 普通路由未命中时，再尝试隐藏路径动态路由
4. 未命中返回 `404 API Not Found`
5. 发生未捕获异常时返回 `500 Internal Server Error`

## 隐藏路径扩展

`index.js` 在 `ROUTES` 中维护普通路由入口，在 `HIDDEN_PATH_KEYS` 中维护隐藏路由 KV 键，具体路径从 `path-config` 命名空间动态读取。

所有隐藏路径统一不接受任何查询参数；若携带 query string，将直接返回 `403`。

未来新增普通路由时，在 `index.js` 中做两件事：

- 新增该模块的静态导入
- 在 `ROUTES` 中新增一条入口（值为模块导出对象）

未来新增隐藏路由时，只需在 `index.js` 中新增一个 `*_PATH_KEY` 并加入 `hiddenPathKeys`。

处理函数采用命名约定自动匹配：

- 普通路由 `/foo-bar` 对应 `routes["/foo-bar"]` 模块中的 `handleFooBar`
- 隐藏路由 KV key `FOO_BAR_PATH` 对应 `handleFooBar`

## 响应工具

`response.js` 提供统一 JSON 响应方法：

- `detailedErrorResponse(error, details?)`
- `jsonErrorResponse(error)`
- `jsonSuccessResponse(data, status?)`

统一响应头：

- `Content-Type: application/json; charset=utf-8`
