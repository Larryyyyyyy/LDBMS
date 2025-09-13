# LDBMS数据库系统实践项目

这是一个用python实现的简易数据库项目，支持多线程，并发，端对端。

更多细节以后补充。

## 使用方法

在项目所在最外层目录打开控制台：

### 服务端

```bash
python Launcher_server.py -create "你的绝对目录" # 创建数据库
python Launcher_server.py -open "你的绝对目录" # 打开数据库并挂载到局域网
```

### 客户端

```bash
python Launcher_client.py # 具体连接设置可以在代码中更改
```

### SQL语句

详细的SQL语句介绍参看`backend/parser/Parser.py`。