# FTP Server & Client

基于 **C++17 + Socket + Epoll + ThreadPool** 实现的简易 FTP 文件传输系统。

项目采用：

* Reactor（Epoll）网络模型
* 线程池处理耗时任务
* FTP 主动控制连接 + PASV 被动数据连接
* 支持文件上传下载
* 支持目录浏览
* 支持用户登录认证

---

# 项目功能

目前实现的 FTP 指令：

| 指令   | 功能     |
| ---- | ------ |
| USER | 用户登录   |
| PASS | 密码认证   |
| PASV | 开启被动模式 |
| LIST | 查看目录内容 |
| RETR | 下载文件   |
| STOR | 上传文件   |
| QUIT | 退出客户端  |

---

# 项目架构

```text
┌─────────────┐
│ FTP Client  │
└──────┬──────┘
       │控制连接
       ▼
┌─────────────┐
│ FTP Server  │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Epoll     │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ ThreadPool  │
└──────┬──────┘
       │
       ▼
LIST / RETR / STOR
```

---

# 环境配置步骤

## 1. 安装编译环境

Ubuntu：

```bash
sudo apt update

sudo apt install -y \
    g++ \
    gcc \
    make \
    cmake
```

查看版本：

```bash
g++ --version
```

建议：

```text
g++ >= 9
C++ >= 17
Linux Kernel >= 4.x
```

---

## 2. 克隆项目

```bash
git clone git@github.com:CX33071/FTP.git

cd FTP
```

---

## 3. 项目目录结构

```text
FTP
│
├── epollserver.cc
├── client.cc
├── README.md

```

---

# 编译项目

## 编译服务器

```bash
g++ epollserver.cc -o server
```

---

## 编译客户端

```bash
g++ client.cc -o client
```

---

# 项目启动流程

## 第一步：启动 FTP 服务器

```bash
./server
```

输出：

```text
ftp server listen2100
```

表示服务器已经监听：

```text
0.0.0.0:2100
```

---

## 第二步：启动 FTP 客户端

假设服务器 IP：

```text
127.0.0.1
```

运行：

```bash
./client 127.0.0.1
```

远程服务器示例：

```bash
./client 192.168.1.100
```

---

## 第三步：登录

服务器内置账号：

```text
用户名：ftp
密码：123456
```

客户端输入：

```text
请输入用户名:
ftp

请输入密码:
123456
```

登录成功：

```text
成功登录
```

---

## 第四步：执行 FTP 命令

### 查看目录

```bash
LIST
```

或者：

```bash
LIST /home/user
```

---

### 上传文件

```bash
STOR local.txt remote.txt
```

说明：

```text
local.txt  -> 本地文件
remote.txt -> 服务器文件
```

示例：

```bash
STOR test.txt upload.txt
```

---

### 下载文件

```bash
RETR remote.txt local.txt
```

说明：

```text
remote.txt -> 服务器文件
local.txt  -> 本地文件
```

示例：

```bash
RETR upload.txt download.txt
```

---

### 退出

```bash
QUIT
```

---

# 被动模式（PASV）

本项目采用 FTP 标准 PASV 模式。

流程如下：

```text
Client
   │
   │ PASV
   ▼
Server
   │
   │ 创建随机监听端口
   ▼
227 Entering Passive Mode
   │
   ▼
Client连接数据端口
   │
   ▼
LIST / RETR / STOR
```

这样可以避免服务端主动连接客户端导致的防火墙问题。

---

# 默认测试账号

```text
用户名：ftp
密码：123456
```

服务器代码：

```cpp
account["ftp"] = "123456";
```

可自行扩展为：

* 文件存储
* MySQL数据库认证
* Redis会话管理

---

# 技术要点

项目使用：

* C++17
* Socket
* TCP/IP
* Epoll ET模式
* ThreadPool线程池
* RAII Socket封装
* FTP PASV模式

涉及知识点：

* Linux网络编程
* Reactor模型
* 多线程并发编程
* 文件IO
* FTP协议

---

# 后续优化方向

* [ ] 支持多用户管理
* [ ] MySQL账号认证
* [ ] Redis会话缓存
* [ ] 文件断点续传
* [ ] 文件秒传
* [ ] TLS/SSL加密传输
* [ ] 日志系统
* [ ] 配置文件解析
* [ ] 命令权限控制
* [ ] 大文件传输优化

---

# License

MIT License

```

作者：曹凌熙
```
