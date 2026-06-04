# 轻量级FTP服务端与客户端 
基于 C++、epoll和线程池实现的高性能FTP服务器和FTP客户端，支持主动模式、被动模式、文件上传、下载、目录列表、用户登录等功能。

## 特点
- 采用epoll ET模式实现高并发IO多路复用
- 线程池异步处理文件传输任务
- 支持FTP主动/被动传输模式
- 实现用户认证、文件上传下载、目录展示

## 环境配置步骤

- Linux操作系统(推荐Ubuntu等)
- 支持 C++11 及以上标准的编译器
- 执行以下命令安装编译工具：
```
//Ubuntu
sudo apt update
sudo apt install g++ make
```

## 编译运行

无需额外依赖库，直接使用 g++ 编译即可
```
//编译
g++ epollserver.cc -o server
g++ client.cc -o client
```

```
//启动服务端
./server
//启动客户端
./client 服务器IP地址
//示例：./client 127.0.0.1
```

## 登录与使用

输入默认账号密码登录：
```
用户名：ftp
密码：12345
```

支持的命令：
```
PASV      切换被动模式
PORT      切换主动模式
LIST      列出指定目录
RETR      下载文件（格式：RETR 远程文件 本地文件）
STOR      上传文件（格式：STOR 本地文件 远程文件）
QUIT      退出客户端
```

