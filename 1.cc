#include <arpa/inet.h>//字节序转换，主机字节序：本地CPU存储顺序，小端序，不同电脑主机序不一样，不能直接发网络；网络字节序：统一规定大端序，所有网络传输必须用这个，发数据前把主机序转为网络序，收数据后必须把网络序转为主机序，htons（端口/数字）主机到网络，inet_pton(ip字符串到网络序二进制ip)
#include <dirent.h>//目录操作,返回dir*目录指针
#include <errno.h>
#include <fcntl.h>//open/fcntl改变文件描述符行为
#include <netinet/in.h>//sockaddr_in结构体
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>//文件状态
#include <unistd.h>//close/read/write
#include <condition_variable>//条件变量，线程池用
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#define MAX_SIZE 100//epoll最大监听事件数

class Socket {
   public:
    Socket(int domain, int type, int protocol);//地址族IPV4/IPV6、套接字类型TCP/UDP、协议号(具体决定用什么协议，几乎永远填0,系统自动根据前两个参数匹配协议)
    ~Socket();
    int fd();
    bool isvalid();
    bool enableport();//端口复用，解决服务器重启地址还在使用问题，linux默认会等待30ms左右，端口被占用，设置端口复用，方便调试

   private:
    int fd_ = -1;
};

class IPV4 {
   public:
    IPV4(uint16_t port);
    sockaddr* change();
    socklen_t len();
    uint16_t getport();

   private:
    sockaddr_in addr_{};
};

struct Client {//每个客户端对应一个client，存储登录状态、缓冲区、数据通道fd
    bool hasuser = false;
    bool islogin = false;
    std::string username;
    std::mutex mutex;
    int pasvfd = -1;
    std::string readbuf;//读缓冲区，解决粘包
    std::string writebuf;
};
class Threadpool {
   public:
    Threadpool(ssize_t num = std::thread::hardware_concurrency());
    ~Threadpool();
    void addtask(std::function<void()> task);

   private:
    void work();

    bool stop_ = false;
    std::mutex mutex_;
    std::condition_variable cond_;
    std::queue<std::function<void()>> tasks_;
    std::vector<std::thread> threads_;
};
class ftpepollserver {
   public:
    ftpepollserver(uint16_t port);
    ~ftpepollserver();
    void run();

   private:
    void handle_read(int fd);
    void handle_write(int fd);
    void add_epoll(int fd, uint32_t events);
    void del_epoll(int fd, uint32_t events);
    void mod_epoll(int fd, uint32_t events);
    void USER(int cfd, Client& client, std::string& user);
    void PASS(int cfd, Client& client, std::string& pass);
    void PASV(int cfd, Client& client);
    int acceptdata(Client& client);
    void LIST(int cfd, Client& client, std::string args);
    void RETR(int cfd, Client& client, std::string path1, std::string path2);
    void STOR(int cfd, Client& client, std::string path1, std::string path2);
    void closePASV(Client& client);

    uint16_t port_;
    Socket listensock_;
    int epfd;
    Threadpool pool;
    std::map<int, Client> clients;
    std::map<std::string, std::string> account;
};

std::string getlineok(std::string line);//去掉\r\n
bool sendn(int fd, char* data, ssize_t len);
void set_nonblock(int fd);
Socket::Socket(int domain, int type, int protocol)
    : fd_(socket(domain, type, protocol)) {}

Socket::~Socket() {
    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
    }
}

int Socket::fd() {
    return fd_;
}

bool Socket::isvalid() {
    return fd_ != -1;
}

bool Socket::enableport() {
    int opt = 1;
    if (setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        std::cout << strerror(errno) << std::endl;
        return false;
    }
    return true;
}

IPV4::IPV4(uint16_t port) {
    addr_.sin_family = AF_INET;
    addr_.sin_addr.s_addr = htonl(INADDR_ANY);//监听所有网卡，0.0.0.0本机所有IPV4地址，不限制谁能连我，连我的本机ip地址，局域网所有人，外网等，只要能找到我的IP+端口，只要能到达我的网络，就能连，客户端来连的时候带上我的ip+端口
    addr_.sin_port = htons(port);
}

sockaddr* IPV4::change() {//返回sockaddr*类型地址，供bind/connect使用
    return reinterpret_cast<sockaddr*>(&addr_);
}

socklen_t IPV4::len() {
    return sizeof(addr_);
}

uint16_t IPV4::getport() {//获取本机端口，用于PASV模式返回端口
    return ntohs(addr_.sin_port);
}

Threadpool::Threadpool(ssize_t num) {
    if (num == 0) {
        num = 4;
    }
    for (ssize_t i = 0; i < num; ++i) {
        threads_.emplace_back(&Threadpool::work, this);
    }
}

Threadpool::~Threadpool() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    cond_.notify_all();
    for (auto& t : threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
}

void Threadpool::addtask(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        tasks_.push(std::move(task));
    }
    cond_.notify_one();
}

void Threadpool::work() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cond_.wait(lock, [this]() { return stop_ || !tasks_.empty(); });
            if (stop_ && tasks_.empty()) {
                return;
            }
            task = std::move(tasks_.front());
            tasks_.pop();
        }
        task();
    }
}
ftpepollserver::ftpepollserver(uint16_t port)
    : port_(port), listensock_(AF_INET, SOCK_STREAM, 0) {
    epfd = epoll_create1(0);
    account["ftp"] = "123456";
}
ftpepollserver::~ftpepollserver() {
    close(epfd);
}
void ftpepollserver::run() {
    if (!listensock_.isvalid()) {
        std::perror("socket");
        return;
    }
    if (!listensock_.enableport()) {
        std::perror("setsockopt");
        return;
    }

    IPV4 addr(port_);
    bind(listensock_.fd(), addr.change(), addr.len());
    listen(listensock_.fd(), 10);//客户端来主动连接服务器，刚开始就是被动模式
    set_nonblock(listensock_.fd());
    std::cout << "ftp server listen" << port_ << '\n';
    add_epoll(listensock_.fd(), EPOLLIN | EPOLLET);//把监听socket加入epoll
    struct epoll_event events[MAX_SIZE];//定义事件数组，接收epoll返回的事件，一次最多处理100个事件
    while (1) {//服务器主循环，等待事件、处理事件、再等待
        int n = epoll_wait(epfd, events, MAX_SIZE, -1);
        for (int i = 0; i < n; i++) {//遍历本次epoll返回的所有发生的事件
            int fd = events[i].data.fd;//获取事件对应的文件描述符
            if (fd == listensock_.fd()) {//有新客户端连接
                while (1) {//一次性把所有等待的客户端全部接入
                    int cfd = accept(listensock_.fd(), nullptr, nullptr);
                    if (cfd == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {//接收失败，因为没有新的客户端了，退出循环
                            break;
                        } else {
                            perror("accept");
                            break;
                        }
                    }
                    std::cout << "有一个客户端成功连接\n";
                    set_nonblock(cfd);
                    std::lock_guard<std::mutex> lock(clients[cfd].mutex);
                    clients[cfd].writebuf += "220 ftp server ready\r\n";//给客户端发欢迎信息
                    add_epoll(cfd, EPOLLIN | EPOLLET | EPOLLOUT);//把客户端socket加入epoll，监听读+写+边缘触发
                }
            } else if (events[i].events & EPOLLIN) {
                handle_read(fd);
            } else if (events[i].events & EPOLLOUT) {
                handle_write(fd);
            }
        }
    }
}
void ftpepollserver::handle_read(int fd) {
    ssize_t n;
    char buf[4096];//临时缓冲区，存放从内核读到的数据
    while (1) {
        n = read(fd, buf, sizeof(buf));
        if (n > 0) {
            clients[fd].readbuf.append(buf, n);
        } else if (n == 0) {
            close(fd);
            clients.erase(fd);
            del_epoll(fd, 0);
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {//数据读完了，在readbuf里
                break;
            } else {
                close(fd);
                return;
            }
        }
    }
    while (1) {
        ssize_t pos = clients[fd].readbuf.find("\n");//按行拆分读缓冲区数据，解决粘包
        if (pos == std::string::npos) {
            break;
        }
        std::string s = clients[fd].readbuf.substr(0, pos + 1);
        clients[fd].readbuf.erase(0, pos + 1);
        s = getlineok(s);
        std::string cmd, arg;
        int p = s.find(' ');
        if (p == std::string::npos) {
            cmd = s;
        } else {
            cmd = s.substr(0, p);
            arg = s.substr(p + 1);
        }
        if (cmd == "USER") {
            USER(fd, clients[fd], arg);
        } else if (cmd == "PASS") {
            PASS(fd, clients[fd], arg);
        } else if (cmd == "QUIT") {
            std::lock_guard<std::mutex> lock(clients[fd].mutex);
            clients[fd].writebuf += "221,bye bye\r\n";
            mod_epoll(fd, EPOLLIN | EPOLLOUT | EPOLLET);
        } else if (!strcasecmp(cmd.c_str(), "pasv")) {
            PASV(fd, clients[fd]);
        } else if (!strcasecmp(cmd.c_str(), "list")) {
            pool.addtask([this, fd, arg]() { LIST(fd, clients[fd], arg); });//丢线程池异步执行，放在竹线程的话，耗时操作，阻塞主循环，你stor的时候别的客户端什么也干不了，也不能接收新的客户端连接
        } else if (!strcasecmp(cmd.c_str(), "stor")) {
            int pos = arg.find(' ');
            std::string s1 = arg.substr(0, pos);
            std::string s2 = arg.substr(pos + 1);

            pool.addtask(
                [this, fd, s1, s2]() { STOR(fd, clients[fd], s1, s2); });
        } else if (!strcasecmp(cmd.c_str(), "retr")) {
            int pos = arg.find(' ');
            std::string s1 = arg.substr(0, pos);
            std::string s2 = arg.substr(pos + 1);
            pool.addtask(
                [this, fd, s1, s2]() { RETR(fd, clients[fd], s1, s2); });
        } else {
            std::lock_guard<std::mutex> lock(clients[fd].mutex);
            clients[fd].writebuf += "502 command errno\r\n";
            mod_epoll(fd, EPOLLIN | EPOLLOUT | EPOLLET);//EPOLLOUT监听内核发送缓冲区有空闲位置，可以写数据了，如果从一开始就一直监听，就一直提醒写事件，除非内核缓冲区满了，但是客户端和服务端没有要发的数据，然后一直进行写事件处理，CPU空转，所以只有客户端或服务端要发消息的时候开启监听读事件，发完立马关闭读事件
        }
    }
    int cfd = accept(listensock_.fd(), nullptr, nullptr);
}
void ftpepollserver::handle_write(int fd) {
    ssize_t n;
    std::lock_guard<std::mutex> lock(clients[fd].mutex);//主线程在handle_write写数据，线程池在LIST/STOR写数据，多线程同时操作writebuf
    while (!clients[fd].writebuf.empty()) {
        n = send(fd, clients[fd].writebuf.data(), clients[fd].writebuf.size(),
                 0);
        if (n > 0) {
            clients[fd].writebuf.erase(0, n);
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {//内核缓冲区满了或者客户端写缓冲区数据发完了
                break;
            }
            close(fd);
            clients.erase(fd);
            del_epoll(fd, 0);
            return;
        }
    }
    if (clients[fd].writebuf.empty()) {
        mod_epoll(fd, EPOLLET | EPOLLIN);
    } else {
        mod_epoll(fd, EPOLLIN | EPOLLET | EPOLLOUT);
    }
}
void ftpepollserver::add_epoll(int fd, uint32_t events) {
    epoll_event event{};
    event.events = events;
    event.data.fd = fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event);
}

void ftpepollserver::del_epoll(int fd, uint32_t events) {
    epoll_event event{};
    event.events = events;
    event.data.fd = fd;
    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, &event);
}
void ftpepollserver::mod_epoll(int fd, uint32_t events) {
    epoll_event event{};
    event.events = events;
    event.data.fd = fd;
    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &event);
}
void ftpepollserver::USER(int cfd, Client& client, std::string& user) {
    if (user.empty()) {
        std::lock_guard<std::mutex> lock(clients[cfd].mutex);
        clients[cfd].writebuf += "501 need username\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    }
    if (account.count(user) == 0) {
        std::lock_guard<std::mutex> lock(clients[cfd].mutex);
        clients[cfd].writebuf += "530 username exist\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    }
    client.username = user;
    client.hasuser = true;
    client.islogin = false;
    std::lock_guard<std::mutex> lock(clients[cfd].mutex);
    clients[cfd].writebuf += "331 need password\r\n";
    mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
}

void ftpepollserver::PASS(int cfd, Client& client, std::string& pass) {
    if (!client.hasuser) {
        std::lock_guard<std::mutex> lock(clients[cfd].mutex);
        clients[cfd].writebuf += "503 need username\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    }
    auto it = account.find(client.username);
    if (it == account.end() || it->second != pass) {
        std::lock_guard<std::mutex> lock(clients[cfd].mutex);
        clients[cfd].writebuf += "530 password errno\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    }
    client.islogin = true;
    std::lock_guard<std::mutex> lock(clients[cfd].mutex);
    clients[cfd].writebuf += "230 login succeful\r\n";
    mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
}

void ftpepollserver::PASV(int cfd, Client& client) {//开一个随机端口，让客户端来连接
    closePASV(client);//同一时间只能有一个数据通道

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    IPV4 addr(0);

    bind(fd, addr.change(), addr.len());
    listen(fd, 1);
    socklen_t len = addr.len();
    getsockname(fd, addr.change(), &len);//获取系统刚刚分配的真实随机端口号
    sockaddr_in caddr{};
    len = sizeof(caddr);
    getsockname(cfd, reinterpret_cast<sockaddr*>(&caddr), &len);//获取服务器本机IP，告诉客户端要连接哪个IP
    unsigned char* ip =
        reinterpret_cast<unsigned char*>(&caddr.sin_addr.s_addr);//把32位整数IP转为4段十进制格式，4个字节拼起来的整数，s_addr是int类型，不能按字节取，把它强转成unsigned char*,unsigned char*正好是1字节，转后一共4字节
    uint16_t port = addr.getport();//得到的端口号是16位整数，FTP规定发给客户端的端口号要拆分，把16位端口拆成高8位、低8位两个字节，256是2的8次方，比如端口是12345,12345的二进制：00110000高8位，00111001低8位，高8位计算：12345/256,低8位计算：12345%256,客户端收到后计算：48*256+57，一个字节占8位，可以表示0-255一共256个数字
    int p1 = port / 256;
    int p2 = port % 256;

    client.pasvfd = fd;
    std::lock_guard<std::mutex> lock(clients[cfd].mutex);
    clients[cfd].writebuf +=
        "227 Entering Passive Mode (" + std::to_string(ip[0]) + "," +
        std::to_string(ip[1]) + "," + std::to_string(ip[2]) + "," +
        std::to_string(ip[3]) + "," + std::to_string(p1) + "," +
        std::to_string(p2) + ")\r\n";
    mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
}

int ftpepollserver::acceptdata(Client& client) {
    if (client.pasvfd == -1) {
        return -1;
    }
    int datafd = accept(client.pasvfd, nullptr, nullptr);
    closePASV(client);//一个客户端只需要连接一次数据通道，数据通道建立拿到新的datafd后立马关闭服务端建立的数据通道的socketfd
    return datafd;
}//接受客户端对PASV端口的连接，拿到数据通道fd,然后立刻关闭监听端口

void ftpepollserver::LIST(int cfd, Client& client, std::string arg) {
    if (client.pasvfd == -1) {
        std::lock_guard<std::mutex> lock(client.mutex);
        clients[cfd].writebuf += "425 need pasv first\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    }
    clients[cfd].writebuf += "150 open data connection for list\r\n";
    mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
    int datafd = acceptdata(client);
    if (datafd == -1) {
        clients[cfd].writebuf += "425 data connect failed\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    }

    if (arg.empty()) {
        arg = ".";
    }
    DIR* dir = opendir(arg.c_str());//目录结构体
    std::string datafdwritebuf;
    if (dir == nullptr) {
        close(datafd);
        clients[cfd].writebuf += "550 open dir failed\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    }

    dirent* ent = nullptr;//dirent目录里的一个文件/文件夹
    while ((ent = readdir(dir)) != nullptr) {//readdir每次返回下一个文件/文件夹的信息
        std::lock_guard<std::mutex> lock(client.mutex);
        std::string line = std::string(ent->d_name) + "\r\n";
        if (!sendn(datafd, line.data(), line.size())) {
            break;
        }
    }
    closedir(dir);
    close(datafd);
    clients[cfd].writebuf += "226 list complete\r\n";
    mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
}
void ftpepollserver::RETR(int cfd,
                          Client& client,
                          std::string path1,
                          std::string path2) {
    if (path1.empty() ) {
        clients[cfd].writebuf += "501 need file name\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    }
    if (client.pasvfd == -1) {
        clients[cfd].writebuf += "425 need pasv first\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    }

    int fd = open(path1.c_str(), O_RDONLY);
    if (fd == -1) {
        clients[cfd].writebuf += "550 file not found\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    }

    clients[cfd].writebuf += "150 open data connect for retr\r\n";
    mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
    int datafd = acceptdata(client);
    if (datafd == -1) {
        close(fd);
        clients[cfd].writebuf += "425 data connect failed\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    }

    char buf[4096];
    ssize_t n = 0;
    bool ok = true;
    while ((n = read(fd, buf, sizeof(buf))) > 0) {
        if (send(datafd, buf, n, 0) != n) {
            ok = false;
            break;
        };
    }

    close(fd);
    close(datafd);//数据传输完必须关闭datafd，客户端才知道传完了
    if (ok) {
        clients[cfd].writebuf += "226 retr complete\r\n";

    } else {
        clients[cfd].writebuf += "426 connect close\r\n";
    }
    mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
}

void ftpepollserver::STOR(int cfd,
                          Client& client,
                          std::string path1,
                          std::string path2) {
    if (path1.empty() || path2.empty()) {
        clients[cfd].writebuf += "501 need file name\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    }
    if (client.pasvfd == -1) {
        clients[cfd].writebuf += "425 need pasv first\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    }

    int fd = open(path2.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        clients[cfd].writebuf += "550 create file failed\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    }

    clients[cfd].writebuf += "150 open data connect for stor\r\n";
    mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
    int datafd = acceptdata(client);
    if (datafd == -1) {
        close(fd);
        clients[cfd].writebuf += "425 data connect failed\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    }

    char buf[4096];
    ssize_t n = 0;
    bool ok = true;
    while ((n = recv(datafd, buf, sizeof(buf), 0)) > 0) {
        if (write(fd, buf, static_cast<ssize_t>(n)) != n) {
            ok = false;
            break;
        }
    }

    close(fd);
    close(datafd);
    if (ok) {
        clients[cfd].writebuf += "226 stor complete\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
    } else {
        clients[cfd].writebuf += "426 connect close\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
    }
}

void ftpepollserver::closePASV(Client& client) {
    if (client.pasvfd != -1) {
        close(client.pasvfd);
        client.pasvfd = -1;
    }
}
std::string getlineok(std::string line) {
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
        line.pop_back();
    }
    return line;
}
bool sendn(int fd, char* data, ssize_t len) {//循环发送指定长度数据知道全部发完
    while (len > 0) {
        ssize_t n = send(fd, data, len, 0);
        if (n <= 0) {
            return false;
        }
        data += n;
        len -= static_cast<ssize_t>(n);
    }
    return true;
}
void set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);//F_GETFL获取fd现在的状态标志
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);//F_SETFL设置fd的状态标志
}
int main() {
    ftpepollserver server(2100);
    server.run();
    return 0;
}
