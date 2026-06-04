#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#define MAX_SIZE 100

class Socket {
   public:
    Socket(int domain, int type, int protocol);
    ~Socket();
    int fd();
    bool isvalid();
    bool enableport();

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

struct Client {
    bool hasuser = false;
    bool islogin = false;
    std::string username;
    std::mutex mutex;
    int pasvfd = -1;
    std::string readbuf;
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

std::string getlineok(std::string line);
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
    addr_.sin_addr.s_addr = htonl(INADDR_ANY);
    addr_.sin_port = htons(port);
}

sockaddr* IPV4::change() {
    return reinterpret_cast<sockaddr*>(&addr_);
}

socklen_t IPV4::len() {
    return sizeof(addr_);
}

uint16_t IPV4::getport() {
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
    listensock_.enableport();
    IPV4 addr(port_);
    bind(listensock_.fd(), addr.change(), addr.len());
    listen(listensock_.fd(), 10);
    set_nonblock(listensock_.fd());
    std::cout << "ftp server listen" << port_ << '\n';
    add_epoll(listensock_.fd(), EPOLLIN | EPOLLET);
    struct epoll_event events[MAX_SIZE];
    while (1) {
        int n = epoll_wait(epfd, events, MAX_SIZE, -1);
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            if (fd == listensock_.fd()) {
                while (1) {
                    int cfd = accept(listensock_.fd(), nullptr, nullptr);
                    if (cfd == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break;
                        } else {
                            perror("accept");
                            break;
                        }
                    }
                    std::cout << "有一个客户端成功连接\n";
                    set_nonblock(cfd);
                    add_epoll(cfd, EPOLLIN | EPOLLET);
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
    char buf[4096];
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
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                close(fd);
                return;
            }
        }
    }
    while (1) {
        ssize_t pos = clients[fd].readbuf.find("\n");
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
            std::cout << "有一个客户端成功退出" << std::endl;
        } else if (!strcasecmp(cmd.c_str(), "pasv")) {
            PASV(fd, clients[fd]);
        } else if (!strcasecmp(cmd.c_str(), "list")) {
            pool.addtask([this, fd, arg]() { LIST(fd, clients[fd], arg); });
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
        }
    }
}
void ftpepollserver::handle_write(int fd) {
    ssize_t n;
    std::lock_guard<std::mutex> lock(clients[fd].mutex);
    while (!clients[fd].writebuf.empty()) {
        n = send(fd, clients[fd].writebuf.data(), clients[fd].writebuf.size(),
                 0);
        if (n > 0) {
            clients[fd].writebuf.erase(0, n);
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
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
    if (account.count(user) == 0) {
        std::lock_guard<std::mutex> lock(clients[cfd].mutex);
        clients[cfd].writebuf += "530 username disexist\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    } else {
        std::lock_guard<std::mutex> lock(clients[cfd].mutex);
        clients[cfd].writebuf += "222 username exist\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
    }
    client.username = user;
    client.hasuser = true;
    client.islogin = false;
}

void ftpepollserver::PASS(int cfd, Client& client, std::string& pass) {
    auto it = account.find(client.username);
    if (it == account.end() || it->second != pass) {
        std::lock_guard<std::mutex> lock(clients[cfd].mutex);
        clients[cfd].writebuf += "530 password errno\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    } else {
        clients[cfd].writebuf += "230 login succeful\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
    }
    client.islogin = true;
    std::lock_guard<std::mutex> lock(clients[cfd].mutex);
}

void ftpepollserver::PASV(int cfd, Client& client) {
    closePASV(client);
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    IPV4 addr(0);
    bind(fd, addr.change(), addr.len());
    listen(fd, 1);
    socklen_t len = addr.len();
    getsockname(fd, addr.change(), &len);
    sockaddr_in caddr{};
    len = sizeof(caddr);
    getsockname(cfd, reinterpret_cast<sockaddr*>(&caddr), &len);
    unsigned char* ip =
        reinterpret_cast<unsigned char*>(&caddr.sin_addr.s_addr);
    uint16_t port = addr.getport();
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
    int datafd = accept(client.pasvfd, nullptr, nullptr);
    closePASV(client);
    return datafd;
}

void ftpepollserver::LIST(int cfd, Client& client, std::string arg) {
    int datafd = acceptdata(client);
    if (arg.empty()) {
        arg = ".";
    }
    DIR* dir = opendir(arg.c_str());
    std::string datafdwritebuf;
    if (dir == nullptr) {
        close(datafd);
        clients[cfd].writebuf += "550 open dir failed\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    } else {
        clients[cfd].writebuf += "222 open dir\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
    }

    dirent* ent = nullptr;
    while ((ent = readdir(dir)) != nullptr) {
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
    int fd = open(path1.c_str(), O_RDONLY);
    if (fd == -1) {
        clients[cfd].writebuf += "550 file not found\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
        return;
    } else {
        clients[cfd].writebuf += "222 file found\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
    }
    int datafd = acceptdata(client);
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
    close(datafd);
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
    int fd = open(path2.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        clients[cfd].writebuf += "550 open failed\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);

    } else {
        clients[cfd].writebuf += "222 open\r\n";
        mod_epoll(cfd, EPOLLIN | EPOLLOUT | EPOLLET);
    }
    int datafd = acceptdata(client);
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
bool sendn(int fd, char* data, ssize_t len) {
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
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}
int main() {
    ftpepollserver server(2100);
    server.run();
    return 0;
}
