#ifndef RDB_CLIENT_H
#define RDB_CLIENT_H

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <grpcpp/grpcpp.h>

#include "netservice.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using netservice::NetService;
using netservice::OperationRequest;
using netservice::OperationResponse;

class ThreadPool {
public:
    ThreadPool(size_t numThreads);
    ~ThreadPool();
    void enqueue(std::function<void()> task);

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queueMutex;
    std::condition_variable condition;
    bool stop;

    void workerThread();
};

class NetClient {
public:
    NetClient(std::shared_ptr<grpc::Channel> channel, size_t threadPoolSize);
    // std::string GetBatchData(const std::string& key, const std::string& value);
    bool OperationService(const std::string& operation, const std::string& key, const std::string& value);

private:
    void operationService(const std::string& operation, const std::string& key, const std::string& value);

    std::unique_ptr<NetService::Stub> stub_;
    ThreadPool threadPool;
    std::mutex mtx;
};


#endif // RDB_CLIENT_H
