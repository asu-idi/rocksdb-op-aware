#ifndef RDB_CLIENT_H
#define RDB_CLIENT_H

#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <atomic>
#include <map>

#include "netservice.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

using netservice::NetService;
using netservice::OperationRequest;
using netservice::OperationResponse;

class ThreadPool {
public:
    explicit ThreadPool(size_t num_threads);
    ~ThreadPool();

    void Enqueue(std::function<void()> task);

private:
    // Worker threads
    std::vector<std::thread> workers_;
    // Task queue
    std::queue<std::function<void()>> tasks_;

    // Synchronization
    std::mutex queue_mutex_;
    std::condition_variable condition_;
    bool stop_;
};

class NetClient {
public:
    NetClient(std::shared_ptr<grpc::Channel> channel);
    ~NetClient(); 

    bool FlushBuffer();
    bool BufferedWriter(const std::string& operation, const std::string& key, const std::string& value);
    bool SingleWriter(const std::string& operation, const std::string& key, std::string& value);

private:
    void SetOperation(OperationRequest& request, const std::string& operation);
    void ProcessCompletionQueue();
    void SendBufferedRequests(OperationRequest request);

    // Asynchronous components
    std::unique_ptr<NetService::Stub> stub_;
    grpc::CompletionQueue cq_;
    std::thread cq_thread_;

    // Structure for managing asynchronous requests
    struct AsyncClientCall {
        OperationResponse response;
        ClientContext context;
        Status status;
        std::unique_ptr<grpc::ClientAsyncResponseReader<OperationResponse>> response_reader;
        uint64_t sequence_number;
    };

    // Request buffer and synchronization mechanism
    OperationRequest request_; 
    std::mutex mtx_;
    const uint64_t IDENTIFICATION_VALUE = 0xABCD;

    // Sequence number for requests
    std::atomic<uint64_t> sequence_number_{0};
    std::map<uint64_t, OperationRequest> pending_requests_;
    std::mutex pending_requests_mtx_;

    // Thread pool for sending requests
    ThreadPool send_thread_pool_;
};

#endif // RDB_CLIENT_H