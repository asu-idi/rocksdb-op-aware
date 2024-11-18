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

#include "netservice.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

using netservice::NetService;
using netservice::OperationRequest;
using netservice::OperationResponse;

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
    };

    // Request buffer and synchronization mechanism
    OperationRequest request_; 
    std::mutex mtx_;
    const uint32_t IDENTIFICATION_VALUE = 0xABCD;
};

#endif // RDB_CLIENT_H