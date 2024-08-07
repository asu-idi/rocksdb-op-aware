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

#include "netservice.grpc.pb.h"


using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using netservice::NetService;
using netservice::OperationRequest;
using netservice::OperationResponse;


class NetClient {
public:
    NetClient(std::shared_ptr<grpc::Channel> channel);
    bool FlushBuffer();
    bool BufferedWriter(const std::string& operation, const std::string& key, const std::string& value);
    void SendRequestsFromQueue();

private:
    std::unique_ptr<NetService::Stub> stub_;
};


#endif // RDB_CLIENT_H
