#include "rdb_client.h"
#include <grpcpp/grpcpp.h>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <atomic>

// 常量定义
const uint32_t IDENTIFICATION_VALUE = 0xABCD;
const size_t MAX_BUFFER_SIZE = 12000;

// NetClient 构造函数
NetClient::NetClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(NetService::NewStub(channel)), is_sending(false) {}

// SetOperation 函数
void NetClient::SetOperation(OperationRequest& request, const std::string& operation) {
    if (operation == "Put") {
        request.set_operation(OperationRequest::Put);
    } else if (operation == "Get") {
        request.set_operation(OperationRequest::Get);
    } else if (operation == "Delete") {
        request.set_operation(OperationRequest::Delete);
    } else if (operation == "BatchPut") {
        request.set_operation(OperationRequest::BatchPut);
    }
}

// 成员变量
std::mutex mtx;
std::queue<OperationRequest> request_queue;
std::condition_variable cv;
std::atomic<bool> is_sending(false);
OperationRequest request; // 缓冲区

// BufferedWriter 函数
bool NetClient::BufferedWriter(const std::string& operation, const std::string& key, const std::string& value) {
    std::unique_lock<std::mutex> lock(mtx);
    SetOperation(request, operation);
    request.add_keys(key);
    request.add_values(value);

    if (request.keys_size() >= MAX_BUFFER_SIZE) {
        // 将当前请求加入队列
        request.set_identification(IDENTIFICATION_VALUE);
        request.set_total_length(request.ByteSizeLong());
        request_queue.push(std::move(request));

        // 重置缓冲区
        request.Clear();

        // 如果没有在发送，则启动发送线程
        if (!is_sending.exchange(true)) {
            std::thread(&NetClient::SendRequestsFromQueue, this).detach();
        }
    }
    return true;
}

// SendRequestsFromQueue 函数
void NetClient::SendRequestsFromQueue() {
    while (true) {
        OperationRequest req;
        {
            std::unique_lock<std::mutex> lock(mtx);
            if (request_queue.empty()) {
                if (request.keys_size() > 0) {
                    request.set_identification(IDENTIFICATION_VALUE);
                    request.set_total_length(request.ByteSizeLong());
                    req = std::move(request);
                    request.Clear();
                } else {
                    is_sending = false;
                    return;
                }
            } else {
                req = std::move(request_queue.front());
                request_queue.pop();
            }
        }

        grpc::ClientContext context;
        OperationResponse response;
        grpc::Status status = stub_->OperationService(&context, req, &response);

        if (!status.ok()) {
            std::cerr << "RPC failed: " << status.error_message() << std::endl;
        }
    }
}

// FlushBuffer 函数
bool NetClient::FlushBuffer() {
    {
        std::unique_lock<std::mutex> lock(mtx);
        if (request.keys_size() > 0) {
            request.set_identification(IDENTIFICATION_VALUE);
            request.set_total_length(request.ByteSizeLong());
            request_queue.push(std::move(request));
            request.Clear();
        }

        if (!is_sending.exchange(true)) {
            // 启动发送线程
            std::thread(&NetClient::SendRequestsFromQueue, this).detach();
        }
    }

    // 等待所有请求发送完毕
    while (is_sending.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return true;
}