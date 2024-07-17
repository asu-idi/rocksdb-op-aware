#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <grpcpp/grpcpp.h>
#include "rdb_client.h"


ThreadPool::ThreadPool(size_t numThreads) : stop(false) {
    for (size_t i = 0; i < numThreads; ++i) {
        workers.emplace_back(&ThreadPool::workerThread, this);
    }
}

ThreadPool::~ThreadPool() {
    {
        std::unique_lock<std::mutex> lock(queueMutex);
        stop = true;
    }
    condition.notify_all();
    for (std::thread &worker : workers) {
        worker.join();
    }
}

void ThreadPool::enqueue(std::function<void()> task) {
    {
        std::unique_lock<std::mutex> lock(queueMutex);
        tasks.push(std::move(task));
    }
    condition.notify_one();
}

void ThreadPool::workerThread() {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            condition.wait(lock, [this] { return stop || !tasks.empty(); });
            if (stop && tasks.empty()) return;
            task = std::move(tasks.front());
            tasks.pop();
        }
        task();
    }
}

NetClient::NetClient(std::shared_ptr<grpc::Channel> channel, size_t threadPoolSize)
    : stub_(NetService::NewStub(channel)), threadPool(threadPoolSize) {}

std::string NetClient::GetBatchData(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mtx);
    request.add_keys(key);
    request.add_values(value);
    return "OK";
}

bool NetClient::StartStream() {
    std::lock_guard<std::mutex> lock(mtx);
    stream_writer_ = stub_->OperationService(&stream_context_, &stream_response_);
    return stream_writer_ != nullptr;
}

bool NetClient::WriteToStream(const std::string& operation, const std::string& key, const std::string& value) {
    threadPool.enqueue([this, operation, key, value] { writeOperation(operation, key, value); });
    return true;
}

void NetClient::writeOperation(const std::string& operation, const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mtx);
    if (!stream_writer_) return;

    if (operation == "Put") {
        request.set_operation(OperationRequest::Put);
    } else if (operation == "Get") {
        request.set_operation(OperationRequest::Get);
    } else if (operation == "Delete") {
        request.set_operation(OperationRequest::Delete);
    } else if (operation == "BatchPut") {
        request.set_operation(OperationRequest::BatchPut);
    } else {
        return;
    }

    request.add_keys(key);
    request.add_values(value);

    stream_writer_->Write(request);
}

std::string NetClient::FinishStream() {
    std::lock_guard<std::mutex> lock(mtx);
    if (!stream_writer_) return "Stream not started";

    stream_writer_->WritesDone();
    grpc::Status status = stream_writer_->Finish();

    if (status.ok()) {
        return stream_response_.result();
    } else {
        return "RPC failed";
    }
}
