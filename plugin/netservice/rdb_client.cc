#include "rdb_client.h"

ThreadPool::ThreadPool(size_t num_threads) : stop_(false) {
    for (size_t i = 0; i < num_threads; ++i) {
        workers_.emplace_back([this]() {
            for (;;) {
                std::function<void()> task;

                {
                    std::unique_lock<std::mutex> lock(this->queue_mutex_);
                    this->condition_.wait(lock, [this]() {
                        return this->stop_ || !this->tasks_.empty();
                    });
                    if (this->stop_ && this->tasks_.empty())
                        return;
                    task = std::move(this->tasks_.front());
                    this->tasks_.pop();
                }

                task();
            }
        });
    }
}

ThreadPool::~ThreadPool() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        stop_ = true;
    }
    condition_.notify_all();
    for (std::thread& worker : workers_)
        worker.join();
}

void ThreadPool::Enqueue(std::function<void()> task) {
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (stop_)
            throw std::runtime_error("enqueue on stopped ThreadPool");
        tasks_.emplace(std::move(task));
    }
    condition_.notify_one();
}

NetClient::NetClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(NetService::NewStub(channel)),
      cq_thread_(&NetClient::ProcessCompletionQueue, this),
      send_thread_pool_(1) 
{
}

NetClient::~NetClient() {
    FlushBuffer();
    cq_.Shutdown();
    if (cq_thread_.joinable()) {
        cq_thread_.join();
    }
}

void NetClient::SetOperation(OperationRequest& request, const std::string& operation) {
    if (operation == "Put") {
        request.set_operation(OperationRequest::Put);
    } else if (operation == "Get") {
        request.set_operation(OperationRequest::Get);
    } else if (operation == "Delete") {
        request.set_operation(OperationRequest::Delete);
    } else if (operation == "BatchPut") {
        request.set_operation(OperationRequest::BatchPut);
    } else {
        request.set_operation(OperationRequest::Put); // 默认操作
    }
}

bool NetClient::BufferedWriter(const std::string& operation, const std::string& key, const std::string& value) {
    std::unique_lock<std::mutex> lock(mtx_);
    SetOperation(request_, operation);
    request_.add_keys(key);
    request_.add_values(value);

    if (request_.keys_size() >= 12000) {
        request_.set_identification(IDENTIFICATION_VALUE);
        request_.set_total_length(request_.ByteSizeLong());
        request_.set_sequence_number(sequence_number_++); // 设置序列号

        OperationRequest temp_request;
        temp_request.Swap(&request_);
        request_.Clear();

        {
            // 将请求保存到待发送映射中
            std::lock_guard<std::mutex> pending_lock(pending_requests_mtx_);
            pending_requests_[temp_request.sequence_number()] = temp_request;
        }

        // 将发送任务添加到线程池
        send_thread_pool_.Enqueue([this, temp_request]() {
            SendBufferedRequests(temp_request);
        });
    }
    return true;
}

bool NetClient::FlushBuffer() {
    std::unique_lock<std::mutex> lock(mtx_);
    if (request_.keys_size() > 0) {
        request_.set_identification(IDENTIFICATION_VALUE);
        request_.set_total_length(request_.ByteSizeLong());
        request_.set_sequence_number(sequence_number_++); // 设置序列号

        OperationRequest temp_request;
        temp_request.Swap(&request_);
        request_.Clear();

        {
            // 将请求保存到待发送映射中
            std::lock_guard<std::mutex> pending_lock(pending_requests_mtx_);
            pending_requests_[temp_request.sequence_number()] = temp_request;
        }

        // 将发送任务添加到线程池
        send_thread_pool_.Enqueue([this, temp_request]() {
            SendBufferedRequests(temp_request);
        });
    }
    return true;
}

void NetClient::SendBufferedRequests(OperationRequest request) {
    // 异步 gRPC 调用
    AsyncClientCall* call = new AsyncClientCall;
    call->sequence_number = request.sequence_number();
    call->response_reader = stub_->AsyncOperationService(&call->context, request, &cq_);
    call->response_reader->Finish(&call->response, &call->status, (void*)call);
}

void NetClient::ProcessCompletionQueue() {
    void* got_tag;
    bool ok = false;
    while (cq_.Next(&got_tag, &ok)) {
        AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
        if (ok) {
            if (!call->status.ok()) {
                std::cerr << "RPC failed: " << call->status.error_message() << std::endl;
            } else {
                // 根据序列号找到对应的请求
                std::lock_guard<std::mutex> pending_lock(pending_requests_mtx_);
                auto it = pending_requests_.find(call->sequence_number);
                if (it != pending_requests_.end()) {
                    // 处理成功的响应
                    pending_requests_.erase(it);
                }
            }
        } else {
            std::cerr << "RPC failed" << std::endl;
        }
        delete call;
    }
}

bool NetClient::SingleWriter(const std::string& operation, const std::string& key, std::string& value) {
    OperationRequest request;
    SetOperation(request, operation);
    request.set_identification(IDENTIFICATION_VALUE);
    request.add_keys(key);
    request.set_total_length(request.ByteSizeLong());
    request.set_sequence_number(sequence_number_++); // 设置序列号

    if (operation != "Get") {
        request.add_values(value);
    }

    OperationResponse response;
    ClientContext context;
    Status status = stub_->OperationService(&context, request, &response);

    if (status.ok()) {
        if (operation == "Get") {
            value = response.get_result();
        }
        return true;
    } else {
        std::cerr << "RPC failed: " << status.error_message() << std::endl;
        return false;
    }
}