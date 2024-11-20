#include "rdb_client.h"

NetClient::NetClient(std::shared_ptr<grpc::Channel> channel, int batch_size, size_t queue_size)
    : BATCH_SIZE(batch_size), QUEUE_SIZE(queue_size), stub_(NetService::NewStub(channel)), running_(true) {
    // processor_thread_ = std::thread(&NetClient::ProcessorThread, this);
    StartThreadPool(4);
}

NetClient::~NetClient() {
    Shutdown();
}

// void NetClient::Shutdown() {
//     {
//         std::lock_guard<std::mutex> lock(mutex_);
//         running_ = false;
//     }
//     condition_.notify_all();
//     if (processor_thread_.joinable()) {
//         processor_thread_.join();
//     }
// }

void NetClient::StartThreadPool(int num_threads) {
    running_ = true;
    for (int i = 0; i < num_threads; ++i) {
        worker_threads_.emplace_back(&NetClient::ProcessorThread, this);
    }
}

void NetClient::Shutdown() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        running_ = false;
    }
    condition_.notify_all();
    for (std::thread& thread : worker_threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void NetClient::SetOperation(OperationRequest& request, const std::string& operation) {
    if (operation == "Put") request.set_operation(OperationRequest::Put);
    else if (operation == "Get") request.set_operation(OperationRequest::Get);
    else if (operation == "Delete") request.set_operation(OperationRequest::Delete);
    else if (operation == "BatchPut") request.set_operation(OperationRequest::BatchPut);
}

bool NetClient::SingleWriter(const std::string& operation, const std::string& key, std::string& value) {
    OperationRequest request;
    SetOperation(request, operation);
    request.add_keys(key);
    if (operation != "Get") {
        request.add_values(value);
    }
    return SendRequest(request);
}

bool NetClient::BufferedWriter(const std::string& operation, const std::string& key, const std::string& value) {
    std::unique_lock<std::mutex> lock(mutex_);
    condition_.wait(lock, [this] { return request_queue_.size() < QUEUE_SIZE; });

    current_batch_.add_keys(key);
    current_batch_.add_values(value);

    if (current_batch_.keys_size() >= BATCH_SIZE) {
        SetOperation(current_batch_, operation);
        current_batch_.set_identification(IDENTIFICATION_VALUE);
        current_batch_.set_total_length(current_batch_.ByteSizeLong());
        
        request_queue_.push(std::move(current_batch_));
        condition_.notify_one(); // Notify the processor thread
    }

    return true;
}

bool NetClient::FlushBuffer() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (current_batch_.keys_size() > 0) {
        current_batch_.set_identification(IDENTIFICATION_VALUE);
        request_queue_.push(std::move(current_batch_));
        condition_.notify_one(); // Notify the processor thread
    }
    return true;
}

void NetClient::ProcessorThread() {
    while (running_ || !request_queue_.empty()) {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.wait(lock, [this] { return !request_queue_.empty() || !running_; });

        if (!running_ && request_queue_.empty()) {
            break;
        }

        if (!request_queue_.empty()) {
            OperationRequest request = std::move(request_queue_.front());
            request_queue_.pop();
            lock.unlock();  // Release the lock before sending the request

            SendRequest(request);
            condition_.notify_one();  // Notify BufferedWriter that space is available
        }
    }
}

// void NetClient::ProcessorThread() {
//     while (running_ || !request_queue_.empty()) {
//         std::unique_lock<std::mutex> lock(mutex_);
//         condition_.wait(lock, [this] { return !request_queue_.empty() || !running_; });

//         if (!running_ && request_queue_.empty()) {
//             break;
//         }

//         OperationRequest request = std::move(request_queue_.front());
//         request_queue_.pop();
//         lock.unlock(); // Release the lock before sending the request

//         SendRequest(request);
//         condition_.notify_one(); // Notify BufferedWriter that space is available
//     }
// }

bool NetClient::SendRequest(OperationRequest& request) {
    grpc::ClientContext context;
    grpc::Status status = stub_->OperationService(&context, request, &response_);

    if (!status.ok()) {
        std::cerr << "RPC failed: " << status.error_message() << std::endl;
        return false;
    }

    // To Do: Add response handling
    // if (request.operation() == OperationRequest::Get) {
    //     std::cout << "Get response: " << response_.get_result() << std::endl;
    // }

    return true;
}
