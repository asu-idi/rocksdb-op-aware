#include "rdb_client.h"

OperationRequest request;
OperationRequest request_backup;
NetClient::NetClient(std::shared_ptr<grpc::Channel> channel) : stub_(NetService::NewStub(channel)) {}
std::mutex mtx;
std::queue<OperationRequest> request_queue;
std::condition_variable cv;
bool is_sending = false;

void SetOperation(const std::string& operation) {
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

bool NetClient::BufferedWriter(const std::string& operation, const std::string& key, const std::string& value) {
    SetOperation(operation);
    request.add_keys(key);
    request.add_values(value);

    if (request.keys_size() >= 12000) {
        std::unique_lock<std::mutex> lock(mtx);

        // Backup the current request and queue it
        request_backup = request;
        request_queue.push(request_backup);

        // Clear the original request for new data
        request.clear_keys();
        request.clear_values();

        // If not already sending, start sending requests
        if (!is_sending) {
            is_sending = true;
            lock.unlock(); // Unlock before sending to avoid holding the mutex during the operation
            SendRequestsFromQueue();
        } else {
            // Wait until the current sending is finished
            cv.wait(lock, [this]() { return !is_sending; });
        }

        return true;
    }

    return true;
}

void NetClient::SendRequestsFromQueue() {
    auto sendRequest = [this]() {
        while (true) {
            OperationRequest req;
            {
                std::unique_lock<std::mutex> lock(mtx);
                if (request_queue.empty()) {
                    is_sending = false;
                    cv.notify_all(); // Notify any waiting threads that sending is complete
                    return;
                }
                // Get the next request from the queue
                req = request_queue.front();
                request_queue.pop();
            }
            // Send the request outside of the lock to avoid holding the mutex during the RPC call
            OperationResponse response;
            grpc::ClientContext context;
            grpc::Status status = stub_->OperationService(&context, req, &response);

            if (!status.ok()) {
                std::cerr << "RPC failed: " << status.error_message() << std::endl;
                // Retry logic or error handling can go here
                exit(1);
            }
        }
    };

    // Launch the sending logic in a separate thread
    std::thread(sendRequest).detach();
}

bool NetClient::FlushBuffer() {
    if (request.keys_size() == 0) {
        return true;
    }

    OperationResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub_->OperationService(&context, request, &response);

    if (!status.ok()) {
        std::cerr << "RPC failed: " << status.error_message() << std::endl;
        return false;
    }

    request.clear_keys();
    request.clear_values();
    return true;
}
