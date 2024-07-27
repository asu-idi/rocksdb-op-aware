#include "rdb_client.h"

OperationRequest request;
NetClient::NetClient(std::shared_ptr<grpc::Channel> channel) : stub_(NetService::NewStub(channel)) {}

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

    if (request.keys_size() >= 7500) {
        OperationResponse response;
        grpc::ClientContext context;
        grpc::Status status = stub_->OperationService(&context, request, &response);

        if (!status.ok()) {
            std::cerr << "RPC failed: " << status.error_message() << std::endl;
            return false;
        }

        request.clear_keys();
        request.clear_values();
    }

    return true;
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

bool NetClient::OperationService(const std::string& operation, const std::string& key, const std::string& value) {

    if (operation == "Put") {
        request.set_operation(OperationRequest::Put);
    } else if (operation == "Get") {
        request.set_operation(OperationRequest::Get);
    } else if (operation == "Delete") {
        request.set_operation(OperationRequest::Delete);
    } else if (operation == "BatchPut") {
        request.set_operation(OperationRequest::BatchPut);
    } else {
        return false;
    }

    request.add_keys(key);
    request.add_values(value);

    OperationResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub_->OperationService(&context, request, &response);

    if (!status.ok()) {
        std::cerr << "RPC failed: " << status.error_message() << std::endl;
        return false;
    }

    return true;
}
