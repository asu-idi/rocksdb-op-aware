#include "rdb_client.h"

NetClient::NetClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(NetService::NewStub(channel)) {
    cq_thread_ = std::thread(&NetClient::ProcessCompletionQueue, this);
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
    }
}

bool NetClient::SingleWriter(const std::string& operation, const std::string& key, std::string& value) {
    OperationRequest request;
    SetOperation(request, operation);
    request.add_keys(key);
    if (operation != "Get") {
        request.add_values(value);
    }

    OperationResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub_->OperationService(&context, request, &response);

    if (!status.ok()) {
        std::cerr << "RPC failed: " << status.error_message() << std::endl;
        return false;
    }

    if (operation == "Get") {
        value = response.get_result();
    }

    return true;
}

bool NetClient::BufferedWriter(const std::string& operation, const std::string& key, const std::string& value) {
    {
        std::unique_lock<std::mutex> lock(mtx_);
        SetOperation(request_, operation);
        request_.add_keys(key);
        request_.add_values(value);

        if (request_.keys_size() >= 12000) {
            request_.set_identification(IDENTIFICATION_VALUE);
            request_.set_total_length(request_.ByteSizeLong());

            OperationRequest temp_request;
            temp_request.Swap(&request_);
            request_.Clear();

            // asynchronize grpc call
            AsyncClientCall* call = new AsyncClientCall;
            call->response_reader = stub_->AsyncOperationService(&call->context, temp_request, &cq_);
            call->response_reader->Finish(&call->response, &call->status, (void*)call);
        }
    }
    return true;
}

bool NetClient::FlushBuffer() {
    {
        std::unique_lock<std::mutex> lock(mtx_);
        if (request_.keys_size() > 0) {
            request_.set_identification(IDENTIFICATION_VALUE);
            request_.set_total_length(request_.ByteSizeLong());

            OperationRequest temp_request;
            temp_request.Swap(&request_);
            request_.Clear();

            // asynchronize grpc call
            AsyncClientCall* call = new AsyncClientCall;
            call->response_reader = stub_->AsyncOperationService(&call->context, temp_request, &cq_);
            call->response_reader->Finish(&call->response, &call->status, (void*)call);
        }
    }
    return true;
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
            }
        } else {

            std::cerr << "RPC failed" << std::endl;
        }
        delete call;
    }
}