#include "rdb_client.h"

NetClient::NetClient(const std::vector<std::shared_ptr<grpc::Channel>>& channels, int batch_size, size_t queue_size)
    : BATCH_SIZE(batch_size), QUEUE_SIZE(queue_size), running_(true) {
  // initialize stubs
  for (const auto& channel : channels) {
    stubs_.emplace_back(NetService::NewStub(channel));
  }
  StartThreadPool();
}

NetClient::~NetClient() {
  Shutdown();
}

void NetClient::StartThreadPool() {
  // one thread for one stub
  for (size_t i = 0; i < stubs_.size(); ++i) {
    worker_threads_.emplace_back(&NetClient::ProcessorThread, this, i);
  }
}

void NetClient::Shutdown() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    running_ = false;
  }
  condition_.notify_all();
  for (auto& thread : worker_threads_) {
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

bool NetClient::SingleWriter(const std::string& operation, const std::string& key,
                             std::string& value) {
  OperationRequest request;
  SetOperation(request, operation);
  request.add_keys(key);
  if (operation != "Get") {
      request.add_values(value);
  }
  // send request to the first stub
  return SendRequest(request, stubs_[0].get());
}

bool NetClient::BufferedWriter(const std::string& operation, const std::string& key,
                               const std::string& value) {
  std::unique_lock<std::mutex> lock(mutex_);
  // if the request queue is full, wait (to avoid too many requests in memory)
  while (request_queue_.size() >= 20*QUEUE_SIZE) {
    condition_.wait(lock);
  }

  current_batch_.add_keys(key);
  current_batch_.add_values(value);

  if (current_batch_.keys_size() >= BATCH_SIZE) {
    SetOperation(current_batch_, operation);
    current_batch_.set_identification(IDENTIFICATION_VALUE);
    current_batch_.set_total_length(current_batch_.ByteSizeLong());

    request_queue_.push(std::move(current_batch_));
    current_batch_.Clear();
    condition_.notify_all(); 
  }
  return true;
}

bool NetClient::FlushBuffer() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (current_batch_.keys_size() > 0) {
    current_batch_.set_identification(IDENTIFICATION_VALUE);
    request_queue_.push(std::move(current_batch_));
    current_batch_.Clear();
    condition_.notify_all();
  }
  return true;
}


void NetClient::ProcessorThread(int stub_index) {
  NetService::Stub* stub = stubs_[stub_index].get();
  while (true) {
    OperationRequest request;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      // wait until there is a request in the queue
      condition_.wait(lock, [this] { return !request_queue_.empty() || !running_; });
      if (!running_ && request_queue_.empty()) {
        break;
      }
      // get request from queue
      request = std::move(request_queue_.front());
      request_queue_.pop();
      // notify BufferedWriter
      condition_.notify_all();
    }
    // send request
    SendRequest(request, stub);
  }
}

bool NetClient::SendRequest(OperationRequest& request, NetService::Stub* stub) {
  ClientContext context;
  OperationResponse response;
  Status status = stub->OperationService(&context, request, &response);

  if (!status.ok()) {
    std::cerr << "RPC failed: " << status.error_message() << std::endl;
    return false;
  }
  return true;
}


