#ifndef RDB_CLIENT_H
#define RDB_CLIENT_H

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "netservice.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using netservice::NetService;
using netservice::OperationRequest;
using netservice::OperationResponse;

class NetClient {
 public:
  NetClient(const std::vector<std::shared_ptr<grpc::Channel>>& channels, int batch_size = 12000,
            size_t queue_size = 10);
  ~NetClient();

  bool SingleWriter(const std::string& operation, const std::string& key,
                    std::string& value);
  bool BufferedWriter(const std::string& operation, const std::string& key,
                      const std::string& value);
  bool FlushBuffer();
  void Shutdown();

 private:
  void ProcessorThread(int stub_index);  // one stub for one client
  void StartThreadPool();
  bool SendRequest(OperationRequest& request, NetService::Stub* stub);
  void SetOperation(OperationRequest& request, const std::string& operation);

  static constexpr uint32_t IDENTIFICATION_VALUE = 0xABCD;
  const int BATCH_SIZE;
  const size_t QUEUE_SIZE;

  std::vector<std::unique_ptr<NetService::Stub>> stubs_; 
  std::mutex mutex_;
  std::condition_variable condition_;
  std::queue<OperationRequest> request_queue_;
  std::atomic<bool> running_{true};
  std::vector<std::thread> worker_threads_;

  // Reusable objects for performance
  OperationRequest current_batch_;
};

#endif