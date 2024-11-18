#include <gflags/gflags.h>
#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <condition_variable>
#include <mutex>

// GRPC
#include <grpcpp/grpcpp.h>

#include "netservice.grpc.pb.h"

// RocksDB
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_util.h"

// HDFS + RocksDB-HDFS Plugin
#include "hdfs.h"
#include "plugin/hdfs/env_hdfs.h"

// UDP
#include <arpa/inet.h>
#include <bits/stdc++.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <queue>

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;

using netservice::NetService;
using netservice::OperationRequest;
using netservice::OperationResponse;

using ROCKSDB_NAMESPACE::BlockBasedTableOptions;
using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::ColumnFamilyOptions;
using ROCKSDB_NAMESPACE::CompactionFilter;
using ROCKSDB_NAMESPACE::ConfigOptions;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::DBOptions;
using ROCKSDB_NAMESPACE::NewLRUCache;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::Slice;

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;
using GFLAGS_NAMESPACE::SetVersionString;

// Global variables
std::queue<std::string> optimizationQueue;
rocksdb::DB* db_;
std::unique_ptr<Server> server;
std::condition_variable cv;
std::mutex cv_m;
bool shutdown_requested = false;
int key_count = 0;

// Command line flags
DEFINE_string(hdfs_path, "false", "HDFS path to store data");
DEFINE_string(db, "tmp/test_db", "Path of the RocksDB database");
DEFINE_string(server_address, "0.0.0.0:50050", "Address to run the server");
DEFINE_string(options_file, "../plugin/netservice/db_bench_options.ini",
              "Path to the options file");

class NetServiceImpl final : public NetService::Service {
 public:
  NetServiceImpl() : next_sequence_number_(0), processing_thread_(&NetServiceImpl::ProcessRequests, this) {
    Options options;
    ConfigOptions config_options;
    rocksdb::Status status;
    rocksdb::Env* env = rocksdb::Env::Default();
    std::vector<ColumnFamilyDescriptor> cf_descs;

    if (FLAGS_hdfs_path != "false") {
      static std::shared_ptr<rocksdb::Env> env_guard;
      status = rocksdb::Env::CreateFromUri(config_options, FLAGS_hdfs_path, "",
                                           &env, &env_guard);
      if (!status.ok()) {
        std::cerr << "Error creating env from URI: " << status.ToString() << std::endl;
        exit(1);
      }
    }

    if (FLAGS_options_file != "") {
      config_options.ignore_unknown_options = false;
      config_options.input_strings_escaped = true;
      config_options.env = options.env;
      status = rocksdb::LoadOptionsFromFile(
          config_options, FLAGS_options_file, &options, &cf_descs);
      if (!status.ok()) {
        std::cerr << "Error loading options from file: " << status.ToString() << std::endl;
        exit(1);
      }
    }

    options.create_if_missing = true;
    options.env = env;

    status = DB::Open(options, FLAGS_db, &db_);
    if (!status.ok()) {
      std::cerr << "Error opening database: " << status.ToString() << std::endl;
      exit(1);
    }
  }

  ~NetServiceImpl() {
    {
      std::lock_guard<std::mutex> lock(request_queue_mtx_);
      shutdown_ = true;
    }
    request_queue_cv_.notify_all();
    if (processing_thread_.joinable()) {
      processing_thread_.join();
    }
    delete db_;
  }

  Status OperationService(ServerContext* context,
                        const OperationRequest* request,
                        OperationResponse* response) override {
    // Create a promise for the response
    auto response_promise = std::make_shared<std::promise<OperationResponse>>();
    auto response_future = response_promise->get_future();

    {
      std::lock_guard<std::mutex> lock(request_queue_mtx_);
      request_queue_[request->sequence_number()] = std::make_pair(*request, response_promise);
    }
    request_queue_cv_.notify_one();

    return Status::OK;
  }

 private:
  void ProcessRequests() {
    while (true) {
      std::pair<OperationRequest, std::shared_ptr<std::promise<OperationResponse>>> current_request;
      {
        std::unique_lock<std::mutex> lock(request_queue_mtx_);
        request_queue_cv_.wait(lock, [&]() {
          return shutdown_ || (!request_queue_.empty() && request_queue_.begin()->first == next_sequence_number_);
        });

        if (shutdown_ && request_queue_.empty()) {
          break;
        }

        auto it = request_queue_.begin();
        if (it->first == next_sequence_number_) {
          current_request = it->second;
          request_queue_.erase(it);
        } else {
          continue;
        }
      }

      // process current request
      const OperationRequest& request = current_request.first;
      OperationResponse response;
      rocksdb::Status status;

      // Set the sequence number
      response.set_sequence_number(request.sequence_number());

      switch (request->operation()) {
        case OperationRequest::Put: {
          for (int i = 0; i < request.keys_size(); i++) {
            status = db_->Put(rocksdb::WriteOptions(), request->keys(i),
                              request.values(i));
            if (!status.ok()) {
              fprintf(stderr, "Error putting key: %s\n", status.ToString().c_str());
              response.set_result(status.ToString());
              break;
            }
            key_count++;
          }
          break;
        }
        case OperationRequest::BatchPut: {
          rocksdb::WriteBatch batch;
          for (int i = 0; i < request.keys_size(); i++) {
            batch.Put(request.keys(i), request.values(i));
          }
          status = db_->Write(rocksdb::WriteOptions(), &batch);
          if (!status.ok()) {
            fprintf(stderr, "Error in batch put: %s\n", status.ToString().c_str());
            response.set_result(status.ToString());
            break;
          }
          key_count += request.keys_size();
          break;
        }
        case OperationRequest::Get: {
          std::string value;
          status = db_->Get(rocksdb::ReadOptions(), request.keys(0), &value);
          if (status.ok()) {
            response.set_get_result(value);
          } else if (status.IsNotFound()) {
            response.set_get_result("Key not found");
          } else {
            response.set_get_result("Error: " + status.ToString());
          }
          break;
        }
        case OperationRequest::Delete: {
          status = db_->Delete(rocksdb::WriteOptions(), request.keys(0));
          if (!status.ok()) {
            fprintf(stderr, "Error deleting key: %s\n", status.ToString().c_str());
            response.set_result(status.ToString());
            break;
          }
          key_count--;
          break;
        }
        default:
          fprintf(stderr, "Unknown operation\n");
          response.set_result("Unknown operation");
          break;
      }

      if (status.ok()) {
        response.set_result("OK");
      } else {
        response.set_result(status.ToString());
      }

      fprintf(stderr, "Key count: %d\n", key_count);

      // Set the response in the promise
      current_request.second->set_value(response);
      next_sequence_number_++;
    }
  }

  std::mutex request_queue_mtx_;
  std::condition_variable request_queue_cv_;
  // store copies of OperationRequest and a promise for the response
  std::map<uint64_t, std::pair<OperationRequest, std::shared_ptr<std::promise<OperationResponse>>>> request_queue_;
  uint64_t next_sequence_number_;
  bool shutdown_ = false;
  std::thread processing_thread_;
};

// Signal handler function to shut down the server
void signalHandler(int signum) {
  std::cout << "Interrupt signal (" << signum << ") received. Shutting down the server..." << std::endl;
  {
    std::lock_guard<std::mutex> lk(cv_m);
    shutdown_requested = true;
  }
  cv.notify_one();
}

void RunGRPCServer() {
  NetServiceImpl service;
  ServerBuilder builder;

  builder.AddListeningPort(FLAGS_server_address,
                           grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  server = builder.BuildAndStart();

  std::cout << "Server listening on " << FLAGS_server_address << std::endl;

  // Register signal handler for SIGINT
  std::signal(SIGINT, signalHandler);

  // Wait for shutdown signal
  std::unique_lock<std::mutex> lk(cv_m);
  cv.wait(lk, []{ return shutdown_requested; });

  // Shutdown the server
  server->Shutdown();
  server->Wait();
}

// An additional UDP server to constantly listen for Optimization requests.
void RunUDPServer() {
  int sockfd;
  char buffer[1024];
  struct sockaddr_in servaddr, cliaddr;

  sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  if (sockfd < 0) {
    perror("socket creation failed");
    exit(EXIT_FAILURE);
  }

  memset(&servaddr, 0, sizeof(servaddr));
  memset(&cliaddr, 0, sizeof(cliaddr));

  servaddr.sin_family = AF_INET;
  servaddr.sin_addr.s_addr = INADDR_ANY;
  servaddr.sin_port = htons(8080);

  if (bind(sockfd, (const struct sockaddr*)&servaddr, sizeof(servaddr)) < 0) {
    perror("bind failed");
    exit(EXIT_FAILURE);
  }

  while (true) {
    socklen_t cliAddrSize = sizeof(cliaddr);
    int bytesReceived = recvfrom(sockfd, buffer, sizeof(buffer), 0,
                                 (struct sockaddr*)&cliaddr, &cliAddrSize);
    if (bytesReceived < 0) {
      std::cerr << "Error receiving data." << std::endl;
      continue;
    }

    buffer[bytesReceived] = '\0';
    optimizationQueue.push(buffer);
    std::cout << "Received data: " << buffer << std::endl;
    buffer[0] = '\0';
  }

  close(sockfd);
  return;
}

void RunOptimizationService() {
  while (true) {
    if (!optimizationQueue.empty()) {
      std::string data = optimizationQueue.front();
      optimizationQueue.pop();
      std::cout << "Processing data: " << data << std::endl;
    }
  }
}

int main(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);

  // Create a thread to run the UDP server
  std::thread udp_server(RunUDPServer);
  udp_server.detach();

  // Run the Optimization service
  std::thread optimization_service(RunOptimizationService);
  optimization_service.detach();

  RunGRPCServer();

  return 0;
}