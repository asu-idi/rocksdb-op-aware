#include <iostream>
#include <memory>
#include <string>
#include <thread>

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
#include "plugin/hdfs/env_hdfs.h"
#include "hdfs.h"

// UDP
#include <bits/stdc++.h> 
#include <stdlib.h> 
#include <unistd.h> 
#include <sys/types.h> 
#include <sys/socket.h> 
#include <arpa/inet.h> 
#include <netinet/in.h> 
#include <queue>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerCompletionQueue;

using netservice::OperationRequest;
using netservice::OperationResponse;
using netservice::NetService;

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

// Global variables
std::queue<std::string> optimizationQueue;
rocksdb::DB* db_;

class NetServiceImpl final {
public:
    NetServiceImpl(const std::string& db_path) {
        Options options;
        options.create_if_missing = true;
        // Load options from a file
        ConfigOptions config_options;
        std::vector<ColumnFamilyDescriptor> cf_descs;

        rocksdb::Status status = rocksdb::LoadOptionsFromFile(config_options, "../db_bench_options.ini", &options, &cf_descs);

        // std::unique_ptr<rocksdb::Env> hdfs;
        // rocksdb::NewHdfsEnv("hdfs://localhost:9000/", &hdfs);

        // options.env = hdfs.get();

        status = DB::Open(options, db_path, &db_);
        if (!status.ok()) {
            std::cerr << "Error opening database: " << status.ToString() << std::endl;
            exit(1);
        }
    }

    ~NetServiceImpl() {
        server_->Shutdown();
        cq_->Shutdown();

        delete db_;
    }

    void RunGRPCServer(const std::string& server_address) {
        ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service_);

        cq_ = builder.AddCompletionQueue();
        server_ = builder.BuildAndStart();

        std::cout << "Server listening on " << server_address << std::endl;
        HandleRPCs();
    }

private:
    class CallData {
    public:
        CallData(NetService::AsyncService* service, ServerCompletionQueue* cq, rocksdb::DB* db)
            : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE), db_(db) {
            Proceed();
        }

        void Proceed() {
            if (status_ == CREATE) {
                status_ = PROCESS;
                service_->RequestOperationService(&ctx_, &request_, &responder_, cq_, cq_, this);
            } else if (status_ == PROCESS) {
                new CallData(service_, cq_, db_);
                HandleRequest();
                status_ = FINISH;
                responder_.Finish(response_, Status::OK, this);
            } else {
                delete this;
            }
        }

    private:
        void HandleRequest() {
            fprintf(stderr, "Received request for operation: %d\n", request_.operation());
            switch (request_.operation()) {
                case OperationRequest::Put: {
                    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), request_.keys(0), request_.values(0));
                    response_.set_result(status.ok() ? "OK" : status.ToString());
                    break;
                }
                case OperationRequest::BatchPut: {
                    rocksdb::WriteBatch batch;
                    for (int i = 0; i < request_.keys_size(); i++) {
                        batch.Put(request_.keys(i), request_.values(i));
                    }
                    rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
                    response_.set_result(status.ok() ? "OK" : status.ToString());
                    break;
                }
                case OperationRequest::Get: {
                    std::string value;
                    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), request_.keys(0), &value);
                    response_.set_result(status.ok() ? value : status.ToString());
                    break;
                }
                case OperationRequest::Delete: {
                    rocksdb::Status status = db_->Delete(rocksdb::WriteOptions(), request_.keys(0));
                    response_.set_result(status.ok() ? "OK" : status.ToString());
                    break;
                }
                default:
                    response_.set_result("Unknown operation");
            }
        }

        NetService::AsyncService* service_;
        ServerCompletionQueue* cq_;
        ServerContext ctx_;
        OperationRequest request_;
        OperationResponse response_;
        ServerAsyncResponseWriter<OperationResponse> responder_;
        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;
        rocksdb::DB* db_;
    };

    void HandleRPCs() {
        new CallData(&service_, cq_.get(), db_);
        void* tag;
        bool ok;
        while (true) {
            cq_->Next(&tag, &ok);
            static_cast<CallData*>(tag)->Proceed();
        }
    }

    NetService::AsyncService service_;
    std::unique_ptr<Server> server_;
    std::unique_ptr<ServerCompletionQueue> cq_;
};

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
      
    if (bind(sockfd, (const struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) { 
        perror("bind failed"); 
        exit(EXIT_FAILURE); 
    }

    while (true) {
        socklen_t cliAddrSize = sizeof(cliaddr);
        int bytesReceived = recvfrom(sockfd, buffer, sizeof(buffer), 0, (struct sockaddr *)&cliaddr, &cliAddrSize);
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

int main() {
    std::string server_address = "0.0.0.0:50050";
    std::string db_path = "/data/viraj/tmp_db";
    int thread_pool_size = 10;
 
    // Create a thread to run the UDP server
    std::thread udp_server(RunUDPServer);
    udp_server.detach();

    // Run the Optimization service
    std::thread optimization_service(RunOptimizationService);
    optimization_service.detach();

    NetServiceImpl service(db_path);
    service.RunGRPCServer(server_address);

    return 0;
}
