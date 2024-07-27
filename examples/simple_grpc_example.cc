// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <iostream>

#include "plugin/netservice/rdb_client.h"

int main() {
    std::string server_address = "0.0.0.0:50050";
    
    // To be supplied by the benchmark
    std::string operation = "Put";
    std::string key_prefix = "key";
    std::string value_prefix = "value";

    // Create the NetClient instance once
    NetClient client(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));

    // Start the streaming RPC
    // if (!client.StartStream()) {
    //     std::cerr << "Failed to start stream" << std::endl;
    //     return 1;
    // } else {
    //     std::cout << "Stream started" << std::endl;
    // }

    // Loop 1000000 times to send data
    for (int i = 0; i < 1000000; i++) {
        std::string key = key_prefix + std::to_string(i);
        std::string value = value_prefix + std::to_string(i);

        // if (!client.WriteToStream(operation, key, value)) {
        //     std::cerr << "Failed to write to stream" << std::endl;
        //     break;
        // }
        client.OperationService(operation, key, value);
    }

    // Finish the streaming RPC and get the result
    // std::string result = client.FinishStream();

    // Print the result
    // std::cout << "Result: " << result << std::endl;

    return 0;
}
