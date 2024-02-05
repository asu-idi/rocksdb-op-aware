#include <cstdio>
#include <string>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"

#include "plugin/hdfs/env_hdfs.h"
#include "hdfs.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;
using ROCKSDB_NAMESPACE::DestroyDB;

std::string kDBPath = "test/rdb_hdfs";

int main() {
    DB *db;

    std::unique_ptr<rocksdb::Env> hdfs;
    rocksdb::NewHdfsEnv("hdfs://10.218.106.144:9000/", &hdfs);

    Options options;
    options.env = hdfs.get();
    options.create_if_missing = true;

    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    
    // Open DB
    Status s = DB::Open(options, kDBPath, &db);
    std::cout << "Created table at " << kDBPath << "\n";

    // Write 1st pair
    s = db->Put(WriteOptions(), "game5", "apex");
    assert(s.ok());

    // Write 2nd pair
    s = db->Put(WriteOptions(), "game6", "pubg");
    assert(s.ok());

    // Iterate over entire db
    std::cout << "Iterating over the entire db now!\n";
    rocksdb::Iterator *it = db->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::cout << it->key().ToString() << ": " << it->value().ToString() << "\n";
    }
    assert(it->status().ok());

    s = DestroyDB(kDBPath, options);
    // s = db->Close();
    assert(s.ok());

    return 0;
}
