// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_RUNTIME_INSERT_EXEC_STATE_H
#define IMPALA_RUNTIME_INSERT_EXEC_STATE_H

#include <string>
#include <map>
#include <boost/unordered_map.hpp>
#include <boost/thread/mutex.hpp>

#include "common/status.h"
#include "common/hdfs.h"

namespace impala {

class TInsertExecStatus;
class TInsertResult;
class TInsertStats;
class TFinalizeParams;
class TUpdateCatalogRequest;
class TInsertPartitionStatus;
class RuntimeProfile;
class HdfsTableDescriptor;

/// InsertExecState manages the state related to the execution of an Insert
/// statement (creation of new files, new partitions, etc.):
/// - per-partition status
/// - collection of files to be moved on finalization
///
/// Thread-safe.
class InsertExecState {
 public:
  /// Counts how many rows an INSERT query has added to a particular partition
  /// (partitions are identified by their partition keys: k1=v1/k2=v2
  /// etc. Unpartitioned tables have a single 'default' partition which is
  /// identified by ROOT_PARTITION_KEY.
  typedef std::map<std::string, TInsertPartitionStatus> PartitionStatusMap;

  /// Tracks files to move from a temporary (key) to a final destination (value) as
  /// part of query finalization. If the destination is empty, the file is to be
  /// deleted.
  typedef std::map<std::string, std::string> FileMoveMap;

  /// Merge values from insert_exec_status.
  void Update(const TInsertExecStatus& insert_exec_status);

  /// Add a new partition with the given parameters. Ignores base_dir if nullptr.
  /// It is an error to call this for an existing partition.
  void AddPartition(const std::string& name, int64_t id, const std::string* base_dir);

  /// Merge given values into stats for partition with name partition_name.
  /// Ignores insert_stats if nullptr.
  /// Requires that the partition already exist.
  void UpdatePartition(const std::string& partition_name,
      int64_t num_modified_rows_delta, const TInsertStats* insert_stats);

  /// Add a new Kudu partition.
  /// It is an error to call this for an existing partition.
  void AddKuduPartition(const std::string& name, int64_t id);

  /// Update stats for Kudu partition with name partition_name with given values.
  /// Requires that the partition already exist.
  void SetKuduPartition(const std::string& partition_name,
      int64_t num_modfied_rows, int64_t num_row_errors, int64_t latest_ts);

  /// Adds new file/location to the move map.
  void AddFileToMove(const std::string& file_name, const std::string& location);

  /// Outputs the partition stats to a string.
  std::string OutputPartitionStats(const std::string& prefix);

  /// Returns the latest Kudu timestamp observed across any backends where DML into Kudu
  /// was executed, or 0 if there were no Kudu timestamps reported.
  uint64_t GetLatestKuduInsertTimestamp();

  /// Return the total number of modified rows across all partitions.
  int64_t GetNumModifiedRows();

  /// Populates catalog_update with PartitionStatusMap data.
  /// Returns true if a catalog update is required, false otherwise.
  bool PrepareCatalogUpdate(TUpdateCatalogRequest* catalog_update);

  /// Moves all temporary staging files to their final destinations, as indicated
  /// by params, and creates new partitions for hdfs_table as required.
  /// Adds child timers to profile for the various stages of finalization.
  /// If the table is on an S3 path and s3_skip_insert_staging is true, does not
  /// create new partition directories.
  Status FinalizeInsert(const TFinalizeParams& params, bool s3_skip_insert_staging,
    HdfsTableDescriptor* hdfs_table, RuntimeProfile* profile) WARN_UNUSED_RESULT;

  void ToThrift(TInsertExecStatus* insert_status);

  void ToTInsertResult(TInsertResult* insert_result);

 private:
  // protects all fields below
  boost::mutex lock_;

  PartitionStatusMap per_partition_status_;
  FileMoveMap files_to_move_;

  /// Determines what the permissions of directories created by INSERT statements should
  /// be if permission inheritance is enabled. Populates a map from all prefixes of
  /// path_str (including the full path itself) which is a path in Hdfs, to pairs
  /// (does_not_exist, permissions), where does_not_exist is true if the path does not
  /// exist in Hdfs. If does_not_exist is true, permissions is set to the permissions of
  /// the most immediate ancestor of the path that does exist, i.e. the permissions that
  /// the path should inherit when created. Otherwise permissions is set to the actual
  /// permissions of the path. The PermissionCache argument is also used to cache the
  /// output across repeated calls, to avoid repeatedly calling hdfsGetPathInfo() on the
  /// same path.
  typedef boost::unordered_map<std::string, std::pair<bool, short>> PermissionCache;
  void PopulatePathPermissionCache(hdfsFS fs, const std::string& path_str,
      PermissionCache* permissions_cache);

  /// Merge src into dst. Not thread-safe.
  void MergeInsertStats(const TInsertStats& src, TInsertStats* dst);
};

}

#endif
