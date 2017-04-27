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

#ifndef IMPALA_RUNTIME_COORDINATOR_H
#define IMPALA_RUNTIME_COORDINATOR_H

#include <vector>
#include <string>
#include <boost/scoped_ptr.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/median.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include "common/global-types.h"
#include "common/hdfs.h"
#include "common/status.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/Types_types.h"
#include "runtime/query-state.h"
#include "runtime/insert-exec-state.h"
#include "scheduling/query-schedule.h"
#include "util/progress-updater.h"

namespace impala {

class CountingBarrier;
class ObjectPool;
class RuntimeState;
class TUpdateCatalogRequest;
class TReportExecStatusParams;
class TPlanExecRequest;
class TRuntimeProfileTree;
class RuntimeProfile;
class QueryResultSet;
class MemTracker;
class PlanRootSink;
class FragmentInstanceState;
class QueryState;


/// Query coordinator: handles execution of fragment instances on remote nodes, given a
/// TQueryExecRequest. As part of that, it handles all interactions with the executing
/// backends; it is also responsible for implementing all client requests regarding the
/// query, including cancellation. Once a query ends, either through cancellation or
/// by returning eos, or by finalizing a DML request, the coordinator releases resources.
///
/// The coordinator monitors the execution status of fragment instances and aborts the
/// entire query if an error is reported by any of them.
///
/// Queries that have results have those results fetched by calling GetNext(). Results
/// rows are produced by a fragment instance that always executes on the same machine as
/// the coordinator.
///
/// Thread-safe, with the exception of GetNext().
///
/// A typical sequence of calls for a single query (calls under the same numbered
/// item can happen concurrently):
/// 1. client: Exec()
/// 2. client: Wait()/client: Cancel()/backend: UpdateBackendExecStatus()
/// 3. client: GetNext()*/client: Cancel()/backend: UpdateBackendExecStatus()
///
/// Query status and cancellation:
/// 1. When an error is encountered, execution is automatically cancelled and the
/// overall query status is set to the first encountered error status.
/// 2. When a query is cancelled via an explicit Cancel() call, the overall query
/// status is also set to CANCELLED and cancellation is initiated for all
/// backends still executing (without an error status).
/// 3. Once a query has returned all rows, cancellation of the remaining executing
/// backends is initiated automatically. The overall query status is OK.
///
/// Thread-safe, unless stated otherwise.
///
/// TODO: initiate cancellation when Exec() fails
/// TODO: move into separate subdirectory and move nested classes into separate files
/// and unnest them
class Coordinator { // NOLINT: The member variables could be re-ordered to save space
 public:
  Coordinator(const QuerySchedule& schedule, RuntimeProfile::EventSequence* events);
  ~Coordinator();

  /// Initiate asynchronous execution of a query with the given schedule. When it returns,
  /// all fragment instances have started executing at their respective backends.
  /// A call to Exec() must precede all other member function calls.
  Status Exec() WARN_UNUSED_RESULT;

  /// Blocks until result rows are ready to be retrieved via GetNext(), or, if the
  /// query doesn't return rows, until the query finishes or is cancelled.
  /// A call to Wait() must precede all calls to GetNext(), and is required for
  /// DML requests. Releases resources in case of error.
  /// Multiple calls to Wait() are idempotent and it is okay to issue multiple
  /// Wait() calls concurrently.
  Status Wait() WARN_UNUSED_RESULT;

  /// Fills 'results' with up to 'max_rows' rows. May return fewer than 'max_rows'
  /// rows, but will not return more.
  ///
  /// If *eos is true, execution has completed. Subsequent calls to GetNext() will be a
  /// no-op.
  ///
  /// GetNext() will not set *eos=true until all fragment instances have either completed
  /// or have failed.
  ///
  /// Returns an error status if an error was encountered either locally or by any of the
  /// remote fragments or if the query was cancelled. Releases resources in case of error.
  ///
  /// GetNext() is not thread-safe: multiple threads must not make concurrent GetNext()
  /// calls (but may call any of the other member functions concurrently with GetNext()).
  Status GetNext(QueryResultSet* results, int max_rows, bool* eos) WARN_UNUSED_RESULT;

  /// Cancel execution of query, if it is still executing. This includes the execution
  /// of the local plan fragment, if any, as well as all plan fragments on remote
  /// nodes. Sets query status to CANCELLED. Idempotent.
  void Cancel();

  /// Updates execution status of a particular backend as well as insert_exec_state_.
  /// Also updates num_remaining_backends_ and cancels execution if the backend has an
  /// error status.
  Status UpdateBackendExecStatus(const TReportExecStatusParams& params)
      WARN_UNUSED_RESULT;

  /// Only valid to call after Exec().
  QueryState* query_state() const { return query_state_; }

  /// Only valid *after* calling Exec(). Return nullptr if the running query does not
  /// produce any rows.
  ///
  /// TODO: The only dependency on this is QueryExecState, used to track memory for the
  /// result cache. Remove this dependency, possibly by moving result caching inside this
  /// class.
  RuntimeState* runtime_state();

  /// Get cumulative profile aggregated over all fragments of the query.
  /// This is a snapshot of the current state of execution and will change in
  /// the future if not all fragments have finished execution.
  RuntimeProfile* query_profile() const { return query_profile_.get(); }

  const TUniqueId& query_id() const;
  MemTracker* query_mem_tracker() const;

  /// This is safe to call only after Wait()
  InsertExecState* insert_exec_state() { return &insert_exec_state_; }

  /// Return error log for coord and all the fragments. The error messages from the
  /// individual fragment instances are merged into a single output to retain readability.
  std::string GetErrorLog();

  const ProgressUpdater& progress() { return progress_; }

  /// Get a copy of the current exec summary. Thread-safe.
  void GetTExecSummary(TExecSummary* exec_summary);

  /// Receive a local filter update from a fragment instance. Aggregate that filter update
  /// with others for the same filter ID into a global filter. If all updates for that
  /// filter ID have been received (may be 1 or more per filter), broadcast the global
  /// filter to fragment instances.
  void UpdateFilter(const TUpdateFilterParams& params);

 private:
  class BackendState;
  struct FilterTarget;
  class FilterState;
  class FragmentStats;

  const QuerySchedule schedule_;

  /// copied from TQueryExecRequest, governs when to call ReportQuerySummary
  TStmtType::type stmt_type_;

  /// BackendStates for all execution backends, including the coordinator.
  /// All elements are non-nullptr. Owned by obj_pool(). Populated by
  /// InitBackendExec().
  std::vector<BackendState*> backend_states_;

  // index into backend_states_ for coordinator fragment; -1 if no coordinator fragment
  int coord_backend_idx_ = -1;

  /// The QueryState for this coordinator. Set in Exec(), released in d'tor.
  QueryState* query_state_ = nullptr;

  /// Non-null if and only if the query produces results for the client; i.e. is of
  /// TStmtType::QUERY. Coordinator uses these to pull results from plan tree and return
  /// them to the client in GetNext(), and also to access the fragment instance's runtime
  /// state.
  ///
  /// Result rows are materialized by this fragment instance in its own thread. They are
  /// materialized into a QueryResultSet provided to the coordinator during GetNext().
  ///
  /// Not owned by this class. Set in Exec(). Reset to nullptr (and the implied
  /// reference of QueryState released) in TearDown().
  FragmentInstanceState* coord_instance_ = nullptr;

  /// Not owned by this class. Set in Exec(). Reset to nullptr in TearDown() or when
  /// GetNext() hits eos.
  PlanRootSink* coord_sink_ = nullptr;

  /// ensures single-threaded execution of Wait(); must not hold lock_ when acquiring this
  boost::mutex wait_lock_;

  bool has_called_wait_ = false;  // if true, Wait() was called; protected by wait_lock_

  /// Keeps track of number of completed ranges and total scan ranges.
  ProgressUpdater progress_;

  /// Total number of filter updates received (always 0 if filter mode is not
  /// GLOBAL). Excludes repeated broadcast filter updates. Set in Exec().
  RuntimeProfile::Counter* filter_updates_received_ = nullptr;

  /// The filtering mode for this query. Set in constructor.
  TRuntimeFilterMode::type filter_mode_;

  /// Tracks the memory consumed by runtime filters during aggregation. Child of
  /// the query mem tracker in 'query_state_' and set in Exec().
  std::unique_ptr<MemTracker> filter_mem_tracker_;

  /// Object pool owned by the coordinator.
  boost::scoped_ptr<ObjectPool> obj_pool_;

  /// Execution summary for a single query.
  /// A wrapper around TExecSummary, with supporting structures.
  struct ExecSummary {
    TExecSummary thrift_exec_summary;

    /// See the ImpalaServer class comment for the required lock acquisition order.
    /// The caller must not block while holding the lock.
    SpinLock lock;

    /// A mapping of plan node ids to index into thrift_exec_summary.nodes
    boost::unordered_map<TPlanNodeId, int> node_id_to_idx_map;

    void Init(const QuerySchedule& query_schedule);
  };

  ExecSummary exec_summary_;

  /// Filled in as the query completes and tracks the results of INSERT queries that
  /// alter the structure of tables. This is either the union of the reports from all
  /// fragment instances, or taken from the coordinator fragment: only one of the two can
  /// legitimately produce updates.
  InsertExecState insert_exec_state_;

  /// Aggregate counters for the entire query.
  boost::scoped_ptr<RuntimeProfile> query_profile_;

  /// Barrier that is released when all calls to BackendState::Exec() have
  /// returned. Initialized in StartBackendExec().
  boost::scoped_ptr<CountingBarrier> exec_rpcs_complete_barrier_;

  /// Barrier that is released when all backends have indicated execution completion
  /// or when any execution error occured. Initialized in StartBackendExec().
  boost::scoped_ptr<CountingBarrier> backend_exec_complete_barrier_;

  /// Event timeline for this query. Not owned.
  RuntimeProfile::EventSequence* query_events_ = nullptr;

  /// total time spent in finalization (typically 0 except for INSERT into hdfs tables)
  RuntimeProfile::Counter* finalization_timer_ = nullptr;

  /// Indexed by fragment idx (TPlanFragment.idx). Filled in InitFragmentStats(),
  /// elements live in obj_pool().
  /// Updated by BackendState sequentially, without synchronization.
  std::vector<FragmentStats*> fragment_stats_;

  /// EXECUTING: in-flight, non-error execution; the only non-terminal state
  /// RETURNED_RESULTS: GetNext() set eos to true
  /// CANCELLED: Cancel() was called (not: someone called CancelInternal())
  /// ERROR: received an error from a backend
  /// ERROR_AFTER_RETURNED_RESULTS: received an error from a backend after GetNext()
  ///   set eos to true
  enum class ExecState {
      EXECUTING, RETURNED_RESULTS, CANCELLED, ERROR, ERROR_AFTER_RETURNED_RESULTS };

  SpinLock exec_state_lock_;  // protects exec_state_ and exec_status_

  /// In the ERROR case, we might initially see a CANCELLED fragment instance
  /// status before an error status from the same backend shows up (one instance's
  /// execution error kicked off the cancellation of all instances, and one of the
  /// cancelled instances status report might shows up before the error report).
  /// This will get fixed when execution status is reported per-QueryState, as opposed
  /// to the current per-FIS.
  ExecState exec_state_ = ExecState::EXECUTING;

  /// overall execution status; only set on exec_state_ transitions:
  /// - EXECUTING: OK
  /// - RETURNED_RESULTS: OK
  /// - CANCELLED: CANCELLED
  /// - ERROR: error status
  /// - ERROR_AFTER_RETURNED_RESULTS: OK
  Status exec_status_;

  /// Protects filter_routing_table_.
  SpinLock filter_lock_;

  /// Map from filter ID to filter.
  typedef boost::unordered_map<int32_t, FilterState> FilterRoutingTable;
  FilterRoutingTable filter_routing_table_;

  /// Set to true when all calls to UpdateFilterRoutingTable() have finished, and it's
  /// safe to concurrently read from filter_routing_table_.
  bool filter_routing_table_complete_ = false;

  /// True if and only if ReleaseResources() has been called.
  bool released_resources_ = false;

  /// Returns a local object pool.
  ObjectPool* obj_pool() { return obj_pool_.get(); }

  /// Returns request's finalize params, or nullptr if not present.
  const TFinalizeParams* finalize_params() const;

  const TQueryCtx& query_ctx() const { return schedule_.request().query_ctx; }

  /// Returns a pretty-printed table of the current filter state.
  std::string FilterDebugString();

  /// Cancels all fragment instances, computes the query summary and releases resources.
  /// Must be called exactly once when transitioning to one of {ERROR,
  /// ERROR_AFTER_RETURNED_RESULTS, CANCELLED}.
  void CancelInternal();

  /// Release execution resources. Not idempotent.
  void ReleaseResources();

  /// If exec_state_ != EXECUTING, does nothing. Otherwise sets exec_state_ to 'state'
  /// and finalizes execution (cancels remaining fragments if transitioning to CANCELLED;
  /// either way, calls ComputeQuerySummary() and releases resources).
  void SetNonErrorTerminalState(ExecState state);

  /// Transitions exec_state_ given a backend error status:
  /// - if exec_state_ already indicates an error, does nothing
  /// - transitions EXECUTING to ERROR and RETURNED_RESULTS to
  ///   ERROR_AFTER_RETURNED_RESULTS
  /// Also updates exec_status_ when transitioning to ERROR (or if we're already in
  /// ERROR, if exec_status_ is CANCELLED, because a backend might have a more interesting
  /// error status; TODO: remove that when switching to per-backend error reporting).
  /// When transitioning for the first time to an error state, calls CancelInternal().
  ///
  /// failed_fragment is the fragment_id that has failed, used for error reporting along
  /// with instance_hostname.
  Status SetErrorState(const Status& status,
      const TUniqueId* failed_fragment, const std::string& instance_hostname)
      WARN_UNUSED_RESULT;

  /// If status is an error status, calls SetErrorState(), otherwise returns
  /// query_status_.
  Status UpdateExecStateOnError(const Status& status,
      const TUniqueId* failed_fragment, const std::string& instance_hostname)
      WARN_UNUSED_RESULT;

  /// Return true if exec_state_is EXECUTING, false otherwise.
  /// Thread-safe.
  bool IsExecuting();

  /// Returns exec_status_. Thread-safe.
  Status GetExecStatus();

  /// Initializes fragment_stats_ and query_profile_. Must be called before
  /// InitBackendStates().
  void InitFragmentStats();

  /// Populates backend_states_ based on schedule_.fragment_exec_params().
  /// BackendState depends on fragment_stats_, which is why InitFragmentStats()
  /// must be called before this function.
  void InitBackendStates();

  /// Computes execution summary info strings for fragment_stats_ and query_profile_.
  /// This waits for backend_exec_complete_barrier_, remote fragments'
  /// profiles must not be updated while this is running.
  void ComputeQuerySummary();

  /// Perform any post-query cleanup required. Called by Wait() only after all fragment
  /// instances have returned, or if the query has failed, in which case it only cleans up
  /// temporary data rather than finishing the INSERT in flight.
  Status FinalizeQuery() WARN_UNUSED_RESULT;

  /// Populates backend_states_, starts query execution at all backends in parallel, and
  /// blocks until startup completes (exec_rpcs_complete_barrier_).
  void StartBackendExec();

  /// Calls CancelInternal() and returns an error if there was any error starting
  /// backend execution.
  /// Also updates query_profile_ with the startup latency histogram.
  Status FinishBackendStartup() WARN_UNUSED_RESULT;

  /// Build the filter routing table by iterating over all plan nodes and collecting the
  /// filters that they either produce or consume.
  void InitFilterRoutingTable();
};

}

#endif
