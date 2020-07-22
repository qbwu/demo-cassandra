/***************************************************************************
 *
 * Copyright (c) 2018 qbwu All Rights Reserved
 *
 **************************************************************************/

/**
 * @file cass_hll_dao.hpp
 * @author qb.wu@outlook.com
 * @date 2018/03/22 16:00:14
 * @brief
 *
 **/

#include <cassert>
#include <mutex>
#include <unordered_map>
#include <functional>
#include <base/logging.h>
#include "utility.h"

namespace offline {
namespace uv_srvs {

const std::string CONF_KEY_CASSCLI = "CassCli";
const std::string CONF_KEY_IP = "IPList";
const std::string CONF_KEY_PORT = "Port";
const std::string CONF_KEY_DBMETA_UPD_INTERVAL_MS = "DBMetaUpdIntervalMs";
const std::string CONF_KEY_OPTIONS = "Options";

class PreparedHandle {
public:
    const std::string& summary() const {
        std::lock_guard<bthread::Mutex> lock(_mtx);
        return _cql_str;
    }

    CassPreparedSCPtr get_handle() const {
        std::lock_guard<bthread::Mutex> lock(_mtx);
        return _handle;
    }

    void set_handle(std::string sum, CassPreparedUCPtr handle) {
        std::lock_guard<bthread::Mutex> lock(_mtx);
        _cql_str = std::move(sum);
        _handle = std::move(handle);
    }

private:
    std::string _cql_str;
    mutable bthread::Mutex _mtx;
    CassPreparedSCPtr _handle;
};

template <typename Impl>
CassHLLDao<Impl>::CassHLLDao(const comcfg::Configure &conf)
        : _cluster { cass_cluster_new() },
          _session { cass_session_new() },
          _impl { conf[CONF_KEY_PROPERTIES.data()] } {
    try {
        _connect_cluster(conf[CONF_KEY_CASSCLI.data()]);

        auto &app_conf = conf[CONF_KEY_APP.data()];
        _max_num_query_key = app_conf[CONF_KEY_MAX_NUM_QUERY_KEY.data()].to_uint32();
        _db_meta_upd_interval_ms = app_conf[CONF_KEY_DBMETA_UPD_INTERVAL_MS.data()].to_int64();
        _intent_tables = parse_table_id_set(app_conf[CONF_KEY_TABLE.data()]);

        conf[CONF_KEY_MAX_RETRY_TIMES.data()].get_uint32(&_max_retry_times);
        conf[CONF_KEY_RETRY_INTERVAL_US.data()].get_uint32(&_retry_interval_us);
    } catch (const std::exception &ex) {
        LOG(ERROR) << "Failed to create CassHLLDao, error: " << ex.what();
        throw std::runtime_error("Initialization Error");
    }
}

template <typename Impl>
CassHLLDao<Impl>::~CassHLLDao() {
    if (_running) {
        _running = false;
        // Wake up the slept background thread at once.
        bthread_stop(_bth);
        bthread_join(_bth, nullptr);
    }
}

template <typename Impl>
inline void CassHLLDao<Impl>::_prepare() {
    for (auto elem : _intent_tables) {
        _prepare_table(elem);
    }
}

template <typename Impl>
void CassHLLDao<Impl>::init() {
    _prepare();
    if (_db_meta_upd_interval_ms > 0) {
        _start_update_thread();
    } else {
        LOG(WARNING) << "Update thread is suppressed since the "
            " _db_meta_upd_interval_ms=" << _db_meta_upd_interval_ms;
    }
}

template <typename Impl>
void CassHLLDao<Impl>::_do_update() {
    _count_down_event.wait();
    while (_running) {
        for (auto &elem : _prep_map) {
            if (_impl.make_prepared_cql(elem.first)
                    != elem.second.summary()) {
                try {
                    _prepare_table(elem.first);
                } catch (const std::exception &ex) {
                    LOG(WARNING) << "Fail to update the prepared statement,"
                        " error: " << ex.what();
                }
            }
        }
        bthread_usleep(_db_meta_upd_interval_ms * 1000);
    }
}

template <typename Impl>
void CassHLLDao<Impl>::_start_update_thread() {

    auto fn = [](void *self) -> void* {
        static_cast<CassHLLDao*>(self)->_do_update();
        return nullptr;

    };
    _running = (bthread_start_background(&_bth, nullptr, fn, this) == 0);
    _count_down_event.signal();

    if (!_running) {
        LOG(ERROR) << "Fail to start the update thread.";
        throw std::runtime_error("Initialization Error");
    }
}

template <typename Impl>
void CassHLLDao<Impl>::_connect_cluster(const comcfg::ConfigUnit &conf) {
    std::string ip = conf[CONF_KEY_IP.data()].to_cstr();
    int port = conf[CONF_KEY_PORT.data()].to_int16();

    CassError rc;
    if ((rc = cass_cluster_set_contact_points(_cluster.get(), ip.data())) != CASS_OK
            || (rc = cass_cluster_set_port(_cluster.get(), port)) != CASS_OK
            // We exchange the performance and availability of read-ops with
            //  those of write-ops, remaining consistency simultaneously.
            || (rc = cass_cluster_set_consistency(_cluster.get(),
                    CASS_CONSISTENCY_ONE)) != CASS_OK) {
        LOG(ERROR) << "Failed to config the cluster, error: " << cass_error_desc(rc)
            << ", ip=" << ip << ", port=" << port;
        throw std::runtime_error("Cassandra Error");
    }

    _config_cluster_opts(conf[CONF_KEY_OPTIONS.data()]);

    auto conn_fut = CassFutureUPtr(
        cass_session_connect(_session.get(), _cluster.get()));

    if ((rc = cass_future_error_code(conn_fut.get())) != CASS_OK) {
        LOG(ERROR) << "Failed to construct the session, error: "
            << cass_error_desc(rc);
        throw std::runtime_error("Cassandra Error");
    }
}

template <typename Impl>
void CassHLLDao<Impl>::_config_cluster_opts(const comcfg::ConfigUnit &conf) {
    using CassFunc = std::function<CassError(CassCluster*, uint32_t)>;
    const std::unordered_map<std::string, CassFunc> opt_setters = {
        { "NumThreadsIO", cass_cluster_set_num_threads_io },
        { "QueueSizeIO", cass_cluster_set_queue_size_io },
        { "CoreConnectionsPerHost", cass_cluster_set_core_connections_per_host },
        { "MaxConnectionsPerHost", cass_cluster_set_max_connections_per_host },
        { "MaxConcurrentCreation", cass_cluster_set_max_concurrent_creation },
        { "MaxConcurrentRequestsThreshold",
            cass_cluster_set_max_concurrent_requests_threshold },
        { "ReconnectWaitTimeMs", [](CassCluster *c, uint32_t v) {
            cass_cluster_set_reconnect_wait_time(c, v); return CASS_OK; } },
        { "ConnectTimeoutMs", [](CassCluster *c, uint32_t v) {
            cass_cluster_set_connect_timeout(c, v); return CASS_OK; } },
        { "RequestTimeoutMs", [](CassCluster *c, uint32_t v) {
            cass_cluster_set_request_timeout(c, v); return CASS_OK; } }
    };

    CassError rc;
    for (auto &elem : opt_setters) {
        if (conf[elem.first.data()].selfType() != comcfg::CONFIG_ERROR_TYPE) {
            rc = elem.second(_cluster.get(), conf[elem.first.data()].to_uint32());
            if (rc == CASS_OK) {
                continue;
            }
            LOG(ERROR) << "Failed to set CassCli.Options: " << elem.first
                << ", error: " << cass_error_desc(rc);
            throw std::logic_error("Configuration Error");
        }
    }
}

template <typename Impl>
void CassHLLDao<Impl>::_prepare_table(TableId table_id) {
    auto cql_clause = _impl.make_prepared_cql(table_id);
    LOG(DEBUG) << "Prepared CQL clause: [" << cql_clause << "]";

    CassError rc;
    auto prep_fut = CassFutureUPtr(
        cass_session_prepare(_session.get(), cql_clause.data()));
    if ((rc = cass_future_error_code(prep_fut.get())) != CASS_OK) {
        LOG(WARNING) << "Failed to prepare statement, error: " << cass_error_desc(rc)
            << "\n Prepared statement: " << cql_clause;
        throw std::runtime_error("Cassandra Error");
    }

    auto prepared = CassPreparedUCPtr(cass_future_get_prepared(prep_fut.get()));
    if (!prepared) {
        LOG(WARNING) << "Failed to get prepared from Cassandra.";
        throw std::runtime_error("Cassandra Error");
    }

    _prep_map[table_id].set_handle(std::move(cql_clause), std::move(prepared));
}

template <typename Impl>
std::vector<typename Impl::Row>
CassHLLDao<Impl>::fetch(const SearchKey<TableKey> &sk) const {
    if (sk.size() > _max_num_query_key) {
        throw std::invalid_argument("Too many keys in a single request.");
    }

    auto num_query = sk.size();
    std::size_t num_fetch_row = 0;
    std::size_t num_row_ok = 0;
    std::size_t num_retry = 0;

    std::vector<Row> ret;
    ret.reserve(sk.size());

    auto start = std::chrono::steady_clock::now();

    auto prof_log = [&start, &num_query, &num_retry,
            &num_fetch_row, &num_row_ok]() {
        auto stop = std::chrono::steady_clock::now();
        LOG(DEBUG) << "{ cass_hll_dao_profile:={\"num_query\": " << num_query
            << ", \"num_retry\": " << num_retry
            << ",\"num_fetch_row\": " << num_fetch_row
            << ", \"num_row_ok\": " << num_row_ok
            << ", \"process_time_ms\": "
            << std::chrono::duration_cast<Milliseconds>(stop - start).count() << "} }";
    };

    try {
        for (auto &elem : _prep_map) {
            auto &tab_keys = sk.get_table_keys(elem.first);
            if (tab_keys.empty()) {
                continue;
            }
            _fetch_from_table(
                elem.first, tab_keys, ret, num_fetch_row, num_row_ok, num_retry);
        }
    } catch (const std::exception &ex) {
        prof_log();
        throw;
    }

    prof_log();
    return ret;
}

template <typename Impl>
void CassHLLDao<Impl>::_fetch_from_table(TableId table_id,
        const std::vector<TableKey> &keys, std::vector<Row> &res,
        std::size_t &num_tot, std::size_t &num_ok, std::size_t &num_retry) const {

    std::vector<std::pair<CassStatementUPtr, CassFutureUPtr>> exec_fut_vec;
    exec_fut_vec.reserve(keys.size());

    auto prepared_stm = _prep_map.at(table_id).get_handle();
    assert(prepared_stm);

    for (auto &elem : keys) {
        auto stm_fut = _exec_once_query(*prepared_stm, table_id, elem);
        if (!stm_fut.first) {
            continue;
        }
        exec_fut_vec.push_back(std::move(stm_fut));
    }

    auto all_done = true;
    int retry = _max_retry_times;
    do {
        all_done = true;
        CassError rc;
        for (auto &elem : exec_fut_vec) {
            auto &stm = elem.first;
            auto &fut = elem.second;
            if (fut && (rc = cass_future_error_code(fut.get())) != CASS_OK) {
                LOG(WARNING) << "Failed to execute, error: " << cass_error_desc(rc);
                fut = CassFutureUPtr(cass_session_execute(_session.get(), stm.get()));
                all_done = false;
                ++num_retry;
            } else if (fut) {
                _parse_result(fut, res, num_tot, num_ok);
                fut.reset(nullptr);
            }
        }
        if (all_done || --retry <= 0) {
            break;
        }
        // 5 ms
        bthread_usleep(_retry_interval_us);
    } while (true);

    if (!all_done) {
        LOG(WARNING) << "Too many failed times, run out of retry times(3).";
        throw std::runtime_error("Cassandra Error");
    }
}

template <typename Impl>
std::pair<CassStatementUPtr, CassFutureUPtr> CassHLLDao<Impl>::_exec_once_query(
    const CassPrepared &prepared, TableId tid, const TableKey &tk) const {
        auto bound_stm = CassStatementUPtr(cass_prepared_bind(&prepared));
        // Execute the queries.
        if (!_impl.bind_statement(tid, tk, bound_stm)) {
            return {};
        }
        auto *stm_ptr = bound_stm.get();
        return { std::move(bound_stm),
             CassFutureUPtr(cass_session_execute(_session.get(), stm_ptr)) };
}

template <typename Impl>
void CassHLLDao<Impl>::_parse_result(
        const CassFutureUPtr &fut, std::vector<Row> &res,
        std::size_t &num_tot, std::size_t &num_ok) const {
    // The order of fields should be consistent with that in _make_prepared_cql
    auto result = CassResultUCPtr(cass_future_get_result(fut.get()));
    auto iterator = CassIteratorUPtr(cass_iterator_from_result(result.get()));

    while (cass_iterator_next(iterator.get())) {
        ++num_tot;
        auto *cass_row = cass_iterator_get_row(iterator.get());

        Row row;
        if (!_impl.parse_row(*cass_row, row)) {
            continue;
        }

        res.push_back(std::move(row));
        ++num_ok;
    }
}

} // uv_srvs
} // offline
