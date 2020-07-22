/***************************************************************************
 *
 * Copyright (c) 2018 qbwu All Rights Reserved
 *
 **************************************************************************/

/**
 * @file cass_hll_dao.h
 * @author qb.wu@outlook.com 
 * @date 2018/03/22 10:11:14
 * @brief
 *
 **/

#ifndef OFFLINE_UV_SRVS_SERVICE_CASS_HLL_DAO_H
#define OFFLINE_UV_SRVS_SERVICE_CASS_HLL_DAO_H

#include <unordered_set>
#include <Configure.h>
#include <bthread.h>
#include <bthread_types.h>
#include <bthread/countdown_event.h>
#include <utils/utils.h>
#include "cass_utils.h"

namespace offline {
namespace uv_srvs {

class PreparedHandle;

template <typename Impl>
class CassHLLDao {
public:
    using Row = typename Impl::Row;
    using TableKey = typename Impl::TableKey;

    explicit CassHLLDao(const comcfg::Configure &conf);
    ~CassHLLDao();

    void init();
    std::vector<Row> fetch(const SearchKey<TableKey> &key) const;

private:
    void _connect_cluster(const comcfg::ConfigUnit &conf);

    void _config_cluster_opts(const comcfg::ConfigUnit &conf);

    void _prepare();

    void _do_update();

    void _start_update_thread();

    void _prepare_table(TableId table_id);

    void _fetch_from_table(TableId table_id,
        const std::vector<TableKey> &keys, std::vector<Row> &res,
        std::size_t &num_tot, std::size_t &num_ok,
        std::size_t &num_retry) const;

    std::pair<CassStatementUPtr, CassFutureUPtr> _exec_once_query(
        const CassPrepared &prepared, TableId tid, const TableKey &tk) const;

    void _parse_result(const CassFutureUPtr &fut, std::vector<Row> &res,
        std::size_t &num_tot, std::size_t &num_ok) const;

    CassClusterUPtr _cluster;
    CassSessionUPtr _session;

    Impl _impl;

    // table_id -> { cql, prepared_stm }
    std::map<TableId, PreparedHandle> _prep_map;

    std::atomic<bool> _running = { false };
    bthread_t _bth;
    bthread::CountdownEvent _count_down_event;

    uint32_t _max_retry_times = 3;
    uint32_t _retry_interval_us = 5000;

    int64_t _db_meta_upd_interval_ms = -1;
    std::size_t _max_num_query_key = 0;
    utils::unordered_set<TableId> _intent_tables;
};

using LBSCassHLLDao = CassHLLDao<LBSCassHLLDaoImpl>;
using StoreVisitCassHLLDao = CassHLLDao<LBSCassHLLDaoImpl>;
using ResidentCassHLLDao = CassHLLDao<ResidentCassHLLDaoImpl>;

} // offline
} // uv_srvs

#include "cass_hll_dao.hpp"

#endif // OFFLINE_UV_SRVS_SERVICE_CASS_HLL_DAO_H
