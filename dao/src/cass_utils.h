/***************************************************************************
 *
 * Copyright (c) 2018 qbwu All Rights Reserved
 *
 **************************************************************************/

/**
 * @file cass_utils.h
 * @author qb.wu@outlook.com
 * @date 2018/03/26 10:11:14
 * @brief
 *
 **/

#ifndef OFFLINE_UV_SRVS_COMMON_CASS_UTILS_H
#define OFFLINE_UV_SRVS_COMMON_CASS_UTILS_H

#include <memory>
#include <cassandra.h>

namespace offline {
namespace uv_srvs {

#undef CASS_OBJ_PTR
#undef CASS_OBJ_CPTR

#define CASS_OBJ_PTR(obj, free)                       \
    struct obj ## Deleter {                            \
        void operator()(obj *p) { free(p); }           \
    };                                                 \
    using obj ## UPtr                                  \
        = std::unique_ptr<obj, obj ## Deleter>;        \
    using obj ## SPtr = std::shared_ptr<obj>;


#define CASS_OBJ_CPTR(obj, free)                      \
    struct obj ## Deleter {                            \
        void operator()(const obj *p) { free(p); }     \
    };                                                 \
    using obj ## UCPtr                                 \
        = std::unique_ptr<const obj, obj ## Deleter>;  \
    using obj ## SCPtr = std::shared_ptr<const obj>;

CASS_OBJ_PTR(CassCluster, cass_cluster_free);
CASS_OBJ_PTR(CassSession, cass_session_free);
CASS_OBJ_PTR(CassFuture, cass_future_free);
CASS_OBJ_PTR(CassStatement, cass_statement_free);
CASS_OBJ_PTR(CassIterator, cass_iterator_free);

CASS_OBJ_CPTR(CassPrepared, cass_prepared_free);
CASS_OBJ_CPTR(CassResult, cass_result_free);

#undef CASS_OBJ_PTR
#undef CASS_OBJ_CPTR

} // uv_srvs
} // offline

#endif // OFFLINE_UV_SRVS_COMMON_CASS_UTILS_H
