/**
 * Copyright (C) 2019 Dean De Leo, email: dleo[at]cwi.nl
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include "latency.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <limits>
#include <cmath>
#include "common/database.hpp"
#include "common/quantity.hpp"
#include "configuration.hpp"

#include <sqlite3.h>

using namespace common;
using namespace std;

namespace gfe::experiment::details {

static uint64_t get_percentile(uint64_t* __restrict A, uint64_t A_sz, uint64_t index){
    uint64_t pos = (index * A_sz) / 100; // i : 100 = pos : num_operations
    return A[pos > 0 ? (pos -1) : 0];
}

LatencyStatistics LatencyStatistics::compute_statistics(
        uint64_t* arr_latencies_nanosecs, 
        uint64_t arr_latencies_sz,
        uint64_t chunk_size){
    LatencyStatistics instance;
    if(arr_latencies_sz == 0) return instance;
    
    std::cout << "Latency Statistics::compute_statistics : General\n";

    // compute mean/std.dev/min/max
    uint64_t sum = 0;
    uint64_t sum2 = 0;
    uint64_t vmin = numeric_limits<uint64_t>::max();
    uint64_t vmax = 0;
    for(uint64_t i = 0; i < arr_latencies_sz; i++){
        uint64_t value = arr_latencies_nanosecs[i];

        sum += value;
        sum2 += value * value;
        vmin = min(value, vmin);
        vmax = max(value, vmax);
    }

    instance.m_num_operations = arr_latencies_sz;
    instance.m_mean = sum / arr_latencies_sz;
    instance.m_stddev = (sum2 / arr_latencies_sz) - (instance.m_mean * instance.m_mean);
    instance.m_min = vmin;
    instance.m_max = vmax;

    std::cout << "Latency Statistics::compute_statistics : Chunking\n";
    // Compute per-chunk means + min/max + percentiles
    if (chunk_size > 0) {
        uint64_t num_chunks = (arr_latencies_sz + chunk_size - 1) / chunk_size;
        instance.m_chunk_means.reserve(num_chunks);
        instance.m_chunk_mins.reserve(num_chunks);
        instance.m_chunk_maxs.reserve(num_chunks);
        instance.m_chunk_p90s.reserve(num_chunks);
        instance.m_chunk_p95s.reserve(num_chunks);
        instance.m_chunk_p99s.reserve(num_chunks);

        for (uint64_t i = 0; i < arr_latencies_sz; i += chunk_size) {
            uint64_t chunk_end = min(i + chunk_size, arr_latencies_sz);
            uint64_t chunk_len = chunk_end - i;

            uint64_t chunk_sum = 0;
            uint64_t chunk_min = numeric_limits<uint64_t>::max();
            uint64_t chunk_max = 0;

            // Copy chunk to temporary array for percentile calculation
            std::vector<uint64_t> chunk_data;
            chunk_data.reserve(chunk_len);

            for (uint64_t j = i; j < chunk_end; j++) {
                uint64_t v = arr_latencies_nanosecs[j];
                chunk_sum += v;
                chunk_min = min(chunk_min, v);
                chunk_max = max(chunk_max, v);
                chunk_data.push_back(v);
            }

            sort(chunk_data.begin(), chunk_data.end());
            uint64_t chunk_mean = chunk_sum / chunk_len;
            uint64_t p90 = get_percentile(chunk_data.data(), chunk_len, 90);
            uint64_t p95 = get_percentile(chunk_data.data(), chunk_len, 95);
            uint64_t p99 = get_percentile(chunk_data.data(), chunk_len, 99);

            instance.m_chunk_means.push_back(chunk_mean);
            instance.m_chunk_mins.push_back(chunk_min);
            instance.m_chunk_maxs.push_back(chunk_max);
            instance.m_chunk_p90s.push_back(p90);
            instance.m_chunk_p95s.push_back(p95);
            instance.m_chunk_p99s.push_back(p99);
        }
    }

    // compute the percentiles for entire dataset
    sort(arr_latencies_nanosecs, arr_latencies_nanosecs + arr_latencies_sz);
    instance.m_percentile90 = get_percentile(arr_latencies_nanosecs, arr_latencies_sz, 90);
    instance.m_percentile95 = get_percentile(arr_latencies_nanosecs, arr_latencies_sz, 95);
    instance.m_percentile97 = get_percentile(arr_latencies_nanosecs, arr_latencies_sz, 97);
    instance.m_percentile99 = get_percentile(arr_latencies_nanosecs, arr_latencies_sz, 99);

    if(arr_latencies_sz % 2 == 0){
        instance.m_median = (arr_latencies_nanosecs[arr_latencies_sz /2] + arr_latencies_nanosecs[(arr_latencies_sz /2)-1]) /2;
    } else {
        instance.m_median = arr_latencies_nanosecs[arr_latencies_sz /2];
    }

    return instance;
}

chrono::nanoseconds LatencyStatistics::mean() const {
    return chrono::nanoseconds(m_mean);
}

chrono::nanoseconds LatencyStatistics::percentile90() const {
    return chrono::nanoseconds(m_percentile90);
}

chrono::nanoseconds LatencyStatistics::percentile99() const {
    return chrono::nanoseconds(m_percentile99);
}

void LatencyStatistics::save(const std::string& name){
    std::cout << "LatencyStatistics:save General: " << name << std::endl;

    assert(configuration().db() != nullptr);

    auto store = configuration().db()->add("latencies");
    store.add("type", name);
    store.add("num_operations", m_num_operations);
    store.add("mean", m_mean);
    store.add("median", m_median);
    store.add("stddev", m_stddev);
    store.add("min", m_min);
    store.add("max", m_max);
    store.add("p90", m_percentile90);
    store.add("p95", m_percentile95);
    store.add("p97", m_percentile97);
    store.add("p99", m_percentile99);

    // Save per-chunk data
    std::cout << "LatencyStatistics:save Chunking (extended): " << name << std::endl;

    auto db = configuration().db();
    sqlite3* conn = static_cast<sqlite3*>(db->get_connection_handle());

    if (!conn) {
        std::cerr << "Error: Database handle is null!" << std::endl;
        return;
    }

    // Create Table (extended)
    sqlite3_exec(conn,
        "CREATE TABLE IF NOT EXISTS latencies_chunks ("
        "type TEXT, "
        "chunk_index INTEGER, "
        "chunk_mean REAL, "
        "chunk_min REAL, "
        "chunk_max REAL, "
        "chunk_p90 REAL, "
        "chunk_p95 REAL, "
        "chunk_p99 REAL"
        ");",
        nullptr, nullptr, nullptr);

    sqlite3_stmt* stmt = nullptr;
    const char* sql =
        "INSERT INTO latencies_chunks "
        "(type, chunk_index, chunk_mean, chunk_min, chunk_max, chunk_p90, chunk_p95, chunk_p99) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?);";

    if (sqlite3_prepare_v2(conn, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        std::cerr << "Failed to prepare statement: " << sqlite3_errmsg(conn) << std::endl;
        return;
    }

    int rc = sqlite3_exec(conn, "BEGIN IMMEDIATE TRANSACTION;", nullptr, nullptr, nullptr);
    if (rc != SQLITE_OK) {
        std::cerr << "Warning: couldn't begin transaction (" << sqlite3_errmsg(conn)
                << "), continuing anyway.\n";
    }

    int chunk_index = 0;
    for (size_t k = 0; k < m_chunk_means.size(); ++k) {
        sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 2, chunk_index++);
        sqlite3_bind_double(stmt, 3, static_cast<double>(m_chunk_means[k]));
        sqlite3_bind_double(stmt, 4, static_cast<double>(m_chunk_mins[k]));
        sqlite3_bind_double(stmt, 5, static_cast<double>(m_chunk_maxs[k]));
        sqlite3_bind_double(stmt, 6, static_cast<double>(m_chunk_p90s[k]));
        sqlite3_bind_double(stmt, 7, static_cast<double>(m_chunk_p95s[k]));
        sqlite3_bind_double(stmt, 8, static_cast<double>(m_chunk_p99s[k]));

        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            std::cerr << "Insert failed: " << sqlite3_errmsg(conn) << std::endl;
            break;
        }

        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
    }

    sqlite3_exec(conn, "COMMIT;", nullptr, nullptr, nullptr);
    sqlite3_finalize(stmt);

    std::cout << "LatencyStatistics: finished saving " << m_chunk_means.size()
              << " chunks for " << name << std::endl;
}

static DurationQuantity _D(uint64_t value){
    return DurationQuantity{chrono::nanoseconds(value)};
}

std::ostream& operator<<(std::ostream& out, const LatencyStatistics& stats){
    out << "N: " << stats.m_num_operations << ", mean: " << _D(stats.m_mean) << ", median: " << _D(stats.m_median) << ", "
        << "std. dev.: " << _D(stats.m_stddev) << ", min: " << _D(stats.m_min) << ", max: " << _D(stats.m_max) << ", "
        << "perc 90: " << _D(stats.m_percentile90) << ", perc 95: " << _D(stats.m_percentile95) << ", "
        << "perc 97: " << _D(stats.m_percentile97) << ", perc 99: " << _D(stats.m_percentile99);

    return out;
}

} // namespace gfe::experiment::details

