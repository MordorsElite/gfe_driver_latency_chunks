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

#pragma once

#include <cinttypes>
#include <chrono>
#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

namespace gfe::experiment::details {

class LatencyStatistics {
    friend std::ostream& operator<<(std::ostream& out, const LatencyStatistics& stats);
    uint64_t m_num_operations {0};
    uint64_t m_mean {0};
    uint64_t m_stddev {0};
    uint64_t m_min {0};
    uint64_t m_max {0};
    uint64_t m_median {0};
    uint64_t m_percentile90 {0};
    uint64_t m_percentile95 {0};
    uint64_t m_percentile97 {0};
    uint64_t m_percentile99 {0};

    // Per-chunk statistics
    std::vector<uint64_t> m_chunk_means;
    std::vector<uint64_t> m_chunk_medians;
    std::vector<uint64_t> m_chunk_mins;
    std::vector<uint64_t> m_chunk_maxs;
    std::vector<uint64_t> m_chunk_p90s;
    std::vector<uint64_t> m_chunk_p95s;
    std::vector<uint64_t> m_chunk_p99s;


public:
    /**
     * Compute the statistics for the given latencies
     * @param arr_latencies_nanosecs an array containing the latency of each operation, in nanosecs
     * @param arr_latencies_sz number of elements in the array arr_latencies_nanosecs
     * @param chunk_size the number of operations per chunk for average computation (default = 100)
     */
    static LatencyStatistics compute_statistics(uint64_t* arr_latencies_nanosecs, uint64_t arr_latencies_sz, uint64_t chunk_size = 100);

    /**
     * Save the statistics into the table "latency" with the given value for the attribute `type'
     */
    void save(const std::string& type);

    /**
     * Retrieve the average latency of each update
     */
    std::chrono::nanoseconds mean() const;

    /**
     * Retrieve the 90th percentile of updates
     */
    std::chrono::nanoseconds percentile90() const;

    /**
     * Retrieve the 99th percentile of updates
     */
    std::chrono::nanoseconds percentile99() const;

    /**
     * Retrieve per-chunk statistics
     */
    const std::vector<uint64_t>& chunk_means() const { return m_chunk_means; }
    const std::vector<uint64_t>& chunk_medians() const { return m_chunk_medians; }
    const std::vector<uint64_t>& chunk_mins() const { return m_chunk_mins; }
    const std::vector<uint64_t>& chunk_maxs() const { return m_chunk_maxs; }
    const std::vector<uint64_t>& chunk_p90s() const { return m_chunk_p90s; }
    const std::vector<uint64_t>& chunk_p95s() const { return m_chunk_p95s; }
    const std::vector<uint64_t>& chunk_p99s() const { return m_chunk_p99s; } 

};

std::ostream& operator<<(std::ostream& out, const LatencyStatistics& stats);

} // namespace
