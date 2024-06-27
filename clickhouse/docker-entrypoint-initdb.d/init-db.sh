#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
CREATE TABLE IF NOT EXISTS velib (
        id UInt64,
        stationCode UInt64,
        station_id UInt64,
        num_bikes_available UInt64,
        mechanical UInt64,
        ebike UInt64,
        num_docks_available UInt64,
        is_installed UInt64,
        is_returning UInt64,
        is_renting UInt64,
        last_reported UInt64,
        weather UInt64,
        temp Int64,
        probarain Int64,
        probafog Int64,
        probawind70 Int64,
        probawind100 Int64,
        lon Float64,
        lat Float64,
        name String
    ) ENGINE = MergeTree()
    ORDER BY id;
EOSQL