#!/bin/bash
#
# Parquet Export Benchmark Script for QuestDB
# Usage: ./benchmark_parquet_export.sh [table_name] [questdb_url]
#

set -e

# Configuration
TABLE_NAME="${1:-Alc.Binlogs.PnlRecord}"
QUESTDB_URL="${2:-http://localhost:9000}"
OUTPUT_FILE="/dev/null"
CODEC="lz4_raw"
ROW_COUNT=140000000

echo "=============================================="
echo "QuestDB Parquet Export Benchmark"
echo "=============================================="
echo "Table: $TABLE_NAME"
echo "Codec: $CODEC"
echo ""

# Function to wait for WAL to be applied (writerTxn = sequencerTxn)
wait_for_wal_apply() {
    local table=$1
    echo "Waiting for WAL to be applied..."
    local attempts=0
    while true; do
        RESULT=$(curl -s -G "$QUESTDB_URL/exec" \
            --data-urlencode "query=SELECT sequencerTxn, writerTxn FROM wal_tables() WHERE name = '$table'" \
            | python3 -c "
import sys, json
d = json.load(sys.stdin)
if 'dataset' in d and len(d['dataset']) > 0:
    seq_txn, writer_txn = d['dataset'][0]
    print(f'{seq_txn} {writer_txn}')
else:
    print('0 0')
" 2>/dev/null || echo "0 0")

        SEQ_TXN=$(echo "$RESULT" | cut -d' ' -f1)
        WRITER_TXN=$(echo "$RESULT" | cut -d' ' -f2)

        if [ "$SEQ_TXN" = "0" ]; then
            attempts=$((attempts + 1))
            if [ "$attempts" -ge 3 ]; then
                echo "ERROR: sequencerTxn is 0 - insert may have failed or table is not WAL"
                exit 1
            fi
        elif [ "$SEQ_TXN" = "$WRITER_TXN" ]; then
            echo "WAL applied (sequencerTxn=$SEQ_TXN, writerTxn=$WRITER_TXN)"
            break
        fi

        echo "  Waiting... (sequencerTxn=$SEQ_TXN, writerTxn=$WRITER_TXN)"
        sleep 1
    done
}

# Check if table exists
TABLE_EXISTS=$(curl -s -G "$QUESTDB_URL/exec" \
    --data-urlencode "query=SELECT count() FROM '$TABLE_NAME'" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print('yes' if 'dataset' in d else 'no')" 2>/dev/null || echo "no")

if [ "$TABLE_EXISTS" = "no" ]; then
    echo "Table does not exist. Creating and populating..."
    echo ""

    # Create table
    echo "Creating table..."
    curl -s -G "$QUESTDB_URL/exec" --data-urlencode "query=CREATE TABLE '$TABLE_NAME' (
        publisher SYMBOL,
        exch SYMBOL,
        symbol SYMBOL,
        event_id LONG,
        ccy LONG,
        incr_pnl DOUBLE,
        real_pnl DOUBLE,
        est_pnl DOUBLE,
        incr_fee DOUBLE,
        fee_amt DOUBLE,
        rebate_amt DOUBLE,
        net_traded_qty DOUBLE,
        excess_qty DOUBLE,
        expected_qty DOUBLE,
        pnl_ccy_usd_rate DOUBLE,
        fee_ccy_usd_rate DOUBLE,
        last_seen_md_id LONG,
        timestamp TIMESTAMP
    ) timestamp(timestamp) PARTITION BY DAY" > /dev/null

    # Populate table
    echo "Inserting $ROW_COUNT rows (this may take a while)..."
    INSERT_RESPONSE=$(curl -s -G "$QUESTDB_URL/exec" --data-urlencode "query=INSERT INTO '$TABLE_NAME'
    SELECT
        rnd_symbol('PUB_A', 'PUB_B', 'PUB_C', 'PUB_D', 'PUB_E') as publisher,
        rnd_symbol('NYSE', 'NASDAQ', 'CME', 'EUREX', 'LSE') as exch,
        rnd_symbol('AAPL', 'GOOG', 'MSFT', 'AMZN', 'META', 'NVDA', 'TSLA', 'JPM', 'BAC', 'WFC') as symbol,
        x as event_id,
        rnd_long(1, 100, 0) as ccy,
        rnd_double() * 10000 - 5000 as incr_pnl,
        rnd_double() * 100000 - 50000 as real_pnl,
        rnd_double() * 100000 - 50000 as est_pnl,
        rnd_double() * 100 as incr_fee,
        rnd_double() * 1000 as fee_amt,
        rnd_double() * 500 as rebate_amt,
        rnd_double() * 10000 as net_traded_qty,
        rnd_double() * 100 as excess_qty,
        rnd_double() * 10000 as expected_qty,
        rnd_double() * 2 as pnl_ccy_usd_rate,
        rnd_double() * 2 as fee_ccy_usd_rate,
        rnd_long(1, 1000000000, 0) as last_seen_md_id,
        '2025-01-01'::timestamp + x * 10000L as timestamp
    FROM long_sequence($ROW_COUNT)")

    # Check for error in response
    INSERT_ERROR=$(echo "$INSERT_RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('error', ''))" 2>/dev/null || echo "")
    if [ -n "$INSERT_ERROR" ]; then
        echo "ERROR: Insert failed: $INSERT_ERROR"
        exit 1
    fi

    echo "Insert query sent."
    echo ""

    # Wait for WAL to be applied
    wait_for_wal_apply "$TABLE_NAME"
    echo ""
fi

# Get table row count
ACTUAL_ROWS=$(curl -s -G "$QUESTDB_URL/exec" \
    --data-urlencode "query=SELECT count() FROM '$TABLE_NAME'" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['dataset'][0][0])" 2>/dev/null || echo "unknown")
echo "Row count: $ACTUAL_ROWS"

# Get table disk size
TABLE_DISK_SIZE=$(curl -s -G "$QUESTDB_URL/exec" \
    --data-urlencode "query=SELECT sum(diskSize) FROM table_partitions('$TABLE_NAME')" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['dataset'][0][0])" 2>/dev/null || echo "0")
TABLE_DISK_SIZE_GB=$(python3 -c "print(round($TABLE_DISK_SIZE / 1073741824, 2))")
echo "Table disk size: ${TABLE_DISK_SIZE_GB} GB"

# Don't delete /dev/null!
if [ "$OUTPUT_FILE" != "/dev/null" ]; then
    rm -f "$OUTPUT_FILE"
fi

# Flush page cache to ensure cold read (requires root)
echo "Flushing page cache..."
sync
if ! sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null; then
    echo "WARNING: Could not flush page cache (requires root). Results may be affected by warm cache."
fi

# Record start time
START_TIME=$(python3 -c 'import time; print(time.time())')

# Run export
echo "Exporting to parquet..."
HTTP_STATUS=$(curl -s -G "$QUESTDB_URL/exp" \
    --data-urlencode "query=SELECT * FROM '$TABLE_NAME'" \
    --data "fmt=parquet" \
    --data "compression_codec=$CODEC" \
    -o "$OUTPUT_FILE" \
    -w "%{http_code}")

# Check for export error
if [ "$HTTP_STATUS" != "200" ]; then
    echo "ERROR: Export failed with HTTP $HTTP_STATUS"
    echo "Response:"
    cat "$OUTPUT_FILE"
    exit 1
fi

# Record end time
END_TIME=$(python3 -c 'import time; print(time.time())')

# Calculate metrics
ELAPSED=$(python3 -c "print(round($END_TIME - $START_TIME, 3))")
FILE_SIZE=$(stat -f%z "$OUTPUT_FILE" 2>/dev/null || stat -c%s "$OUTPUT_FILE" 2>/dev/null || echo "0")
FILE_SIZE_GB=$(python3 -c "print(round($FILE_SIZE / 1073741824, 2))")
OUTPUT_THROUGHPUT=$(python3 -c "print(round($FILE_SIZE / $ELAPSED / 1048576, 2))")
INPUT_THROUGHPUT=$(python3 -c "print(round($TABLE_DISK_SIZE / $ELAPSED / 1048576, 2))")

echo ""
echo "Results:"
echo "  HTTP Status:      $HTTP_STATUS"
echo "  Elapsed:          ${ELAPSED}s"
echo "  Table disk size:  ${TABLE_DISK_SIZE_GB} GB"
echo "  Parquet size:     ${FILE_SIZE_GB} GB"
echo "  Input throughput: ${INPUT_THROUGHPUT} MB/s (native table)"
echo "  Output throughput: ${OUTPUT_THROUGHPUT} MB/s (parquet)"
