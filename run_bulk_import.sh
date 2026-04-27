#!/bin/bash

# Check if the graph name was provided
if [ -z "$1" ]; then
    echo "Error: You must provide the graph name as an argument."
    echo "Usage: ./run_bulk_import.sh graph_name"
    exit 1
fi

GRAPH_NAME=$1
ARGS=()

echo "🔍 Analyzing CSV files in the csv_output directory..."

# 1. Scan NODES
for f in csv_output/nodes_*.csv; do
    if [ -f "$f" ]; then
        LABEL=$(basename "$f" .csv | sed 's/nodes_//')
        ARGS+=("-N" "$LABEL" "$f")
        echo "📍 Found node: $LABEL ($f)"
    fi
done

# 2. Scan RELATIONS
for f in csv_output/edges_*.csv; do
    if [ -f "$f" ]; then
        TYPE=$(basename "$f" .csv | sed 's/edges_//')
        ARGS+=("-R" "$TYPE" "$f")
        echo "🔗 Found relation: $TYPE ($f)"
    fi
done

if [ ${#ARGS[@]} -eq 0 ]; then
    echo "❌ No CSV files found in csv_output/."
    exit 1
fi

# Set the Python executable using the virtual environment if it exists
PYTHON_EXECUTABLE="python3"
if [ -x "./.venv/bin/python3" ]; then
    PYTHON_EXECUTABLE="./.venv/bin/python3"
fi

# PHASE 1: Clean Slate (Mandatory for maximum speed)
echo "🧹 1/3 - Cleaning existing graph ($GRAPH_NAME)..."
docker exec falkordb_server redis-cli -p 6379 DEL "$GRAPH_NAME"

# PHASE 2: Massive Binary Loading (Nodes + Relations in RAM)
echo "🚀 2/3 - Starting massive loading (buffer reduced to 16MB and skipping duplicates)..."
"$PYTHON_EXECUTABLE" -m falkordb_bulk_loader.bulk_insert "$GRAPH_NAME" \
    "${ARGS[@]}" \
    --id-type STRING \
    --escapechar "none" \
    -b 16 -t 16 -c 500

# Check if the loading was successful before applying metadata
if [ $? -ne 0 ]; then
    echo "❌ Error during massive loading. Aborting."
    exit 1
fi

# PHASE 3: Application of Indexes and Constraints
echo "🔒 3/3 - Applying metadata (Indexes and Constraints)..."
"$PYTHON_EXECUTABLE" apply_indexes_and_constraints.py "$GRAPH_NAME"

echo "✅ Import completed successfully!"