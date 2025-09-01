#!/bin/sh
cd /app

INPUT_DIR=/input
OUTPUT_DIR=/output

# Concatenate all files into single jsonl
echo "Concatenating files"
find "${INPUT_DIR}" -maxdepth 1 -type f -name '*.jsonl' | while read -r file; do
	cat "$file" >> "${INPUT_DIR}/temp.jsonl"
	num_lines=$(wc -l < "$file")
	echo "$(basename "$file $num_lines")" >> "${OUTPUT_DIR}/processed_files.txt"
done

# Run bulk creation
echo "Importing data into $CONN_STRING"
uv run citation_network.py populate-database ${CONN_STRING} "${INPUT_DIR}/temp.jsonl" --overwrite-file

# Remove temp file
rm "${INPUT_DIR}/temp.jsonl"
