#!/bin/sh
cd /app

INPUT_DIR=/input

# Concatenate all files into single jsonl
echo "Incrementally adding data into $CONN_STRING"
find "${INPUT_DIR}" -maxdepth 1 -type f -name '*.jsonl' | while read -r file; do
	file_name=$(basename "$file") 
	if [ ! -f "${INPUT_DIR}/processed_files.txt" ] || ! grep -Fxq "${file_name}" "${INPUT_DIR}/processed_files.txt"; then
		echo "Importing from ${file_name}"
		uv run citation_network.py populate-database "${CONN_STRING}" "${file}" --overwrite-file
		echo "$file_name" >> "${INPUT_DIR}/processed_files.txt"
	fi
done


