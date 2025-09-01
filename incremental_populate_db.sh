#!/bin/sh
cd /app

INPUT_DIR=/input
OUTPUT_DIR=/output

# Concatenate all files into single jsonl
echo "Incrementally adding data into $CONN_STRING"

find "${INPUT_DIR}" -maxdepth 1 -type f -name '*.jsonl' | while read -r file; do
	file_name=$(basename "$file")
	# Compute the number of lines, if the directory didn't have 
	num_lines=$(wc -l < "$file")
	if [ ! -f "${OUTPUT_DIR}/processed_files.txt" ] || ! grep -Fq "${file_name}" "${OUTPUT_DIR}/processed_files.txt"; then
		echo "Importing from ${file_name}"
		uv run citation_network.py add-data "${CONN_STRING}" "${file}"
	else
		# Locate the line in processed_files.txt and split by whitespace
		processed_line=$(grep -E "^${file_name}" "${OUTPUT_DIR}/processed_files.txt")
		processed_file_name=$(echo "$processed_line" | awk '{print $1}')
		processed_num_lines=$(echo "$processed_line" | awk '{print $2}')
		if [ "$num_lines" -ne "$processed_num_lines" ]; then
			diff_lines=$((num_lines - processed_num_lines))
			# Create a temporary file with the last diff_lines lines
			tmp_file=$(mktemp)
			tail -n "$diff_lines" "$file" > "$tmp_file"
			uv run citation_network.py add-data "${CONN_STRING}" "$tmp_file"
			rm "$tmp_file"
		fi
	fi
	echo "$file_name $num_lines" >> "${OUTPUT_DIR}/new_processed_files.txt"
done

# Overwrite the processed_files.txt file with the new one
mv "${OUTPUT_DIR}/new_processed_files.txt" "${OUTPUT_DIR}/processed_files.txt"


