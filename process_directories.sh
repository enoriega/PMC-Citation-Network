# Iterate over each directory inside /input
cd /app

for D in $(find /input -mindepth 1 -maxdepth 1 -type d -name "xml*"); do
    OUTPUT_FILE="/output/$(basename $D).jsonl"
    if [ ! -f "$OUTPUT_FILE" ]; then
        # Run the command with the current directory and create an output file with the same name as the directory
        echo "Processing directory: $(basename $D)"
        OUTPUT_FILE="/output/$(basename $D).jsonl"
        uv run main.py "${D}" --output-file ${OUTPUT_FILE}
    fi
done
# Run the command with the current directory and create an output file with the same name as the directory