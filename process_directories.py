""" Run the process directory command from the parse_xmls app in the docker image. """
from pathlib import Path
from tqdm import tqdm
from parse_xmls import main

if __name__ == "__main__":
	# Load the list of files already processed
	# Format per line is: XML file name <space> JSONL file name
	processed = {}
	processed_path = Path("/output/processed_files.txt")
	if processed_path.exists():
		with processed_path.open() as f:
			for l in f:
				if l:
					k, v = l.strip().split()
					processed[k] = v

	# Iterate over each directory inside /input
	with processed_path.open("a") as f: # Let's keep a pointer to the processed path to write down the new files in it
		to_process = set()
		# Iterate over the input directry's processed_files.txt to see the available files without doing all FS io
		with open("/input/processed_files.txt") as g:
			# Format: xml file name <space> directory index (int)
			for l in g:
				if l:
					xml_name, dir_ix = l.strip().split()
					if xml_name not in processed:
						# Hold this directory to process
						xml_dir = Path("/input") / f"xml_{dir_ix}"
						to_process.add(xml_dir)
						f.write(f"{xml_name} {xml_dir.name}\n")

		# Process the directories that have at least one file that hasn't been processed before
		for xml_dir in tqdm(to_process, desc="Processing directories"):
			output_file = Path("/output") / (xml_dir.name + ".jsonl")
			main(xml_dir, output_file)