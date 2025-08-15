# PMC Citation Network

This project processes a directory of PMC (PubMed Central) XML files to extract citation and article metadata, generating files suitable for building a citation network.

## Features

- Parses NXML (JATS) files using `lxml`
- Extracts journal metadata, article identifiers (PMID, PMCID, DOI, PII), and publication dates
- Handles references and citation lists
- Outputs data in JSON Lines format for easy downstream processing
- Command-line interface powered by [Typer](https://typer.tiangolo.com/)
- Progress reporting with [tqdm](https://tqdm.github.io/)

## Requirements

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) (for dependency management)
- [lxml](https://lxml.de/)
- [tqdm](https://tqdm.github.io/)
- [typer](https://typer.tiangolo.com/)

Install dependencies (recommended: use a virtual environment):

```sh
uv sync
```

## Usage

Run the main script to process a directory of XML files:

```sh
uv run main.py --input data/xml --output output.jsonl
```

- `--input`: Path to the directory containing PMC XML files
- `--output`: Path to the output JSON Lines file

## Project Structure

```
PMC-Citation-Network/
├── data/
│   └── xml/                # Input XML files
├── output.jsonl            # Example output file
├── main.py                 # Main processing script and CLI
├── models/                 # (Optional) Data models
└── README.md
```

## Example Output

Each line in the output file is a JSON object with extracted metadata, e.g.:

```json
{
  "journal_name": "ACS Pharmacology & Translational Science",
  "journal_issn": "2575-9108",
  "pmid": "36937555",
  "pmcid": "PMC10011043",
  "doi": "10.1016/j.displa.2023.102403",
  "publication_date": "2023-02-22",
  "references": [
    {"pmid": "...", "doi": "..."},
    ...
  ]
}
```

## Development

- Extend `parse_xml` in `main.py` to extract additional fields as needed.
- Add unit tests for XML parsing and data extraction logic.

## License

MIT License

---

*Created for citation network analysis and