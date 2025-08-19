import json
import typer
from tqdm import tqdm
from models import *
from sqlmodel import SQLModel, create_engine
from sqlalchemy import Engine
from sqlalchemy.orm import Session
import os
from datetime import datetime

app = typer.Typer()

def _bulk_insert(engine, *objects):
	""" Bulk inserts the collections into the database """
	with Session(engine) as session:
		for os in objects:
			session.bulk_save_objects(os)
			
		session.commit()

@app.command()
def create_schema(
	connection_string:str = typer.Argument(help="Database connection string, e.g. sqlite:///mydb.db"),
	echo:bool = typer.Option(False, help="Print SQL commands")
	) -> None:
	""" 
	Creates a database database with the schema
	"""
	
	engine = create_engine(connection_string, echo=echo)
	SQLModel.metadata.create_all(engine)


@app.command()
def populate_database(
    connection_string: str = typer.Argument(help="Database connection string, e.g. sqlite:///mydb.db"),
    data_file: str = typer.Argument(help="Path to the input data file"),
	batch_size:int= typer.Argument(500_000, help="Commit changes after processing `batch_size` records"), 
    echo: bool = typer.Option(False, help="Print SQL commands"),
    overwrite_file:bool= typer.Option(False, help="Overwrites existing SQLite file if true, otherwise, program fails")
) -> None:
	""" 
	Creates the database and populates it with contents from data_file. 
	This command uses bulk operations and its meant to create an initial database.
	For incremental additions use the `add_data` command
	"""

	# Erase the database file if exists
	if connection_string.startswith("sqlite:///"):
		db_path = connection_string[10:] # Remove the prefix
		if os.path.exists(db_path):
			if overwrite_file:
				os.remove(db_path)
			else:
				raise RuntimeError(f"DB file: {db_path} - already exists")

	# First create the schema
	create_schema(connection_string, echo)
	engine = create_engine(connection_string, echo=echo)

	# Then, read the data and bulk add it
	with open(data_file) as f:
		# First create the journals and identifier objects
		journals, identifiers = [], []
		articles_ids = {}
		journals_ids = {}
		seen = set()
		id_types = ["pmc", "pmid", "doi", "pii"]
		identifier_ix = 0

		for ix, line in tqdm(enumerate(f), desc="Resolving journals and article identifies", unit="files"):
			data = json.loads(line)
			jname = data["journal_name"]
			if data['journal_issn'] is not None and len(data['journal_issn'].strip()) > 0:
				issn = data['journal_issn']
			else:
				issn = None

			if issn:
				key = issn
			else:
				key = jname
				
			if key not in seen:
				journals.append(
					Journal(
						id=ix,
						name=jname,
						issn=issn
					)
				)
				seen.add(key)
				
				journals_ids[key] = ix

			for id_type in id_types:
				if f"article_{id_type}" in data:
					identifiers.append(
						Identifier(
							id = identifier_ix,
							key_type=id_type,
							key=data[f"article_{id_type}"],
							value=data["article_id"]
						)
					)
					identifier_ix += 1
					articles_ids[data[f"article_{id_type}"]] = ix

			if ix + 1 == batch_size:
				_bulk_insert(engine, journals, identifiers)
				journals, identifiers = [], []

		# Insert any leftovers
		if journals or identifiers:
			_bulk_insert(engine, journals, identifiers)

	# Go back to the begining of the file
	with open(data_file) as f:
		# Now do the same with the articles and references
		articles, references = [], []
		for ix, line in tqdm(enumerate(f), desc="Resolving articles and citations", unit="files"):
			data = json.loads(line)
			pub_date_str = data.get("pub_date")
			pub_date = None
			if pub_date_str:
				try:
					pub_date = datetime.strptime(pub_date_str, "%m/%d/%Y").date()
				except ValueError:
					pub_date = None

			jname = data["journal_name"]
			if data['journal_issn'] is not None and len(data['journal_issn'].strip()) > 0:
				issn = data['journal_issn']
			else:
				issn = None

			if issn:
				key = issn
			else:
				key = jname

			journal_id = journals_ids[key]

			articles.append(
				Article(
					id=ix,
					article_identifier=data["article_id"],
					pmcid=data.get("article_pmc"),
					pmid=data.get("article_pmid"),
					doi=data.get("article_doi"),
					pii=data.get("article_pii"),
					journal_id=journal_id,
					publisher_id=data.get("article_publisher-id"),
					pub_date=pub_date
				)
			)

			for ref in data.get("references", []):
				r = ref['id']
				if r in articles_ids:
					source = ix
					dest = articles_ids[r]
					if (source, dest) not in seen:
						references.append(
							Reference(
								citying_id=source,
								cites_id=dest
							)
						)
						seen.add((source, dest))

			if ix + 1 == batch_size:
				_bulk_insert(engine, articles, references)
				articles, references = [], []

		# Insert any leftovers
		if articles or references:
			_bulk_insert(engine, articles, references)
			



if __name__ == "__main__":
	app()
