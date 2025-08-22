import os
import json
import typer
from tqdm import tqdm
from sqlmodel import SQLModel, create_engine, Session, select, func, desc
from datetime import datetime
from pathlib import Path
from models import *

app = typer.Typer()

def _bulk_insert(engine, *objects):
	""" Bulk inserts the collections into the database """
	with Session(engine) as session:
		for os in objects:
			session.bulk_save_objects(os)
			
		session.commit()

def _get_journal_data(data):
	""" Convenience function to get the journal's data. Key is used to test for prior existence """
	
	jname = data["journal_name"]
	if data['journal_issn'] is not None and len(data['journal_issn'].strip()) > 0:
		issn = data['journal_issn']
	else:
		issn = None

	if issn:
		key = issn
	else:
		key = jname
	return jname,issn,key


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
		# journals_ids = {}
		seen = set()
		id_types = ["pmc", "pmid", "doi", "pii"]
		identifier_ix = 0

		for ix, line in tqdm(enumerate(f), desc="Resolving journals and article identifies", unit="files"):
			data = json.loads(line)
			jname, issn, key = _get_journal_data(data)
				
			if key not in seen:
				journals.append(
					Journal(
						id=key,
						name=jname,
						issn=issn
					)
				)
				seen.add(key)
				
				# journals_ids[key] = ix

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

			jname, issn, journal_id = _get_journal_data(data)

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

@app.command()
def add_data(
	connection_string: str = typer.Argument(help="Database connection string, e.g. sqlite:///mydb.db"),
	data_file: str = typer.Argument(help="Path to the input data file"),
	echo: bool = typer.Option(False, help="Print SQL commands"),
) -> None:
	""" 
	Updates a databse file with contents from data_file. 
	For bulk additions use the `populate_database` command
	"""


	# First create the schema
	create_schema(connection_string, echo)
	engine = create_engine(connection_string, echo=echo)

	# Then, read the data and bulk add it
	with open(data_file) as f:
		# First create the journals and identifier objects
		identifiers = []
		id_types = ["pmc", "pmid", "doi", "pii"]


	with open(data_file) as f:
		# Now do the same with the articles and references
		articles_w_references = []

		for line in tqdm(f, desc="Adding articles, journals and citations", unit="files"):
			data = json.loads(line)				

			# Deal with the identifiers
			for id_type in id_types:
				if f"article_{id_type}" in data:
					identifiers.append(
						Identifier(
							key_type=id_type,
							key=data[f"article_{id_type}"],
							value=data["article_id"]
						)
					)

			# Get journal from the current record
			jname, issn, key = _get_journal_data(data)
				
			journal = Journal(
				id=key,
				name=jname,
				issn=issn
			)

			pub_date_str = data.get("pub_date")
			pub_date = None
			if pub_date_str:
				try:
					pub_date = datetime.strptime(pub_date_str, "%m/%d/%Y").date()
				except ValueError:
					pub_date = None


			refs = data.get("references", [])
			
			articles_w_references.append(
				(
					Article(
						article_identifier=data["article_id"],
						pmcid=data.get("article_pmc"),
						pmid=data.get("article_pmid"),
						doi=data.get("article_doi"),
						pii=data.get("article_pii"),
						journal=journal,
						publisher_id=data.get("article_publisher-id"),
						pub_date=pub_date
					),
					refs		
				)
			)


	
	# Save the articles
	with Session(engine) as session:
		tracked = {}
		with session.no_autoflush:
			session.add_all(identifiers)
			for article, refs in tqdm(articles_w_references, desc="Cross-referencing files"):
				# Make sure to track the journal instance or create it if its new
				journal = article.journal
				if journal.id in tracked:
					article.journal = tracked[journal.id]
				elif j := session.get(Journal, journal.id):
					article.journal = j
				else:
					session.add(journal)
					tracked[journal.id] = journal
				# Create the new article instance in the DB
				session.add(article)
				# Create the references
				for ref in refs:
					stmt = select(Identifier.value).where(Identifier.key_type == ref['id_type'], Identifier.key == ref['id'])
					if article_id := session.exec(stmt).first():
						dest = session.exec(select(Article).where(Article.article_identifier == article_id)).first()
						article.references.append(dest)
		session.commit()



@app.command()	
def print_article_citations(
		connection_string:str = typer.Argument(help="Database connection string, e.g. sqlite:///mydb.db"),
		pmcids_file:Path|None = typer.Argument(None, help="File containing list of PMCIDs to query, one per file"),
		echo: bool = typer.Option(False, help="Print SQL commands"),
	):
	"""
	Gets the citations to and from papers. Will retrieve only those in pmcids_file, if specified, otherwise, it returns the citations for all the articles.
	"""
	engine = create_engine(connection_string, echo=echo)

	stmt = (
		select(
			Article.article_identifier,
			func.count(Reference.cites_id).label("num_citations")
		)
		.join(Reference, Article.id == Reference.cites_id)
	)

	# Read the PMCIDs from the file
	if pmcids_file:
		with pmcids_file.open() as f:
			ids = {l.strip() for l in f}
		# Select only the articles in the input set
		stmt = stmt.where(Article.article_identifier.in_(ids))
	
	# Aggregate the 
	stmt = stmt.group_by(Article.article_identifier).order_by(desc("num_citations"))
	
	with Session(engine) as session:
		result = session.exec(stmt)
		sample_ids = result.all()
		output = [
			{"article_identifier": article_id, "num_citations": num_citations}
			for article_id, num_citations in sample_ids
		]
	print(json.dumps(output, indent=2))


if __name__ == "__main__":
	app()
