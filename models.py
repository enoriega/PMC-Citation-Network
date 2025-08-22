from sqlmodel import Field, Index, SQLModel, Relationship
from datetime import date

class Journal(SQLModel, table=True):
	id: str | None = Field(default=None, primary_key=True)
	name:str = Field(index=True)
	issn:str | None = Field(default=None, index=True)

	articles: list['Article'] = Relationship(back_populates="journal")

class Reference(SQLModel, table=True):
	cites_id: int = Field(foreign_key="article.article_identifier", primary_key=True) # Article being cited
	citying_id: int = Field(foreign_key="article.article_identifier", primary_key=True) # Article citying 


class Article(SQLModel, table=True):
	id: int | None = Field(default=None, primary_key=True)
	article_identifier: str = Field(unique=True, index=True)
	pmcid: str | None = Field(default=None)
	pmid: str | None = Field(default=None)
	doi: str | None = Field(default=None)
	pii: str | None = Field(default=None)

	journal_id: int | None = Field(default=None, foreign_key="journal.id", index=True)
	pub_date: date | None = Field(default=None, index=True)
	publisher_id: str | None = Field(default=None)

	journal: Journal = Relationship(back_populates="articles")

	# Articles THIS article references (outgoing edges)
	references: list["Article"] = Relationship(
		back_populates="citedby",
		link_model=Reference,
		sa_relationship_kwargs={
			# Article.id -> Reference.from_id (this article is the citer)
			"primaryjoin": "Article.id == Reference.citying_id",
			# ... and we land on the cited article via Reference.to_id
			"secondaryjoin": "Article.id == Reference.cites_id",
		},
	)

	# Articles that CITE THIS article (incoming edges)
	citedby: list["Article"] = Relationship(
		back_populates="references",
		link_model=Reference,
		sa_relationship_kwargs={
			# Now this article is the cited node
			"primaryjoin": "Article.id == Reference.cites_id",
			"secondaryjoin": "Article.id == Reference.citying_id",
		},
	)



# We use this to map between multiple identifiers
class Identifier(SQLModel, table=True):
	id: int | None = Field(default=None, primary_key=True)
	key_type: str 
	key: str
	value: str

	__table_args__ = (
        Index("ix_identifier_key_type_key", "key_type", "key"),
    )


