""" Reads a directory with xml files and generates the citation files needed for the citation network """
from lxml import etree
from pathlib import Path
import typer, json
from typing import Sequence, Any
from tqdm import tqdm

def sort_by_seq(collection:Sequence[Any], attribute:str, seq:list[str]):
    """ Enforces a priority on multiple nodes of the same type by the value of a field"""
    order = {key:val for val, key in enumerate(seq)}
    return list(sorted(collection, key=lambda n: order.get(n.attrib[attribute], float("inf"))))

def parse_xml(file:Path):
    """ Reads the NXML file and extracts the data necessary for building the citation network data later on """

    ret = {}

    tree = etree.parse(file)

    # Extract the journal issn and name
    journal = tree.xpath('//journal-meta')
    if journal:
        journal = journal[0]
        jnames = sort_by_seq(journal.xpath('journal-id'), "journal-id-type", ["nlm-ta", "iso-abbrev"])
        # If there is no journal-id node, we will try to get the journal title instead
        if not jnames:
            jnames = journal.xpath('journal-title-group/journal-title')
        issns = sort_by_seq(journal.xpath('issn'), "pub-type", ["epub", "ppub"])
    
    ret['journal_name'] = jnames[0].text
    if issns:
        ret['journal_issn'] = issns[0].text
    else:
        ret['journal_issn'] = None

    # Extract the article's IDs, and publication date
    article = tree.xpath('//article-meta')[0]

    ids = sort_by_seq(article.xpath("article-id[@pub-id-type]"), "pub-id-type", ["pmc", "pmid", "doi", "pii"])
    ret["article_id"] = ids[0].text

    for id_ in ids:
        if "pub-id-type" in id_.attrib:
            ret["article_" + id_.attrib["pub-id-type"]] = id_.text

    pub_dates = sort_by_seq(article.xpath('pub-date[@pub-type]'), "pub-type", ["pmc-release", "accepted", "received"])

    if pub_dates:
        pub_date = pub_dates[0]
        date = []
        day = pub_date.find("day")
        month = pub_date.find("month")
        year = pub_date.find("year")
        for e in [month, day, year]:
            if e is not None:
                date.append(e.text)
        ret["pub_date"] = '/'.join(date)
    else:
        ret["pub_date"] = None


    # Now get the references. Each reference must have a pub-id node with a type, which we can use to resolve the article later on
    ref_list = tree.xpath("//ref-list")
    if ref_list:
        ref_list = ref_list[0]
        references = []
        for ref in ref_list.xpath("ref/element-citation"):
            ref_id = ref.find("pub-id")
            if ref_id is not None:
                references.append({"id_type": ref_id.attrib['pub-id-type'], "id": ref_id.text})

        ret["references"] = references

    return json.dumps(ret)

app = typer.Typer()

@app.command()
def main(input_dir: Path, output_file: Path = Path("out.jsonl")):
    with output_file.open("w") as f:
        for file in tqdm(filter(lambda f: f.name.endswith("xml"), input_dir.iterdir()), desc=f"Parsing XMLs in {input_dir.name} for metadata"):
            try:
                line = parse_xml(file)
                f.write(line + "\n")
            except Exception as ex:
                print(f"Problem parsing {file.name}: {ex}")


if __name__ == "__main__":
    app()
