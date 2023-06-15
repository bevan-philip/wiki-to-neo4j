# wiki-to-neo4j

wiki-to-neo4j is a project that convert a MediaWiki wiki dump, into a Neo4j database.

It achieves this via extracting Infobox template information, and using natural language processing to get further information from the text on the page.

---

## filter.py
filter.py is a filtering program that reduces the size of MediaWiki dumps, by removing miscallaneous information, and pages not within the selected namespace.

```filter.py -h ``` for usage instructions.

## wiki4j.py
wiki4j.py takes a MediaWiki database as an input, and converts it into a Neo4j graph database.

```wiki4j.py -h``` for usage instructions.

