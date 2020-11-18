from neo4j import GraphDatabase
from xml.dom import minidom
import mwparserfromhell
import nltk
import sys

class Neo4JInterface:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_create_page(self, w_id, title, text):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_page, w_id, title, text)
            print(result)

    def create_relationship(self, link_from, link_to):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_relationship, link_from, link_to)
            print(result)

    @staticmethod
    def _create_page(tx, w_id, title, text):
        result = tx.run("MERGE (n:Item { id: $id, name: $name })"
                        "ON CREATE SET n.text = $text  "
                        "ON MATCH SET n.text = $text  "
                        "RETURN n.id, n.name ", id=w_id, name=title, text=text)
        return result.single()[0]

    @staticmethod
    def _create_relationship(tx, link_from, link_to):
        result = tx.run("MATCH (from:Item { name: $link_from })"
                        "MATCH (to:Item { name: $link_to })"
                        "MERGE (from)-[rel:LINKED]->(to)",
                        "RETURN from.id", link_from=link_from, link_to=link_to)
        return result.single()[0]

class Page:
    def __init__(self, w_id, title, text):
        self.w_id = w_id
        self.title = title
        self.text = text
        self.wikicode = mwparserfromhell.parse(text)
    
    def links(self):
        return self.wikicode.filter_wikilinks()

    def process_text(self):
        filtered = self.wikicode.strip_code()
        filtered = filtered.replace("\n", " ").split(" ")

        if filtered[0].startswith("left"):
            del filtered[0]

        return " ".join(map(str, filtered))

    def tokenize_text(self):
        sentences = nltk.sent_tokenize(self.process_text())
        sentences = [nltk.word_tokenize(sent) for sent in sentences]
        sentences = [nltk.pos_tag(sent) for sent in sentences]
        return sentences

if __name__ == "__main__":
    # neoInst = Neo4JInterface("bolt://localhost:7687", "neo4j", "e")

    xmldoc = minidom.parse('filtered_dump.xml')
    itemlist = xmldoc.getElementsByTagName('page')
    i = 0

    for item in itemlist:
        w_id = item.getElementsByTagName('id')[0].firstChild.nodeValue
        title = item.getElementsByTagName('title')[0].firstChild.nodeValue
        text = item.getElementsByTagName('text')[0].firstChild.nodeValue

        PageInst = Page(w_id, title, text)

        print("================================")
        print(title)
        print("================================")
    #     neoInst.print_create_page(w_id, title, text)

        text = PageInst.process_text()
        
        print(text)

        sentences = PageInst.tokenize_text()
        for sentence in sentences:
            print(sentence)

        i += 1 
        if i > 20:
            break
    #     for link in links:
    #         neoInst.create_relationship(title, str(link.title))
    # neoInst.close()
