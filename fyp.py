from neo4j import GraphDatabase
from lxml import etree, objectify
import mwparserfromhell
import nltk
import spacy
import re
import sys
import math
import enchant

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

    def print_create_relationship(self, link_from, link_to, relation):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_relationship, link_from, link_to, relation)
            print(result)

    @staticmethod
    def _create_page(tx, w_id, title, text):
        result = tx.run("MERGE (n:Item { id: $id, name: $name })"
                        "ON CREATE SET n.text = $text  "
                        "ON MATCH SET n.text = $text  "
                        "RETURN n.name, n.id ", id=w_id, name=title, text=text)
        return result.single()[0]

    @staticmethod
    def _create_relationship(tx, link_from, link_to, relation):
        merge_query = "MERGE (from)-[rel:$RELATION]->(to)".replace("$RELATION", relation)
        query = ("MATCH (from: Item { name: $link_from }) "
                 "MATCH (to: Item { name: $link_to }) "
                 "MERGE (from)-[rel:$RELATION]->(to)".replace("$RELATION", relation))

        query += " RETURN from.name, to.name"
        result = tx.run(query, link_from=link_from, link_to=link_to)
        result = result.single()
        if result is not None:
            return result
        return "No relationship created."
    
class Page:
    def __init__(self, w_id, title, text):
        self.w_id = w_id
        self.title = title
        self.text = text
        self.wikicode = mwparserfromhell.parse(text)
        self.full_links = self.links()
        self.partial_links = self.lookup_links()

    def links(self):
        r = []
        for link in self.wikicode.filter_wikilinks():
            link = link.title
            if not link.startswith("File:"):
                r.append(link)
        return r
    
    def lookup_links(self):
        """
        Returns a list with all the links split into their word components.
        This is to enable easier lookups, as NP chunks will likely split the word up.
        And there will be other components in the NP chunks, muddying lookup.
        """
        r = []
        for link in self.wikicode.filter_wikilinks():
            link = link.title
            if not link.startswith("File:"):
                for y in link.split(" "):
                    r.append(y)
        return r
        
    def process_text(self):
        """
        Converts the text from Wikicode into plain text.
        """
        filtered = self.wikicode.strip_code(normalize=True)
        # Ensures all the words are split.
        filtered = filtered.replace("\n", " ").split(" ")

        # The RS Wiki places an image at the start of most articles.
        # The image's text is "left" in the Wikicode, but it isn't.
        # This is our crude attempt at filtering it out.
        if filtered[0].startswith("left"):
            del filtered[0]

        # strip_code isn't fully perfect on our dataset. Occasionally, remnants of images
        # sneak through as "thumb|XXXpx|Word", so we try and catch these instances, and
        # extract the word from it, or otherwise remove the broken word entirely.
        i = 0
        while i < len(filtered):
            if "thumb|" in filtered[i]:
                filtered[i] = filtered[i].split("|")[-1]
            if "File:" in filtered[i] or filtered[i].endswith("|left"):
                del filtered[i]
                i -= 1
            i += 1
            
        return " ".join(map(str, filtered)).strip()

    def find_link_relation_word(self, max_dependencies, nlp, dictionary):
        """
        It takes the current page, filters it by links, processes with spaCy NLP,
        loops over all NP chunks, checks if it is a link, and finds the relation word
        that links the current page to the link.
        """
        link_dependency = {}
        
        # Parses the text with the spaCy NLP that is passed through.
        doc = nlp(self.process_text())

        for chunk in doc.noun_chunks:
            # If the dependency type ends with "obj", it finds if there are
            # any links within the NP chunk.
            
            # If there are any links, it ensures they are complete links (i.e. not just
            # part of a link). After that, it'll add the link and dependency to the 
            # {link, set of dependencies}.
            if chunk.root.dep_.endswith("obj") and chunk.root.head.text.isalpha():
                link = []
                dependency = chunk.root.head.text.upper()
                for word in chunk.text.split(" "):
                    if word in self.partial_links:
                        link.append(word)
                link = " ".join(link)
                if link in self.full_links and dictionary.check(dependency) and len(dependency) > 1:
                    if link in link_dependency:
                        if len(link_dependency[link]) >= max_dependencies:
                            # Finds the minimum length word in the set. 
                            min_word = min(link_dependency[link], key=len)
                            # If the new dependency is bigger, we'll substitute it in.
                            if len(min_word) < len(dependency):
                                link_dependency[link].remove(min_word)
                                link_dependency[link].add(dependency)
                        else:
                            link_dependency[link].add(dependency)
                    else:
                        link_dependency[link] = {dependency}

        return link_dependency
    
if __name__ == "__main__":
    neoInst = Neo4JInterface("bolt://localhost:7687", "neo4j", "e")

    xmldoc = etree.parse('test.xml')
    root = xmldoc.getroot()

    # Creates an instance of the en_US dictionary.
    dictionary = enchant.Dict("en_US")

    # Strips the tags of namespaces. Makes traversal easier.
    # We can still do this and be far more efficient than Minidom.
    for elem in root.iterdescendants():
        elem.tag = etree.QName(elem).localname

    # Creates an iterator for all the page elements, so I can iterate over them.
    itemlist = root.iterfind("page")

    # Setup and load spaCy model
    nlp = spacy.load("en_core_web_sm")

    print("Parsing file.")

    for item in itemlist:
        # Retrieves the page ID, title and page text.
        w_id = item.find('id').text
        title = item.find('title').text
        text = item.find('revision').find('text').text

        neoInst.print_create_page(w_id, title, text)

    print("Creating relationships")
    # As the iterator is reset, we'll instantiate it again.
    itemlist = root.iterfind("page")
    for item in itemlist:
        # Retrieves the page ID, title and page text.
        w_id = item.find('id').text
        title = item.find('title').text
        text = item.find('revision').find('text').text
        
        PageInst = Page(w_id, title, text)

        link_dependency = PageInst.find_link_relation_word(2, nlp, dictionary)
        for link in link_dependency:
            for relation in link_dependency[link]:
                neoInst.print_create_relationship(title, link, relation)
    neoInst.close()
