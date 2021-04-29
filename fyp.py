from neo4j import GraphDatabase
from lxml import etree, objectify
import mwparserfromhell
import spacy
import re
import sys
import math
import enchant

"""
The interface to the Neo4J database. 
"""
class Neo4JInterface:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_create_page(self, w_id, title, text, page_type):
        # Calls the private create_page method, and uses it to create a page.
        # Requires the ID of the wiki page, the title, text, and the type of page.
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_page, w_id, title, text, page_type)
            print(result)

    def print_create_relationship(self, link_from, link_to, relation):
        # Creates a relationship between two pages.
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_relationship, link_from, link_to, relation)
            if result != None: print(result, "with relation:", relation)

    @staticmethod
    def _create_page(tx, w_id, title, text, page_type):
        query = "MERGE (n:$PTYPE { id: $id, name: $name })".replace("$PTYPE", page_type)
        result = tx.run(query +
                        "ON CREATE SET n.text = $text  "
                        "ON MATCH SET n.text = $text  "
                        "RETURN n.name, n.id ", id=w_id, name=title, text=text, ptype=page_type)
        return result.single()[0]

    @staticmethod
    def _create_relationship(tx, link_from, link_to, relation):
        # Apparently the $RELATION won't be replaced by the tx.run variable substitution
        # mechanism, therfore I've adopted this janky approach.
        query = ("MATCH (from { name: $link_from }) "
                 "MATCH (to { name: $link_to }) "
                 "MERGE (from)-[rel:$RELATION]->(to)".replace("$RELATION", relation))

        query += " RETURN from.name, to.name"
        result = tx.run(query, link_from=link_from, link_to=link_to)
        result = result.single()
        return result
    
class Page:
    def __init__(self, w_id, title, text):
        self.w_id = w_id
        self.title = title
        self.text = text
        self.wikicode = mwparserfromhell.parse(text)
        self.full_links = self.links()
        self.partial_links = self.lookup_links()
        self.templates = self.wikicode.filter_templates()

    def links(self):
        r = []
        for link in self.wikicode.filter_wikilinks():
            link = link.title
            if not link.startswith("File:"):
                # If there is an anchor in the page, we remove it.
                r.append(link.split("#", 1)[0])
        return r
    
    def lookup_links(self):
        """
        Returns a list with all the links split into their word components.
        This is to enable easier lookups, as NP chunks will likely split the word up.
        And there will be other components in the NP chunks, muddying lookup.
        """
        r = []
        for link in self.full_links:
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

    def rel_standardise(self, rel):
        """
        Changes relationships to a standard format, and changes any spaces to underscores.
        """
        return rel.upper().strip().replace(" ", "_")

    def find_link_relation_word(self, max_deps, nlp, dictionary):
        """
        It takes the current page, filters it by links, processes with spaCy NLP,
        loops over all NP chunks, checks if it is a link, and finds the relation word
        that links the current page to the link. It restricts the amount of relation words 
        per link depending on the value of max_dependencies.
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
                dependency = self.rel_standardise(chunk.root.head.text)
                for word in chunk.text.split(" "):
                    if word in self.partial_links:
                        link.append(word)
                link = " ".join(link)
                if link in self.full_links and dictionary.check(dependency) and len(dependency) > 1:
                    if link in link_dependency:
                        if len(link_dependency[link]) >= max_deps:
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

    def infobox_link_dep(self):
        """
        Finds the Infobox template within a page, and returns any links present within
        the parameter value, along with the name of the parameter it originates from.
        """
        link_dependency = {}
        for template in self.templates:
            # Not always the first link in a page.
            if str(template.name).lower().startswith("infobox"):
                for param in template.params:
                    for link in param.value.filter_wikilinks():
                        link = link.title.split("#", 1)[0]
                        if not link.startswith("File:") and not link == title:
                            dependency = self.rel_standardise(str(param.name))
                            link_dependency[link] = {dependency}
        return link_dependency

if __name__ == "__main__":
    # Parses the arguments for input file.
    parser = argparse.ArgumentParser()
    parser.add_argument('input', type=str,
                        help='name of input filename')
    parser.add_argument('neo4j_address', type=str,
                        help='address of the neo4j server')
    parser.add_argument('neo4j_username', type=str,
                        help='username of neo4j server')
    parser.add_argument('neo4j_password', type=str,
                        help='neo4j db password')
    args = parser.parse_args()

    neoInst = Neo4JInterface(args.neo4j_address, args.neo4j_username, args.neo4j_password)
    # The file that will be processed. 
    xmldoc = etree.parse(args.input)
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
        page_type = "Page"
        
        PageInst = Page(w_id, title, text)

        for template in PageInst.templates:
            # If the infobox is annotated with a type, use it as the page type
            if str(template.name).lower().startswith("infobox") and len(template.name) > 2:
                page_type = template.name.split(" ")[-1].capitalize()

        neoInst.print_create_page(w_id, title, text, page_type)

    print("Creating relationships")
    # As the iterator is reset, we'll instantiate it again.
    itemlist = root.iterfind("page")
    for item in itemlist:
        # Retrieves the page ID, title and page text.
        w_id = item.find('id').text
        title = item.find('title').text
        text = item.find('revision').find('text').text

        # Creates a page object
        PageInst = Page(w_id, title, text)
        # Finds links on each page, with a relation word that links them together.
        link_dependency = PageInst.find_link_relation_word(2, nlp, dictionary)
        info_link_dependency = PageInst.infobox_link_dep()

        # Overrides any links from the unstructured links with the structured links.
        for link in info_link_dependency:
            link_dependency[link] = info_link_dependency[link]

        # Writes it all into the database.
        for link in link_dependency:
            for relation in link_dependency[link]:
                neoInst.print_create_relationship(title, link, relation)
    neoInst.close()
