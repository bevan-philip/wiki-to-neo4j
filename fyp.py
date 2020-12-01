from neo4j import GraphDatabase
from lxml import etree, objectify
import mwparserfromhell
import nltk
import re
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
    def _create_page(tx, page):
        result = tx.run("MERGE (n:Item { id: $id, name: $name })"
                        "ON CREATE SET n.text = $text  "
                        "ON MATCH SET n.text = $text  "
                        "RETURN n.id, n.name ", id=page.w_id, name=page.title, text=page.text)
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
        filtered = self.wikicode.strip_code()
        # Ensures all the words are split.
        filtered = filtered.replace("\n", " ").split(" ")

        # The RS Wiki places an image at the start of most articles.
        # The image's text is "left" in the Wikicode, but it isn't.
        # This is our crude attempt at filtering it out.
        if filtered[0].startswith("left"):
            del filtered[0]

        return " ".join(map(str, filtered))

    def tokenize_text(self):
        """
        Converts the word into a sentence, and then tokenizes
        and tags the sentence.
        """
        sentences = nltk.sent_tokenize(self.process_text())
        sentences = [nltk.word_tokenize(sent) for sent in sentences]
        sentences = [nltk.pos_tag(sent) for sent in sentences]
        return sentences


class BigramChunker(nltk.ChunkParserI):
    def __init__(self, train_sents):
        train_data = [[(t, c) for _, t, c in nltk.chunk.tree2conlltags(sent)]
                      for sent in train_sents]
        self.tagger = nltk.BigramTagger(train_data)

    def parse(self, sentence):  # [_code-unigram-chunker-parse]
        pos_tags = [pos for (word, pos) in sentence]
        tagged_pos_tags = self.tagger.tag(pos_tags)
        chunktags = [chunktag for (pos, chunktag) in tagged_pos_tags]
        conlltags = [(word, pos, chunktag) for ((word, pos), chunktag)
                     in zip(sentence, chunktags)]
        return nltk.chunk.conlltags2tree(conlltags)

def traverse(t, links, active_v=""):
    try:
        t.label()
    except AttributeError:
        if t[1].startswith("V") and t[0] != "P":
            active_v = t[0]
        return (t, active_v)
    else:
        # Now we know that t.node is defined
        if t.label() == "NP":
            total_np_chunk = []
            matches = ""
            traversal = ()
            for child in t:
                if child[0] in links:
                    matches += child[0] + " "
                traversal = traverse(child, links, active_v)
                total_np_chunk.append(traversal[0])
            if len(matches) > 0:
                print(traversal[1] + " rel " + matches)
        else:
            for child in t:
                traversal = traverse(child, links, active_v)
                if len(traversal) > 1:
                    active_v = traversal[1]
        return t.label()

if __name__ == "__main__":
    # neoInst = Neo4JInterface("bolt://localhost:7687", "neo4j", "e")

    xmldoc = etree.parse('test.xml')
    root = xmldoc.getroot()

    # Strips the tags of namespaces. Makes traversal easier.
    # We can still do this and be far more efficient than Minidom.
    for elem in root.iterdescendants():
        elem.tag = etree.QName(elem).localname

    # Creates an iterator for all the page elements, so I can iterate over them.
    itemlist = root.iterfind("page")
    i = 0

    # Dataset to train BiggramChunker
    train_sents = nltk.corpus.conll2000.chunked_sents(
        "train.txt", chunk_types=['NP', 'VB'])
    bigram_chunker = BigramChunker(train_sents)

    for item in itemlist:
        # Retrieves the page ID, title and page text.
        w_id = item.find('id').text
        title = item.find('title').text
        text = item.find('revision').find('text').text

        PageInst = Page(w_id, title, text)

        print("================================")
        print(PageInst.title)
        print("================================")
    #     neoInst.print_create_page(w_id, title, text)

        text = PageInst.process_text()
        
        # print(text)

        sentences = PageInst.tokenize_text()

        for sent in sentences:
            chunked = bigram_chunker.parse(sent)
            # print(PageInst.links())
            traverse(chunked, PageInst.lookup_links())

        i += 1 
        if i > 20:
            break
    #     for link in links:
    #         neoInst.create_relationship(title, str(link.title))
    # neoInst.close()
