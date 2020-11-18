from xml.dom import minidom
from tqdm import tqdm
import argparse

def pretty_print(data): 
    """
    Removes the whitespace from XML output.
    Credit to: https://stackoverflow.com/questions/14479656/empty-lines-while-using-minidom-toprettyxml
    """
    return '\n'.join([line for line in minidom.parseString(
    data).toprettyxml(indent=' '*2).split('\n') if line.strip()])

def remove_element(parent_element, name_of_child):
    namespaces = parent_element.getElementsByTagName(name_of_child)[0]
    parent = namespaces.parentNode
    parent.removeChild(namespaces)

if __name__ == "__main__":
    # Parses the arguments for input file and destination.
    parser = argparse.ArgumentParser()
    parser.add_argument('input', type=str,
                    help='name of input filename')
    parser.add_argument('dest', type=str,
                    help='name of destination filename')
    args = parser.parse_args()
    

    print("Reading MediaWiki XML file. This may take a little bit of time.")
    xmldoc = minidom.parse(args.input)

    print("Stripping namespaces")
    # Here, we strip all the namespace definitions. We have no need for this.
    remove_element(xmldoc, "namespaces")
    itemlist = xmldoc.getElementsByTagName('page')

    print("Stripping information from each page.")
    # tqdm adds a progress bar. Useful for such a long operation.
    for item in tqdm(itemlist):
        redirect = item.getElementsByTagName('redirect')
        remove_elements = ["contributor", "comment", "parentid", "model", "format", "timestamp", "minor", "ns"]
        text = item.getElementsByTagName('text')[0].firstChild.nodeValue


        # Remove any redirect or disambiguation pages. Pollutes our data.
        if len(redirect) >= 1 or "{{disambig}}" in text.lower():
            parent = item.parentNode
            parent.removeChild(item)
        else:
            for remove_target in remove_elements:
                try:
                    remove_element(item, remove_target)
                # Certain tags may only be on some pages. This doesn't matter, we just ignore the error.
                except Exception:
                    pass


    print("Writing cleaned XML to disk.")
    with open(args.dest, "w", encoding="utf-8") as writeFile:
        writeFile.write(pretty_print(xmldoc.toxml()))
    print("Completed.")
