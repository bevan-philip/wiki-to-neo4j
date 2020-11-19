from lxml import etree
from tqdm import tqdm
import argparse

if __name__ == "__main__":
    # Parses the arguments for input file and destination.
    parser = argparse.ArgumentParser()
    parser.add_argument('input', type=str,
                        help='name of input filename')
    parser.add_argument('dest', type=str,
                        help='name of destination filename')
    args = parser.parse_args()

    print("Reading MediaWiki XML file.")
    parsed = etree.parse(args.input)
    tree = parsed.getroot()

    print("Stripping namespaces")
    # Here, we strip all the namespace definitions. We have no need for this.
    namespaces = tree.xpath('siteinfo/namespaces')
    for elem in tqdm(namespaces):
        elem.getparent().remove(elem)

    print("Cleaning up tags.")
    # tqdm adds a progress bar. Useful for such a long operation.
    # Clean up the tags
    for elem in tqdm(tree.iterdescendants()):
        elem.tag = etree.QName(elem).localname

    print("Removing pages with redirects")
    redirect = tree.xpath('page/redirect')
    for elem in tqdm(redirect):
        page = elem.getparent()
        page.getparent().remove(page)

    remove_elements = ["contributor", "comment", "parentid",
                       "model", "format", "timestamp", "minor", "ns"]
    print("Cleaning up unneeded information")
    for remove in tqdm(remove_elements):
        for subelem in tree.iterfind('.//%s' % remove):
            subelem.getparent().remove(subelem)

    for elem in tree.iterfind('.//%s' % "text"):
        if "{{disambig}}" in elem.text.lower():
            page = elem.getparent().getparent()
            page.getparent().remove(page)

    etree.cleanup_namespaces(tree)
    with open("test.xml", "wb") as openFile:
        openFile.write(etree.tostring(tree, pretty_print=True))
