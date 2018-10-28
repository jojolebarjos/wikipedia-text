# -*- coding: utf-8 -*-


import os
import io
import numpy
import lzma
from lxml import etree, html
import re
import gzip
from tqdm import tqdm
from collections import Counter


# Keep track of unknown tags, for debug purpose
unknown_tags = Counter()


# Precompile regular expressions used during processing
r_header = re.compile(r'h([0-9]+)', re.UNICODE)
r_white = re.compile(r'[ \t\f\r\n\u200B]+', re.UNICODE)


# Data placeholder used during processing
class Node:
    def __repr__(self):
        result = dict(vars(self))
        if 'content' in result:
            result['content'] = '...'
        return repr(result)


# Recursive decoding of HTML elements
def decode(element):
    
    # Helper to build node
    def build(tag=None, ignore_element=False, ignore_content=False, **args):
        if ignore_element:
            result = []
            if not ignore_content:
                if element.text:
                    result.append(element.text)
                for child in element.getchildren():
                    result.extend(decode(child))
            if element.tail:
                result.append(element.tail)
            return result
        node = Node()
        node.tag = tag or element.tag
        for key, value in args.items():
            setattr(node, key, value)
        node.content = []
        if not ignore_content:
            if element.text:
                node.content.append(element.text)
            for child in element.getchildren():
                node.content.extend(decode(child))
        result = [node]
        if element.tail:
            result.append(element.tail)
        return result
    
    # Ignore weird types
    if type(element.tag) is str:
    
        # Most structural elements are kept as-is
        if element.tag in {
            'blockquote',
            'ul',     # Unordered list container
            'ol',     # Ordered list container
            'dl',     # Description list container
            'li',     # List item
            'dt',     # Term item
            'dd'        # Description item
        }:
            return build()
        
        # Header are better with their level extracted
        match = r_header.fullmatch(element.tag)
        if match:
            return build('h', level = int(match.group(1)))
        
        # Some structural elements are considered as paragraphs
        if element.tag in {'div', 'p'}:
            return build('p')
        
        # Link are kept as-is
        if element.tag == 'a':
            return build(href = element.attrib['href'])
        
        # Abbreviations are kept, as they might provide useful insights
        if element.tag == 'abbr':
            # TODO maybe abbr entities are irrelevant
            return build(title = element.attrib.get('title', None))
        
        # Time markers are kept, as they might provide useful insights
        if element.tag == 'time':
            # TODO maybe time entities are irrelevant
            return build(datetime = element.attrib.get('datetime', None))
        
        # Keep some elements as-is
        if element.tag in {
            'cite',   # Reference citation (mostly used for footnotes)
            'q',      # Inline quotation
            'sub',    # Subscript
            'sup'     # Superscript
        }:
            return build()
        
        # Code and symbol like are kept under a single mark (to avoid confusion with unexpected content)
        if element.tag in {
            'code',   # Inline code snippet, usually for variables or short commands
            'kbd',    # Mark used for keys (can be considered as code)
            'tt',     # Typescript, usually rendered as monospaced characters
            'var'     # Variable marker, usually rendered as code
        }:
            return build('code')
        
        # Keep some element as marker, without their content
        if element.tag in {
            'br',     # Line-break may be useful
            'math'    # Math formulas are stripped, for simplicity
        }:
            return build(ignore_content=True)
        
        # Remaining text formatting is stripped
        # TODO maybe should keep some simplified formats (e.g. emphasis)
        if element.tag in {
            'b',      # Bold
            'bdi',    # Bi-directional isolation, used to handle mixed text orientation (probably useless in this usage)
            'big',    # Emphasis-like
            'del',    # Mark for removed/deprecated text, rendered as strikethrough
            'dfn',    # Emphases, usually rendered as bold
            'em',     # Emphasis (often rendered as italic)
            'font',   # Font, mostly used to define text color
            'i',      # Italic
            'ins',    # Mark for newly inserted text, usually used for revisions
            'mark',   # Highlight, usually used for revisions
            'rb',     # Ruby-related, base marker (this is the only one kept, as it is the actual content)
            'ruby',   # Ruby is used to annotate glyphs (usually Asian languages)
            's',      # Strikethrough
            'small',  # Emphasis-like
            'span',   # Generic structure used to apply custom formatting, stripped as it is too complicated too handle
            'strong', # Emphasis, usually rendered as bold
            'u',      # Underline
            'wbr'     # Word break opportunity, irrelevant for plain text corpora
        }:
            return build(ignore_element=True)
        
        # Some structures are completely ignored
        if element.tag in {
            'audio',  # Embedded audio player
            'center', # Centered block (usually used for banners, i.e. don't care)
            'hr',     # Horizontal rule (or topic change)
            'img',    # Embedded image
            'meta',   # Invisible properties
            'pre',    # Preserve plain text formatting, usually for code snippet (ignored for simplicity)
            'rp',     # Ruby-related, fallback parenthesis
            'rt',     # Ruby-related, pronunciation
            'rtc',    # Ruby-related, semantic annotation
            'table'   # Table (removed for simplicity)
        }:
            return build(ignore_element=True, ignore_content=True)
        
        # Unknown tags are ignored and reported
        unknown_tags[element.tag] += 1
    return build(ignore_element=True, ignore_content=True)


# Recursive flattening of decoded nodes, better suited for further processing
def flatten(content):
    
    # Accumulators
    sequence = []
    stack = []
    
    # Recursive traversal used to flatten the tree
    def traverse(content):
        for node in content:
            if type(node) is Node:
                
                # If this is a paragraph...
                if node.tag == 'p':
                    
                    # Close the previous one
                    if len(stack) > 0:
                        end = (False, stack[-1])
                        sequence.append(end)
                    
                    # Open the new (nested) one
                    stack.append(node)
                    begin = (True, node)
                    sequence.append(begin)
                    
                    # Add children
                    traverse(node.content)
                    
                    # Close it
                    end = (False, node)
                    sequence.append(end)
                    stack.pop()
                    
                    # Reopen the previous one
                    if len(stack) > 0:
                        begin = (True, stack[-1])
                        sequence.append(begin)
                
                # If this is a structural element...
                elif node.tag in {'h', 'blockquote', 'ul', 'ol', 'dl', 'li', 'dt', 'dd'}:
                    
                    # Close the current paragraph
                    if len(stack) > 0:
                        end = (False, stack[-1])
                        sequence.append(end)
                    
                    # Mark the beginning of the group
                    begin = (True, node)
                    sequence.append(begin)
                    
                    # Reopen the current paragraph
                    if len(stack) > 0:
                        begin = (True, stack[-1])
                        sequence.append(begin)
                    
                    # Add children
                    traverse(node.content)
                    
                    # Close the current paragraph again
                    if len(stack) > 0:
                        end = (False, stack[-1])
                        sequence.append(end)
                    
                    # Mark the end of the group
                    end = (False, node)
                    sequence.append(end)
                    
                    # And finally reopen the current paragraph again
                    if len(stack) > 0:
                        begin = (True, stack[-1])
                        sequence.append(begin)
                
                # Otherwise, this must be a formatting element...
                else:
                    
                    # Mark the beginning of the group
                    begin = (True, node)
                    sequence.append(begin)
                    
                    # Add children
                    traverse(node.content)
                    
                    # Mark the end of the group
                    end = (False, node)
                    sequence.append(end)
            
            # Add text as well
            else:
                sequence.append(node)
    
    # Process the whole sequence
    traverse(content)
    return sequence


# Concatenation and empty node pruning
def clean(sequence):
    
    # Accumulators
    result = []
    
    # Finalize current paragraph
    def accept(start, end):
        # TODO this might cause issues for code snippets
        has = False
        buffer = ''
        for i in range(start, end):
            item = sequence[i]
            if type(item) is str:
                buffer += r_white.sub(' ', item)
            else:
                if not has:
                    has = True
                    result.append(sequence[start - 1])
                    buffer = buffer.lstrip()
                    if len(buffer) > 0:
                        result.append(buffer)
                elif len(buffer) > 0:
                    result.append(buffer)
                buffer = ''
                result.append(item)
        if has:
            buffer = buffer.rstrip()
            if len(buffer) > 0:
                result.append(buffer)
            result.append(sequence[end])
        else:
            buffer = buffer.strip()
            if len(buffer) > 0:
                result.append(sequence[start - 1])
                result.append(buffer)
                result.append(sequence[end])
    
    # Process the whole sequence...
    # TODO remove nested quotes?
    # TODO make sure that list items are in list containers
    # TODO make sure that only list items are in lists
    # TODO check that headers are not in quote or list
    start = None
    for index, item in enumerate(sequence):
        if type(item) is tuple:
            begin, node = item
            
            # Isolate paragraphs
            if node.tag == 'p':
                if begin:
                    start = index + 1
                else:
                    accept(start, index)
                    start = None
            
            # And keep the other items as-is
            elif start is None:
                result.append(item)
    
    # Sequence is ready to be encoded
    return result


# Encode clean sequence into XML tree
def encode(url, title, sequence):
    
    # Pad sequence with global node
    root = Node()
    root.tag = 'root'
    root.title = title
    root.url = url
    sequence = [(True, root), *sequence, (False, root)]
    
    # Recursive generation of XML tree
    def build(start):
        
        # Create node, according to type
        index = start
        _, node = sequence[index]
        if node.tag == 'root':
            element = etree.Element('article')
            element.attrib['title'] = node.title
            element.attrib['url'] = node.url
        elif node.tag == 'h':
            element = etree.Element('h')
            element.attrib['level'] = str(node.level)
        elif node.tag == 'a':
            element = etree.Element('a')
            element.attrib['href'] = node.href
        elif node.tag == 'abbr':
            element = etree.Element('abbr')
            if node.title:
                element.attrib['title'] = node.title
        elif node.tag == 'time':
            element = etree.Element('time')
            if node.datetime:
                element.attrib['datetime'] = node.datetime
        elif node.tag in {'blockquote', 'ul', 'ol', 'dl', 'li', 'dt', 'dd', 'p', 'cite', 'q', 'sub', 'sup', 'code', 'math', 'br'}:
            element = etree.Element(node.tag)
        else:
            raise AssertionError(node.tag)
        
        # Add trailing text as node text, if any
        index += 1
        if type(sequence[index]) is str:
            element.text = sequence[index]
            index += 1
        
        # Add following items as children, until end of block is reached
        while True:
            begin, node = sequence[index]
            if not begin:
                break
            
            # Build child
            child, index = build(index)
            
            # Add trailing text as child tail, if any
            if type(sequence[index]) is str:
                child.tail = sequence[index]
                index += 1
            
            # Register child
            element.append(child)
        
        # For headers and description titles, flatten inner paragraph (i.e. a header acts as a paragraph itself)
        if element.tag in {'h', 'dt'} and len(element) > 0:
            if len(element) > 1:
                # TODO handle multiple paragraphs per header/title, i.e. merge them
                print('WARNING: header/title has more than one paragraph (%s)' % url)
            paragraph = element[0]
            element.text = paragraph.text
            element[:] = paragraph[:]
        
        # Node is complete
        return element, index + 1
    
    # Generate elements for the whole sequence
    element, _ = build(0)
    
    # Hack: remove trailing license notice
    if len(element) > 0 and element[-1].tag == 'p' and element[-1].text and element[-1].text.startswith('This article is issued from'):
        del element[-1]
    
    # Ready
    return element


# Convert raw HTML bytes into clean XML article
def parse(url, title, data):
    
    # Parse HTML body and extract tree
    tree = html.fromstring(data)
    tree = tree.xpath('//div[@id="mw-content-text"]')
    root = Node()
    root.tag = 'p'
    if len(tree) > 0:
        root.content = decode(tree[0])
    else:
        root.content = []
    
    # Flatten and clean structure
    sequence = flatten([root])
    sequence = clean(sequence)
    
    # Encode into XML tree
    tree = encode(url, title, sequence)
    return tree


# Read zero-terminated byte string
def read_string(file):
    buffer = []
    while True:
        byte = file.read(1)
        if len(byte) == 0 or byte == b'\x00':
            break
        buffer.extend(byte)
    return bytes(buffer).decode('utf-8')


# Extract HTML articles and redirections from ZIM archive into a compressed XML file
def process(input_path, output_path, lang):
    
    # Define little-endian types
    uint8 = numpy.dtype(numpy.uint8).newbyteorder('<')
    uint16 = numpy.dtype(numpy.uint16).newbyteorder('<')
    uint32 = numpy.dtype(numpy.uint32).newbyteorder('<')
    uint64 = numpy.dtype(numpy.uint64).newbyteorder('<')
    
    # Open file
    with io.open(input_path, 'rb') as file:
    
        # Check header
        magic, = numpy.frombuffer(file.read(4), uint32)
        if magic != 72173914:
            raise IOError('invalid ZIM file')
    
        # Get counts and offsets
        file.seek(24)
        article_count, cluster_count = numpy.frombuffer(file.read(4 * 2), uint32)
        urls_offset, titles_offset, clusters_offset, mime_types_offset = numpy.frombuffer(file.read(8 * 4), uint64)
    
        # Get MIME types
        mime_types = []
        file.seek(mime_types_offset)
        while True:
            mime_type = read_string(file)
            if len(mime_type) == 0:
                break
            mime_types.append(mime_type)
    
        # Get directory offsets
        file.seek(urls_offset)
        directory_offsets = file.read(8 * article_count)
        directory_offsets = numpy.frombuffer(directory_offsets, uint64)
        directory_offsets = numpy.sort(directory_offsets)
        directory_urls = {}
    
        # For each directory, acquire metadata
        print('Discovering items...')
        redirect_items = []
        article_items = []
        for directory_index in tqdm(range(article_count)):
            
            # Check MIME type 
            file.seek(directory_offsets[directory_index])
            mime_type, = numpy.frombuffer(file.read(2), uint16)
            if mime_type == 0xfffe or mime_type == 0xfffd:
                continue
            redirect = mime_type == 0xffff
            if not redirect:
                mime_type = mime_types[mime_type]
                if mime_type != 'text/html':
                    continue
            
            # Check namespace
            file.seek(1, 1)
            namespace, = numpy.frombuffer(file.read(1), uint8)
            namespace = chr(namespace)
            if namespace != 'A':
                continue
            
            # Acquire location
            file.seek(4, 1)
            if redirect:
                redirect_index, = numpy.frombuffer(file.read(4), uint32)
            else:
                cluster_index, blob_index = numpy.frombuffer(file.read(4 * 2), uint32)

            # Acquire name
            url = read_string(file)
            title = read_string(file)
            
            # Register item
            directory_urls[directory_index] = url
            if redirect:
                redirect_items.append((url, title, redirect_index))
            else:
                article_items.append((url, title, cluster_index, blob_index))
    
        # Open compressed output file for streaming
        with etree.xmlfile(output_path, encoding='utf-8', compression=9) as xml_file:
            xml_file.write_declaration()
            
            # Add root node with various information
            attributes = {
                'article' : str(len(article_items)),
                'redirect' : str(len(redirect_items)),
                'lang' : lang
            }
            with xml_file.element('wikipedia', attrib=attributes):
                xml_file.write('\n')
            
                # Exporting redirections
                print('Writing redirections...')
                for url, title, redirect_index in tqdm(redirect_items):
                    node = etree.Element('redirect')
                    node.attrib['url'] = url
                    node.attrib['title'] = title
                    node.attrib['target'] = directory_urls[redirect_index]
                    xml_file.write(node)
                    xml_file.write('\n')
                
                # Get cluster offsets
                file.seek(clusters_offset)
                cluster_offsets = file.read(8 * cluster_count)
                cluster_offsets = numpy.frombuffer(cluster_offsets, uint64)
                
                # Get cluster list and associated blobs
                article_items_per_cluster = {}
                for url, title, cluster_index, blob_index in article_items:
                    if cluster_index not in article_items_per_cluster:
                        cluster = []
                        article_items_per_cluster[cluster_index] = cluster
                    else:
                        cluster = article_items_per_cluster[cluster_index]
                    cluster.append((url, title, blob_index))
                
                # Stream relevant clusters
                print('Writing articles...')
                with tqdm(total=len(article_items)) as progress:
                    for cluster_index in sorted(article_items_per_cluster, key=lambda cluster_index: cluster_offsets[cluster_index]):
                        
                        # Jump to cluster and open sub-stream, according to compression level
                        start = int(cluster_offsets[cluster_index])
                        file.seek(start)
                        compression_type, = numpy.frombuffer(file.read(1), uint8)
                        if compression_type == 4:
                            subfile = lzma.open(file)
                            start = 0
                        else:
                            subfile = file
                            start += 4
                        
                        # Acquire blob table
                        first_offset, = numpy.frombuffer(subfile.read(4), uint32)
                        blob_count = first_offset // 4
                        offsets = numpy.empty(blob_count, numpy.int32)
                        offsets[0] = first_offset
                        offsets[1:] = numpy.frombuffer(subfile.read(4 * (blob_count - 1)), uint32)
                        
                        # For each relevant blob, read bytes
                        for url, title, blob_index in sorted(article_items_per_cluster[cluster_index], key=lambda x: x[2]):
                            subfile.seek(start + offsets[blob_index])
                            data = subfile.read(offsets[blob_index + 1] - offsets[blob_index])
                            
                            # Convert and export article
                            node = parse(url, title, data)
                            xml_file.write(node)
                            xml_file.write('\n')
                            progress.update(1)
    
    # Report unknown tags
    print('Unknown tags:')
    for tag, count in unknown_tags.most_common():
        print('  %s: %s' % (tag, count))


# Standalone usage does the export process
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Extract XML from Wikipedia ZIM dump.')
    parser.add_argument('input', help='ZIM input path')
    parser.add_argument('output', help='Gzipped XML output path')
    parser.add_argument('language', help='language code (en, fr, de, it...)')
    args = parser.parse_args()
    process(args.input, args.output, args.language)
