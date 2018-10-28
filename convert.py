# -*- coding: utf-8 -*-


import os
import io
import gzip
from lxml import etree
from tqdm import tqdm


# Convert GZipped XML to plain text
def to_plain_text(input_path, output_path):
    
    # Open archive for streaming
    with gzip.open(input_path, 'r') as input:
        content = etree.iterparse(input, events=('start', 'end'))
        
        # Acquire root node properties
        _, root = next(content)
        assert root.tag == 'wikipedia'
        lang = root.attrib['lang']
        count = int(root.attrib['article'])
        
        # Stream articles
        with io.open(output_path, 'w', newline='\n', encoding='utf-8') as output:
            with tqdm(total=count) as progress:
                for action, element in content:
                    if action == 'end':
                        
                        # Free memory on end of articles and redirections
                        if element.tag == 'article' or element.tag == 'redirect':
                            root.clear()
                            if element.tag == 'article':
                                progress.update(1)
                        
                        # Process paragraph content
                        elif element.tag == 'p':
                            text = ''.join(element.itertext())
                            output.write(text)
                            output.write('\n')


# Standalone usage does the export process
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Extract plain text from Wikipedia XML GZ dump.')
    parser.add_argument('input', help='XML GZ input path')
    parser.add_argument('output', help='TXT output path')
    args = parser.parse_args()
    to_plain_text(args.input, args.output)
