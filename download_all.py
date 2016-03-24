#!/usr/bin/env python

import os
import sys
import requests
from HTMLParser import HTMLParser

url_base = sys.argv[1]


class MyHTMLParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self.data = None

    def handle_data(self, data):
        self.data = data


if __name__ == "__main__":

    files = []

    response = requests.get(url_base)

    if response.status_code is requests.codes.ok:

        html = response.text.split("\n")

        for line in html:

            if "colname" in line and "th" not in line:
                parser = MyHTMLParser()
                parser.feed(line)
                files.append(parser.data)

    else:
        print "HTTP Error: %s" % response.status_code

    if len(files) > 0:

        for filename in files:
            if not os.path.exists(filename):
                print "Downloading %s" % filename
                response = requests.get("%s%s" % (url_base, filename), stream=True)

                if response.status_code is requests.codes.ok:
                    with open(filename, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=1024):
                            if chunk:
                                f.write(chunk)
                                f.flush()
                else:
                    print "HTTP Error: %s" % response.status_code
            else:
                print "File %s exists, skipping." % filename
