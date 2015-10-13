#!/usr/bin/env python

import sys
from optparse import OptionParser
from pypipeline import scrape
from pypipeline.scrape import Scraper

if __name__ == "__main__":
    usage = "%prog [top_dir...]"

    parser = OptionParser(usage=usage)
    scrape.add_options(parser)
    (options, args) = parser.parse_args(sys.argv)

    if len(args) < 2:
        parser.print_help()
        sys.exit(1)
    
    scraper = Scraper(options)
    for top_dir in args[1:]:
        scraper.scrape(top_dir)
