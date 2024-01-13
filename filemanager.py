#!/usr/bin/env python3

from lib.meta import TVScanner

scanner = TVScanner()
#scanner.add_new_shows()
#scanner.add_new_episodes()
scanner.update_shows()
scanner.close()