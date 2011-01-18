#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# limiter.py 18-Jan-2011
#
# All programs in this directory and
# subdirectories are published under the GNU General Public License as
# described below.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or (at
# your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
# USA
#
# Further information about the GNU GPL is available at:
# http://www.gnu.org/copyleft/gpl.html
#
#
"""
A processor used for limiting the extraction and scoping processings.

Basically this will be used for ignoring any `robots.txt` for being processed.
"""

from spyder.core.constants import CURI_OPTIONAL_TRUE, CURI_EXTRACTION_FINISHED


def create_processor(settings):
    """
    Create a new `do_not_process_robots` processor using the given `Settings`.
    """
    return do_not_process_robots


def do_not_process_robots(curi):
    """
    Do not process `CrawlUris` if they are **robots.txt** files.
    """
    if CURI_EXTRACTION_FINISHED not in curi.optional_vars and \
        curi.effective_url.endswith("robots.txt"):
        curi.optional_vars[CURI_EXTRACTION_FINISHED] = CURI_OPTIONAL_TRUE

    return curi
