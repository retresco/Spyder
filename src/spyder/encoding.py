#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# encoding.py 09-Feb-2011
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#


def get_content_type_encoding(curi):
    """
    Determine the content encoding based on the `Content-Type` Header.

    `curi` is the :class:`CrawlUri`.
    """
    content_type = "text/plain"
    charset = ""

    if curi.rep_header and "Content-Type" in curi.rep_header:
        (content_type, charset) = extract_content_type_encoding(
                curi.rep_header["Content-Type"])

    if charset == "" and curi.content_body and len(curi.content_body) >= 512:
        # no charset information in the http header
        first_bytes = curi.content_body[:512].lower()
        ctypestart = first_bytes.find("content-type")
        if ctypestart != -1:
            # there is a html header
            ctypestart = first_bytes.find("content=\"", ctypestart)
            ctypeend = first_bytes.find("\"", ctypestart + 9)
            return extract_content_type_encoding(
                    first_bytes[ctypestart + 9:ctypeend])

    return (content_type, charset)


def extract_content_type_encoding(content_type_string):
    """
    Extract the content type and encoding information.
    """
    charset = ""
    content_type = ""
    for part in content_type_string.split(";"):
        part = part.strip().lower()
        if part.startswith("charset"):
            charset = part.split("=")[1]
            charset = charset.replace("-", "_")
        else:
            content_type = part

    return (content_type, charset)
