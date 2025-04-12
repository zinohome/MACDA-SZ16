#  @Email   : ibmzhangjun@139.com
#  @Software: MACDA
import functools
import os
import textwrap

from core.settings import settings
from utils.log import log as log
import simplejson as json

from naja_atra import request_map, ModelDict
from naja_atra import Request
from naja_atra import server

import requests


format_json = functools.partial(json.dumps, indent=2, sort_keys=True)
indent = functools.partial(textwrap.indent, prefix='  ')
def format_prepared_request(req):
    """Pretty-format 'requests.PreparedRequest'

    Example:
        res = requests.post(...)
        print(format_prepared_request(res.request))

        req = requests.Request(...)
        req = req.prepare()
        print(format_prepared_request(res.request))
    """
    headers = '\n'.join(f'{k}: {v}' for k, v in req.headers.items())
    content_type = req.headers.get('Content-Type', '')
    if 'application/json' in content_type:
        try:
            body = format_json(json.loads(req.body))
        except json.JSONDecodeError:
            body = req.body
    else:
        body = req.body
    s = textwrap.dedent("""
    REQUEST
    =======
    endpoint: {method} {url}
    headers:
    {headers}
    body:
    {body}
    =======
    """).strip()
    s = s.format(
        method=req.method,
        url=req.path,
        headers=indent(headers),
        body=body,
    )
    return s

@request_map("/", method=["GET", "POST", "PUT"])
@request_map("/test", method=["GET", "POST", "PUT"])
@request_map("/gate/METRO-PHM/api/faultRecordsSubsystem/saveRecord", method=["GET", "POST", "PUT"])
@request_map("/gate/METRO-PHM/api/devices/status/train/saveOrUpdate", method=["GET", "POST", "PUT"])
@request_map("/gate/METRO-SELFCHECK-SUBSYSTEM/api/faultRecordsSubsystem/saveStatus", method=["GET", "POST", "PUT"])
def hello(req=Request()):
    raw_request = format_prepared_request(req)
    log.debug(raw_request)
    return raw_request

def main():
    server.start(host="0.0.0.0", port=8080)

if __name__ == "__main__":
    main()