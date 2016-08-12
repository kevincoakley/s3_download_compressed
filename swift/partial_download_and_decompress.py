#!/usr/bin/env python

import os
import zlib
import swiftclient.client

os_tenant_name = os.environ.get("OS_TENANT_NAME", '')
os_username = os.environ.get("OS_USERNAME", '')
os_password = os.environ.get("OS_PASSWORD", '')
os_auth_url = os.environ.get("OS_AUTH_URL", '')

swift = swiftclient.client.Connection(auth_version='2',
                                      user=os_username,
                                      key=os_password,
                                      tenant_name=os_tenant_name,
                                      authurl=os_auth_url)

response_headers, swift_objects = swift.get_container("one_percent", prefix="2015_", limit=10)

for swift_object in swift_objects:
    print swift_object["name"]

    # Request the first 4.5 MB of the object
    request_headers = {"Range": "bytes=0-4500000"}

    response_headers, content = swift.get_object("one_percent",
                                                 swift_object["name"],
                                                 headers=request_headers)

    decompress_obj = zlib.decompressobj(16+zlib.MAX_WBITS)
    decompressed_string = decompress_obj.decompress(content)

    # The last line is most likely incomplete, remove it
    decompressed_string = decompressed_string[:decompressed_string.rfind('\n')]

    with open("output.txt", "a") as output_file:
        output_file.write(decompressed_string)
