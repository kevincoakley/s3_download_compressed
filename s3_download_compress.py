import gzip
import boto3
import os.path
import threading
import swiftclient.client

os_tenant_name = os.environ.get("OS_TENANT_NAME", '')
os_username = os.environ.get("OS_USERNAME", '')
os_password = os.environ.get("OS_PASSWORD", '')
os_auth_url = os.environ.get("OS_AUTH_URL", '')

working_directory = "/save/"


def worker(object_filename):
    swift = swiftclient.client.Connection(auth_version='2',
                                          user=os_username,
                                          key=os_password,
                                          tenant_name=os_tenant_name,
                                          authurl=os_auth_url)
    with open("%s%s" % (working_directory, object_filename), 'rb') as fo:
        file_data = fo.read()
    swift.put_object('twitter', object_filename, file_data)
    print "%s uploaded to swift!" % object_filename

if __name__ == "__main__":

    count = 1

    while count < 100:

        filename = "election%03d.gz" % count
        save_path = "%s%s" % (working_directory, filename)

        if not os.path.exists(save_path):

            print "Start: %s" % save_path

            try:
                key = boto3.resource('s3').Object("ucsd-twitter", "data/election%03d" % count)\
                    .get(RequestPayer='requester')

                with gzip.open(save_path, 'w') as f:
                    chunk = key['Body'].read(1024*8)
                    while chunk:
                        f.write(chunk)
                        chunk = key['Body'].read(1024*8)

                f.close()
            except Exception as e:
                print "Exception: %s" % e

            print "End: %s\n" % save_path

            t = threading.Thread(target=worker, args=(filename,))
            t.daemon = True
            t.start()
            t.join()

        else:
            print "%s exists, skipping!" % save_path

        count += 1
