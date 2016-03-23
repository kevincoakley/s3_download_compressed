import gzip
import boto3
import os.path
import threading
import swiftclient.client
from botocore import exceptions as boto_exception

os_tenant_name = os.environ.get("OS_TENANT_NAME", '')
os_username = os.environ.get("OS_USERNAME", '')
os_password = os.environ.get("OS_PASSWORD", '')
os_auth_url = os.environ.get("OS_AUTH_URL", '')

s3_bucket = "ucsd-twitter"
s3_root = "data/"

working_directory = "/save/"


def s3_object_exists(d_s3_object_path):

    # Try to load the metadata for the S3 object, return False if the request receives a file not
    # found error
    try:
        boto3.resource('s3').Object(s3_bucket, d_s3_object_path).load(RequestPayer='requester')
    except boto_exception.ClientError as d_e:
        if d_e.response['Error']['Code'] == "404":
            exists = False
        else:
            raise d_e
    else:
        exists = True

    return exists


def worker(d_filename):

    local_path = "%s%s" % (working_directory, d_filename)

    swift = swiftclient.client.Connection(auth_version='2',
                                          user=os_username,
                                          key=os_password,
                                          tenant_name=os_tenant_name,
                                          authurl=os_auth_url)
    with open(local_path, 'rb') as fo:
        file_data = fo.read()
    swift.put_object('twitter', d_filename, file_data)
    print "%s uploaded to swift!" % d_filename

    # Remove the file once uploaded to Swift and create a empty file in its place
    os.remove(local_path)
    open(local_path, 'a').close()


if __name__ == "__main__":

    count = 1

    while count < 400:

        # Generate the S3 object name from the counter and the S3 object path
        s3_object_name = "election%03d" % count
        s3_object_path = "%s%s" % (s3_root, s3_object_name)

        # Generate the local filename from the S3 object name and the working directory
        filename = "%s.gz" % count
        save_path = "%s%s" % (working_directory, filename)

        # Only try to download the S3 object if it doesn't exist on the local computer
        if not os.path.exists(save_path):

            # Check if the object exists in S3 before trying to download it
            if s3_object_exists(s3_object_path):
                print "Start: %s" % save_path

                # Download the uncompressed S3 object to a gzip compressed file on the local
                # computer
                try:
                    key = boto3.resource('s3').Object(s3_bucket, s3_object_path)\
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

                # Once the download of the S3 object is complete then create a thread to upload
                # the local compressed file to Swift
                t = threading.Thread(target=worker, args=(filename,))
                t.daemon = True
                t.start()
                t.join()
            else:
                print "%s does not exist on S3" % s3_object_path
        else:
            print "%s exists, skipping!" % save_path

        count += 1
