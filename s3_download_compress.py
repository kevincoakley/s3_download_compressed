import gzip
import boto3
import os.path

count = 1

while count < 100:

    save_path = "/save/election%03d.gz" % count

    if not os.path.exists(save_path):

        print "Start: data/election%03d" % count

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

        print "End: data/election%03d\n" % count

    else:
        print "%s exists, skipping!" % save_path

    count += 1
