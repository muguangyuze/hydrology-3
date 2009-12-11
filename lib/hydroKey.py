# wrapper to emulate rackspace cloudfile methods in amazon s3 boto keys

class HydroKey(object):

    def __init__(self, connection, bucket_name, key_name):
        import boto
        self.connection, self.bucket_name, self.key_name = connection, bucket_name, key_name
        self.bucket    = boto.s3.bucket.Bucket(connection, bucket_name)
        self.key       = boto.s3.bucket.Key(self.bucket)
        self.key.key   = key_name


    def load_from_filename(self, filename):
        # TODO: mime_type
        self.key.set_contents_from_filename(filename)
