# wrapper to emulate rackspace cloudfile container methods in amazon s3 boto buckets


class HydroBucket(object):

    def __init__(self, connection, bucket_name):
        import boto
        self.connection, self.bucket_name = connection, bucket_name
        self.bucket = boto.s3.bucket.Bucket(connection, bucket_name)
        
    def get_object(self, object_name):

        k = Key( self.bucket )
        k.key = object_name

    def create_object(self, filename):
        from hydroKey import HydroKey
        return HydroKey(self.connection, self.bucket, filename)
        # return self.bucket.new_key(filename)

    # def put_object(self, filename):
    #     from hydroKey import HydroKey
    # 
    #     from os import os.path.basename
    #     k = Key(self.bucket)
    #     k.key = basename(filename)
    #     k.set_contents_from_filename(filename)

    def delete_object(self, filename):
        return self.bucket.delete_key(filename)

    def list_objects_info(self):

        list = []
        for object in self.bucket.list(): list.append( {'name':object.key, 'bytes':object.size, 'hash':object.etag} )
        return list

