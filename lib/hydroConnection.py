# wrapper to emulate rackspace cloudfile connection methods in amazon s3 boto connection

class HydroConnection(object):

    def __init__(self, key, secret_key):
        import boto
        self.connection = boto.connect_s3(key, secret_key)
        # return self.connection

    def get_container(self, bucket_name):
        from hydroBucket import HydroBucket
        # TODO: validate container_name
        return HydroBucket(self.connection, bucket_name)

    # def get_containers(self):
    #     return self.connection.get_all_buckets()

    def create_container(self, container_name):
        # TODO: validate container_name
        return self.connection.create_bucket(container_name)

    def delete_container(self, container_name):
        return self.connection.delete_bucket(container_name)

    def list_containers(self):
        list = []
        for b in self.connection.get_all_buckets(): list.append(b.name)
        return list


    # TODO: implement wrappers to methods named for cloudfiles equivs, as needed


