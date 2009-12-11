# import cloudfiles
import os
import hashlib

class Hydro(object):

    def loadConfig(self):
        from config import config
        if   config['cloud'] == 'amazon':    import boto
        elif config['cloud'] == 'rackspace': import cloudfiles

        self.config = config

    def getConnection(self):

        if   self.config['cloud'] == 'amazon':
            from hydroConnection import HydroConnection
            self.connection = HydroConnection( self.config['key'], self.config['secret'])

        elif self.config['cloud'] == 'rackspace':
            import cloudfiles
            self.connection = cloudfiles.Connection( self.config['name'],  self.config['key'] )

        if self.connection is None: raise ArgumentError, 'no connection returned'
        return self.connection

    def getContainer(self):
        if self.config['cloud'] == 'amazon':    self.container = self.connection.get_container( self.config['bucket'] )
        if self.config['cloud'] == 'rackspace': self.container = self.connection.get_container( self.config['container'] )

        if self.container is None: raise ArgumentError, 'no container returned for:\t%s' % self.config['container']
        return self.container

    def getFileInfo(self, filelist=[]):
        # self.getContainer()
        if len(filelist)==0: return self.container.list_objects_info()
        fileinfo = []
        for filename in filelist:
            basename = os.path.basename(filename)
            for remote_info in self.container.list_objects_info(prefix=basename):
                if remote_info['name'] == basename: fileinfo.append(remote_info)
        return fileinfo

    def checkConfig(self, check='all'):

        if   self.config['cloud'] == 'rackspace': check_keys = 'name key container'.split()
        elif self.config['cloud'] == 'amazon':    check_keys = 'key secret bucket'.split()

        for key in check_keys:
            if key not in self.config or self.config[key] == '':
                raise ValueError, 'missing from config:t%s' % key
        self.getConnection()
        self.getContainer()

        # print "Config\tOK!"
        # if check == 'conn' or check == 'cont' or check == 'all':
        #     if not self.getConnection(self.config['name'], self.config['key']):
        #         print "There was a Connection\tPROBLEM!"
        #         return False
        # 
        # if check == 'cont' or check == 'all':
        #     if not self.getContainer():
        #         print "There was a PROBLEM w/ Container %s!" % self.config['container'] 
        #         return False

        return True

    def putFile(self, filename):
        if os.path.isfile(filename):
            basename   = os.path.basename(filename)
            # local_hash = self.hashFile(filename)
            # local_size = os.path.getsize(filename)
            # listinfo = self.getFileInfo([basename])
            # 
            # if len(listinfo)>0 and listinfo[0] != None and listinfo[0]['bytes']==local_size and listinfo[0]['hash']==local_hash:
            #     print "File exists in cloud w/ same size, md5sum: %s / %s\t%d bytes, %s" % (self.container, basename, local_size, local_hash)
            #     print "File NOT put to cloud!"
            #     return False

            # self.container.put_file(filename)
            cloudfile = self.container.create_object( basename )
            cloudfile.load_from_filename( filename )
            # print 'put cloud object: %s / %s %d bytes' % (self.container, cloudfile.name, cloudfile.size)
            print cloudfile.key_name
            print dir(cloudfile)
        else:
            warnings.warn( 'put fail, not a file: %s' % filename )
        return

    def getFile(self, filename):
        self.container.get_object(filename).save_to_filename(filename)
        print "saved from %s container to file:\t%s" % (self.container, filename)


    def listFiles(self, fileList=[]):
        self.getContainer()
        list = self.getFileInfo(fileList)
        if len(list) <=0:
            print 'empty container:', self.config['bucket']
        else:
            for fileinfo in list:
                print "%s:\n\t%s bytes,\n\tmd5:\t%s" % (fileinfo['name'], fileinfo['bytes'], fileinfo['hash'])

    def rmFile(self, filename):
        rm_file = os.path.basename(filename)
        self.container.delete_object(rm_file)
        return True

    def list_containers(self, containerList=[]):
        print "Available containers:"

        print '\n'.join( self.connection.list_containers() )

    def mkContainer(self, container_name):
        self.connection.create_container(container_name)
        return True
        
    def rmContainer(self, container_name):
        self.connection.delete_container(container_name)
        return True

    def hashFile(self, filename):
        md5          = hashlib.md5()
        local_file   = open(filename)
        md5.update(local_file.read())
        local_file.close()
        return md5.hexdigest()
