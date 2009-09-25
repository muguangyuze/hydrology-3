import cloudfiles
import os
import hashlib

class Hydro(object):

    def loadConfig(self):
        import cloudfiles
        from config import config
        self.config = config

    def getConnection(self, name, key):
        self.connection = cloudfiles.get_connection( name, key )
        if self.connection is None: raise ArgumentError, 'no connection returned'
        return self.connection

    def getContainer(self):
        self.container = self.connection.get_container( self.config['container'] )
        if self.container is None: raise ArgumentError, 'no container returned for:\t%s' % self.config['container']
        return self.container

    def getFileInfo(self, filelist=[]):
        if len(filelist)==0: return self.container.list_objects_info()
        fileinfo = []
        for filename in filelist:
            basename = os.path.basename(filename)
            for remote_info in self.container.list_objects_info(prefix=basename):
                if remote_info['name'] == basename: fileinfo.append(remote_info)
        return fileinfo

    def checkConfig(self, check='all'):

        for configName in 'name key container'.split(' '):
            if configName not in self.config or self.config[configName] == '':
                raise ArgumentError, 'missing from config:t%s' % configName

        # print "Config\tOK!"
        if check == 'conn' or check == 'cont' or check == 'all':
            if not self.getConnection(self.config['name'], self.config['key']):
                print "There was a Connection\tPROBLEM!"
                return False

        if check == 'cont' or check == 'all':
            if not self.getContainer():
                print "There was a PROBLEM w/ Container %s!" % self.config['container'] 
                return False

        return True

    def putFile(self, filename):
        if os.path.isfile(filename):
            basename   = os.path.basename(filename)
            local_hash = self.hashFile(filename)
            local_size = os.path.getsize(filename)
            listinfo = self.getFileInfo([basename])
            if len(listinfo)>0 and listinfo[0] != None and listinfo[0]['bytes']==local_size and listinfo[0]['hash']==local_hash:
                print "File exists in cloud w/ same size, md5sum: %s / %s\t%d bytes, %s" % (self.container, basename, local_size, local_hash)
                print "File NOT put to cloud!"
                return False

            cloudfile = self.container.create_object( basename )
            cloudfile.load_from_filename( filename )
            print 'put cloud object: %s / %s %d bytes' % (self.container, cloudfile.name, cloudfile.size)
        else:
            warnings.warn( 'put fail, not a file: %s' % filename )
        return

    def getFile(self, filename):
        self.container.get_object(filename).save_to_filename(filename)
        print "saved from %s container to file:\t%s" % (self.container, filename)

    def listFiles(self, fileList=[]):
        for fileinfo in self.getFileInfo(fileList):
            print "%s:\n\t%s bytes,\n\tmd5:\t%s" % (fileinfo['name'], fileinfo['bytes'], fileinfo['hash'])

    def rmFile(self, filename):
        rm_file = os.path.basename(filename)
        self.container.delete_object(rm_file)
        return True

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
