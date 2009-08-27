import os, sys, warnings
import cloudfiles
from config import name, key, container

def zbCloudFiles(argv):

    # configs = [ 'name', 'key', 'container']

    if getattr(self, 'name') is None:
        raise ArgumentError, 'invalid config.py: specify name'

    if key is None:
        raise ArgumentError, 'invalid config.py, specify key'

    if container is None:
        raise ArgumentError, 'invalid config.py, specify container'

    zb = cloudfiles.get_connection(name, key).get_container(container)

    # TODO: test for returned container, if missing: raise Exception
    # if zb == None: raise ArgumentError, 'invalid config.py'

    if len(argv)==1: task = 'help'
    else:            task = argv[1]

    if task == 'help':
        print "available tasks:"
        print "\tlist (<file(s)>)         \t# list <file(s)> or all files in container"
        print "\tput <file(s)>\t# load <file(s)> to container in cloud"
        print "\tget <file(s)>\t# download <file(s)> from container in cloud"
        print "\trm  <file(s)>\t# remove <file(s)> from container in cloud"

    elif task == 'list':
        zb_list = zb.list_objects()

        if len(argv)==2:
            print '\n'.join( zb.list_objects() )
        else:
            for list_file in argv[2:]:
                list_name = os.path.basename(list_file)
                if list_name in zb_list:
                    cf = zb.get_object(list_file)
                    print "File exists in cloud: %s / %s\t%d bytes" % (container, list_name, cf.size)


    elif task == 'put':

        zb_list = zb.list_objects()
        for put_file in argv[2:]:

            if os.path.isfile(put_file):

                put_name = os.path.basename(put_file)
                local_size = os.path.getsize(put_file)

                if (put_name in zb_list) and zb.get_object(put_name).size == local_size:
                    print "File exists in cloud w/ same size: %s / %s\t%d bytes" % (container, put_name, local_size)
                else:
                    cf = zb.create_object( put_name )
                    cf.load_from_filename(put_file)
                    print 'put cloud object: %s / %s %d bytes' % (container, cf.name, cf.size)
            else:
                warnings.warn( 'not a file: %s' % put_file )

    elif task == 'get':
        for get_file in argv[2:]:
            cf = zb.get_object(get_file)
            cf.save_to_filename(get_file)

    elif task == 'rm' or task == 'remove':
        zb_list = zb.list_objects()

        for rm_file in argv[2:]:
            if rm_file in zb_list:
                zb.delete_object( os.path.basename(rm_file) )
                print "deleted cloud object: %s / %s" % (container, rm_file)
            else:
                print "can't delete file not in cloud: %s / %s" % (container, rm_file)

if __name__ == "__main__":
    zbCloudFiles(sys.argv)


