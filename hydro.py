#!/usr/bin/env python2.4

import os, sys, warnings
import cloudfiles
from config import config

def printHelp(scriptName=''):
    print "%s available tasks:" % scriptName
    print "\tcheck (conn/cont/all)\t# Check configuration, connection, container"
    print "\tlist  (<file(s)>)\t# list <file(s)> or all files in container"
    print "\tput    <file(s)> \t# load <file(s)> to container in cloud"
    print "\tget    <file(s)> \t# download <file(s)> from container in cloud"
    print "\trm     <file(s)> \t# remove <file(s)> from container in cloud"

def checkConfig(checkType=''):

    for checkValue in 'name key container'.split(' '):
        if checkValue not in config or config[checkValue] == '':
            raise ArgumentError, 'missing from config: %s' % conf

    print "Config\tOK!"
    if checkType == 'conn' or checkType == 'cont' or checkType == 'all':

        if getConnection(config['name'], config['key']):
            print "Connection\tOK!"
        else:
            print "There was a Connection\tPROBLEM!"
            return False

    if checkType == 'cont' or checkType == 'all':

        if getContainer(getConn(config['name'], config['key']), config['container']):
            print "Container %s\tOK!" % config['container']
        else:
            print "There was a PROBLEM w/ Container %s!" % config['container'] 
            return False

    return True

def getConnection(name, key):
    conn = cloudfiles.get_connection( name, key )
    if conn is None: raise ArgumentError, 'no connection returned'
    return conn

def getContainer(conn, container):
    cont = conn.get_container( container )
    if cont is None: raise ArgumentError, 'no container returned for:\t%s' % container
    return cont

def hydro(argv):

    if len(argv) == 1: task = 'help'
    else:              task = argv[1]

    if task == 'help':
        printHelp(argv[0])
        return

    elif task == 'check':
        if len(argv) == 2: checkConfig()
        else:              checkConfig(argv[2])
        return

    checkConfig()
    cont = getContainer( getConnection( config['name'], config['key'] ), config['container'] )

    cont_list = cont.list_objects()

    if task == 'list':

        if len(argv) == 2:
            print '\n'.join( cont.list_objects() )
        else:
            for list_file in argv[2:]:
                list_name = os.path.basename(list_file)
                if list_name in cont_list:
                    cf = cont.get_object(list_file)
                    if cf:  print "File exists in cloud: %s / %s\t%d bytes" % (container, list_name, cf.size)
                    else:   print "File %s does not exist in cloud: %s / %s\t" % (container, list_name)

    elif task == 'put':

        for put_file in argv[2:]:
            if os.path.isfile(put_file):

                put_name   = os.path.basename(put_file)
                local_size = os.path.getsize(put_file)

                if (put_name in cont_list) and cont.get_object(put_name).size == local_size:
                    print "File exists in cloud w/ same size: %s / %s\t%d bytes" % (container, put_name, local_size)
                else:
                    cf = cont.create_object( put_name )
                    cf.load_from_filename(put_file)
                    print 'put cloud object: %s / %s %d bytes' % (container, cf.name, cf.size)
            else:
                warnings.warn( 'put fail, not a file: %s' % put_file )

    elif task == 'get':

        for get_file in argv[2:]:
            if get_file in cont_list:
                cont.get_object(get_file).save_to_filename(get_file)
                print "saved from %s cloud to file:\t%s" % (container, rm_file)
            else:
                print "can't get file not in cloud:\t%s / %s" % (container, get_file)

    elif task == 'rm' or task == 'remove':

        for rm_file in argv[2:]:
            if rm_file in zb_list:
                cont.delete_object( os.path.basename(rm_file) )
                print "deleted cloud object: %s / %s" % (container, rm_file)
            else:
                print "can't delete file not in cloud: %s / %s" % (container, rm_file)

if __name__ == "__main__":
    hydro(sys.argv)
