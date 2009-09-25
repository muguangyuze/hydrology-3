#!/usr/bin/env python2.4

import os, sys, warnings

sys.path.append(os.path.join('.', 'lib'))
sys.path.append('.')
from hydrology import Hydro

def printHelp(scriptName=''):
    print "%s available tasks:" % scriptName
    print "\tcheck (conn/cont/all)\t# Check configuration, connection, container"
    print "\tlist  (<file(s)>)\t# list <file(s)> or all files in container"
    print "\tput    <file(s)> \t# load <file(s)> to container in cloud"
    print "\tget    <file(s)> \t# download <file(s)> from container in cloud"
    print "\trm     <file(s)> \t# remove <file(s)> from container in cloud"
    print "\tmkcontainer <name>\t# create container in cloud"
    print "\trmcontainer <name>\t# remove container from cloud"

def hydro(argv):

    if len(argv) == 1: task = 'help'
    else:              task = argv[1]

    if task == 'help':
        printHelp(argv[0])
        return

    hydro = Hydro()
    hydro.loadConfig()

    if task == 'check':
        check = 'all'  
        if len(argv) == 3: check = argv[2]
        hydro.checkConfig(check)
        return

    hydro.checkConfig()

    if task == 'list':

        if len(argv) == 2: hydro.listFiles()
        else:              hydro.listFiles(argv[2:])

    elif task == 'put':
        for put_file in argv[2:]:
            hydro.putFile(put_file)

    elif task == 'get':
        for get_file in argv[2:]:
            hydro.getFile(get_file)

    elif task == 'rm' or task == 'remove':
        for rm_file in argv[2:]:
            hydro.rmFile(rm_file)
            print "deleted cloud object: %s / %s" % (hydro.config['container'], rm_file)

    elif task == 'mk' or task == 'mkcontainer':
        if len(argv) < 3: raise 'please specify container name'
        container_name = argv[2]        
        hydro.mkContainer(container_name)

    elif task == 'del' or task == 'deletecontainer':
        if len(argv) < 3: raise 'please specify container name'
        container_name = argv[2]        
        hydro.rmContainer(container_name)

if __name__ == "__main__":
    hydro(sys.argv)
