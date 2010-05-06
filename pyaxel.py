import sys, os, urllib2, socket
from optparse import OptionParser

def get_file_size(path):
    #TODO
    #print "File size is: ", os.path.getsize(path)
    #return os.path.getsize(path)
    pass

def get_data(outfile, url, st, data_len):
    os.lseek(outfile, st, os.SEEK_SET)

    # TODO: create url object to start fetching data, etc.


    # get data in 128 byte intervals
    pieceLen = 128
    while data_len > 0:
        fetchLen = pieceLen if data_len >= pieceLen else data_len
        # TODO: fix this line - os.write(outfile, os.read(infile,
        # fetchLen))
        data_len -= fetchLen    

def split_data(datasize, num_connections):
    psize = datasize / num_connections
    len_list = []
    for i in range(num_connections):
        len_list.append(psize)
    len_list[0] += datasize % num_connections
    # print len_list
    return len_list


if __name__ == "__main__":
    
    parser = OptionParser(usage="Usage: %prog [options] url")
    parser.add_option("-s", "--max-speed", dest="max_speed", 
                      help="Specifies maximum speed (bytes per second)."
                      " Useful if you don't want the program to suck up all"
                      " of your bandwidth",
                      metavar="SPEED")
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose", default=True,
                      help="don't print status messages to stdout")
    parser.add_option("-n", "--num-connections", dest="num_connections", default=4,
                      help="You can specify an alternative number of connections here.",
                      metavar="NUM")
    parser.add_option("-o", "--output", dest="output_file", 
                      help="By default, data does to a local file of the same name. If "
                      "this option is used, downloaded data will go to this file.")    
    
    (options, args) = parser.parse_args()
    
    print "Options: ", options
    print "args: ", args

    if len(args) != 1:
        parser.print_help()
        sys.exit(1)

    # General configuration
    urllib2.install_opener(urllib2.build_opener(urllib2.ProxyHandler()))
    urllib2.install_opener(urllib2.build_opener(urllib2.HTTPCookieProcessor()))    
    socket.setdefaulttimeout(120) # 2 minutes

    url = args[0]
    
    output_file = url.rsplit("/",1)[1] #basename of the url
    
    if options.output_file != None:
        output_file = options.output_file

    print "Destination = ", output_file
    
    filesize = get_file_size(url)
    print "Need to fetch %d bytes\n" % filesize

    len_list = split_data(filesize, options.num_connections)

    #create output file
    outfile = os.open(output_file, os.O_CREAT | os.O_WRONLY)

    start_offset = 0
    for i in len_list:
        # each iteration should spawn a thread.
        get_data(outfile, url, start_offset, i)
        start_offset += i

    # TODO: start a thread to monitor and output the download progress
    # and to respond cleanly to terminate requests (via Ctrl+C)
    
