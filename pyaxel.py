import sys, os, urllib2, socket, time, threading, math
from optparse import OptionParser

std_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2) Gecko/20100115 Firefox/3.6',
    'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
    'Accept': 'text/xml,application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5',
    'Accept-Language': 'en-us,en;q=0.5',
}


def report_bytes(bytes):
    if bytes == 0: return "0b"
    k = math.log(bytes,1024)
    return "%.2f%s" % (bytes / (1024.0**int(k)), "bKMGTPEY"[int(k)])

def report_time(time_in_secs):
    mult_list = [60, 60*60, 60*60*24]
    unit_list = ["second(s)", "minute(s)", "hour(s)", "day(s)"]
    for i in range(len(mult_list)):
        if time_in_secs < mult_list[i]:
            return "%d %s" % (int(time_in_secs / (mult_list[i-1] if i>0 else 1)), unit_list[i])
    return "%d %s" % ( (int(time_in_secs / mult_list[2])), unit_list[3])

def get_file_size(url):
    request = urllib2.Request(url, None, std_headers)
    data = urllib2.urlopen(request)
    content_length = data.info()['Content-Length']
    # print content_length
    return int(content_length)

def update_progress(progress):
    ret_str = ""
    for i in range(len(progress)):
        dots[i] = "".join(['=' for j in range((progress[i][0]*uwidth)/len_list[i])])
        if ret_str == "":
            ret_str += dots[i]
        else:
            ret_str += "|" + dots[i]
        if len(dots[i]) < uwidth:
            ret_str += '>'
            ret_str += "".join([' ' for i in range(uwidth-len(dots[i])-1)])
    return ret_str

def get_progress_report(progress, orig_filesize = 0):
    ret_str = "["
    dl_len, max_elapsed_time = 0, 0.0

    ret_str += update_progress(progress)

    for rec in progress:
        #ret_str += " " + report_bytes(rec[0])
        dl_len += rec[0]
        max_elapsed_time = max(rec[1], max_elapsed_time)
    ret_str += "] Speed = "
    if max_elapsed_time == 0:
        avg_speed = 0
    else:
        avg_speed = dl_len / max_elapsed_time
    ret_str += "%s/s." % report_bytes(avg_speed)

    #Estimate remaining time.
    if orig_filesize > 0:
        if avg_speed > 0:
            remaining_dl = orig_filesize - dl_len
            if remaining_dl > 0:
                ret_str += " %s left." % report_time( (orig_filesize - dl_len) / avg_speed)                
        else:
            ret_str += " Starting download..."
    
    ret_str = str(dl_len*100/orig_filesize) + "% " + ret_str 
 
    #print "\n",ret_str
    sys.stdout.flush()
    print "\r%s" % (ret_str),

        
class FetchData(threading.Thread):

    def __init__(self, name, url, out_file, start_offset, length, progress):
        threading.Thread.__init__(self)
        self.name = name
        self.url = url
        self.out_file = out_file
        self.start_offset = start_offset
        self.length = length
        self.progress = progress
        self._need_to_quit = False

    def run(self):
        # Ready the url object
        # print "Running thread with %d-%d" % (self.start_offset, self.length)
        request = urllib2.Request(self.url, None, std_headers)
        request.add_header('Range','bytes=%d-%d' % (self.start_offset, 
                                                    self.start_offset+self.length))
        data = urllib2.urlopen(request)

        # Open the output file
        out_fd = os.open(self.out_file, os.O_WRONLY)
        os.lseek(out_fd, self.start_offset, os.SEEK_SET)
        
        block_size = 1024
        while self.length > 0:
            if self._need_to_quit:
                return
            fetch_size = block_size if self.length >= block_size else self.length
            start_time = time.time()
            data_block = data.read(fetch_size)            
            end_time = time.time()
            elapsed = end_time - start_time            
            assert(len(data_block) == fetch_size)
            self.length -= fetch_size
            self.progress[int(self.name)][0] += fetch_size
            self.progress[int(self.name)][1] += elapsed
            os.write(out_fd, data_block)


if __name__ == "__main__":
    try:
        fetch_threads = []
        parser = OptionParser(usage="Usage: %prog [options] url")
        parser.add_option("-s", "--max-speed", dest="max_speed", 
                          help="Specifies maximum speed (bytes per second)."
                          " Useful if you don't want the program to suck up all"
                          " of your bandwidth",
                          metavar="SPEED")
        parser.add_option("-q", "--quiet",
                          action="store_false", dest="verbose", default=True,
                          help="don't print status messages to stdout")
        parser.add_option("-n", "--num-connections", dest="num_connections", type="int", default=4,
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

        if output_file == "":
            print "Invalid URL"
            sys.exit(1)

        print "Destination = ", output_file
        
        filesize = get_file_size(url)
        print "Need to fetch %s\n" % report_bytes(filesize)

        # get list of data segment sizes to be fetched by each thread.
        len_list = [ (filesize / options.num_connections) for i in range(options.num_connections) ]
        len_list[0] += filesize % options.num_connections

        # dots is a string representation of percentage downloaded
        dots = [" " for i in range(options.num_connections)]

        # getting terminal width from 'stty size' command
        term_rows, term_cols = map(int, os.popen('stty size', 'r').read().split())
        uwidth = (term_cols - 2*options.num_connections - 48)/options.num_connections

        #create output file
        out_fd = os.open(output_file, os.O_CREAT | os.O_WRONLY)

        progress = [ [0,0.0] for i in len_list ]
        start_offset = 0
        for i in range(len(len_list)):
            # each iteration should spawn a thread.
            # print start_offset, len_list[i]
            current_thread = FetchData(i, url, output_file, start_offset, len_list[i], progress)
            fetch_threads.append(current_thread)
            current_thread.start()
            start_offset += len_list[i]

        while threading.active_count() > 1:
            #print "\n",progress               
            get_progress_report(progress, filesize)
            time.sleep(1)

        # Blank spaces trail below to erase previous output. TODO: Need to
        # do this better.
        get_progress_report(progress, filesize)

    except KeyboardInterrupt, k:
        for thread in fetch_threads:
            thread._need_to_quit = True

    except Exception, e:
        # TODO: handle other types of errors too.
        print e
        for thread in fetch_threads:
            thread._need_to_quit = True
