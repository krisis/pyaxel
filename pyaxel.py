import sys, os, urllib2, socket, time, threading, math, cPickle
from optparse import OptionParser

std_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2) Gecko/20100115 Firefox/3.6',
    'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
    'Accept': 'text/xml,application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5',
    'Accept-Language': 'en-us,en;q=0.5',
}


class ConnectionState:
    def __init__(self, n_conn, filesize):
        self.n_conn = n_conn
        self.filesize = filesize
        self.progress = [[0,0.0] for i in range(n_conn)]
        self.chunks = [ (filesize / n_conn) for i in range(n_conn) ]
        self.chunks[0] += filesize % n_conn
        pass

    def get_progress(self):
        return self.progress

    def update_progress(self, fetch_size, elapsed_time, conn_id):
        self.progress[conn_id][0] += fetch_size
        self.progress[conn_id][1] += elapsed_time 

    def resume_state(self, in_fd):
        saved_obj = cPickle.load(in_fd) 
        self.n_conn = saved_obj.n_conn
        self.filesize = saved_obj.filesize
        self.progress = saved_obj.progress
        self.chunks = saved_obj.chunks

    def save_state(self, out_fd):
        #out_fd will be closed after save_state() is completed
        #to ensure that state is written onto the disk
        cPickle.dump(self, out_fd) 



class ProgressBar:
    def __init__(self, n_conn, conn_state):
        self.n_conn = n_conn
        self.dots = ["" for i in range(n_conn)]
        self.conn_state = conn_state
        pass
    
    def _get_term_width(self):
        term_rows, term_cols = map(int, os.popen('stty size', 'r').read().split())
        return term_cols

    def _get_download_rate(self, bytes):
        ret_str = report_bytes(bytes)
        ret_str += "/s."
        return len(ret_str), ret_str

    def _get_percentage_complete(self, dl_len):
        assert self.conn_state.filesize != 0
        ret_str = str(dl_len*100/self.conn_state.filesize) + "%."
        return len(ret_str), ret_str
    
    def _get_time_left(self, time_in_secs):
        ret_str = ""
        mult_list = [60, 60*60, 60*60*24]
        unit_list = ["second(s)", "minute(s)", "hour(s)", "day(s)"]
        for i in range(len(mult_list)):
            if time_in_secs < mult_list[i]:
                ret_str = "%d %s" % (int(time_in_secs / (mult_list[i-1] if i>0 else 1)), unit_list[i])
                break
        if len(ret_str) == 0: 
            ret_str = "%d %s." % ( (int(time_in_secs / mult_list[2])), unit_list[3])
        return len(ret_str), ret_str

    def _get_pbar(self, width):
        ret_str = "["
        for i in range(self.n_conn):
            self.dots[i] = "".join(['=' for j in range((self.conn_state.progress[i][0]*width)/self.conn_state.chunks[i])])
            if ret_str == "[":
                ret_str += self.dots[i]
            else:
                ret_str += "|" + self.dots[i]
            if len(self.dots[i]) < width:
                ret_str += '>'
                ret_str += "".join([' ' for i in range(width-len(self.dots[i])-1)])

        ret_str += "]"
        return len(ret_str), ret_str

    def display_progress(self):
        dl_len, max_elapsed_time = 0, 0.0
        for rec in self.conn_state.progress:
            dl_len += rec[0]
            max_elapsed_time = max(max_elapsed_time, rec[1])

        if max_elapsed_time == 0:
            avg_speed = 0
        else:
            avg_speed = dl_len / max_elapsed_time

        ldr, drate = self._get_download_rate(avg_speed)
        lpc, pcomp = self._get_percentage_complete(dl_len)
        ltl, tleft = self._get_time_left((self.conn_state.filesize - dl_len)/avg_speed if avg_speed > 0 else 0)
        # term_width - #(|) + #([) + #(]) + #(strings) + 6 (for spaces and periods)
        available_width = self._get_term_width() - (ldr + lpc + ltl) - self.n_conn - 1 - 6
        lpb, pbar = self._get_pbar(available_width/self.n_conn)
        sys.stdout.flush()
        print "\r%s %s %s %s" % (drate, pcomp, tleft, pbar),
    

def report_bytes(bytes):
    if bytes == 0: return "0b"
    k = math.log(bytes,1024)
    ret_str = "%.2f%s" % (bytes / (1024.0**int(k)), "bKMGTPEY"[int(k)])
    return ret_str

def get_file_size(url):
    request = urllib2.Request(url, None, std_headers)
    data = urllib2.urlopen(request)
    content_length = data.info()['Content-Length']
    # print content_length
    return int(content_length)

        
class FetchData(threading.Thread):

    def __init__(self, name, url, out_file, state_file, start_offset, conn_state):
        threading.Thread.__init__(self)
        self.name = name
        self.url = url
        self.out_file = out_file
        self.state_file = state_file
        self.start_offset = start_offset
        self.conn_state = conn_state
        self.length = conn_state.chunks[name] - conn_state.progress[name][0]
        self._need_to_quit = False

    def run(self):
        # Ready the url object
        # print "Running thread with %d-%d" % (self.start_offset, self.length)
        request = urllib2.Request(self.url, None, std_headers)
        request.add_header('Range','bytes=%d-%d' % (self.start_offset, 
                                                    self.start_offset+self.length))
        while 1:
            try:
                data = urllib2.urlopen(request)
            except urllib2.URLError, u:
                print "Connection", self.name, " did not start with", u
            else:
                break

        # Open the output file
        out_fd = os.open(self.out_file, os.O_WRONLY)
        os.lseek(out_fd, self.start_offset, os.SEEK_SET)
        
        block_size = 1024
        #indicates if connection timed out on a try
        retry = 0
        while self.length > 0:
            if self._need_to_quit:
                return
            fetch_size = block_size if self.length >= block_size else self.length
            if retry == 0:
                start_time = time.time()
            try:
                data_block = data.read(fetch_size)            
                if len(data_block) == 0: 
                    print "Connection %s: [TESTING]: 0 sized block fetched." % (self.name)
                if len(data_block) != fetch_size:
                    print "Connection %s: len(data_block) != fetch_size, but continuing anyway." % (self.name)
                    self.run()
                    return

            except socket.timeout, s:
                print "Connection", self.name, "timed out with", s
                retry = 1
                self.run()
                return

            else:
                retry = 0

            end_time = time.time()
            elapsed = end_time - start_time            
            #assert(len(data_block) == fetch_size)
            self.length -= fetch_size
            self.conn_state.update_progress(fetch_size, elapsed, int(self.name))
            os.write(out_fd, data_block)
            self.start_offset += len(data_block)
            #saving state after each blk of dwnld
            state_fd = file(self.state_file, "wb")
            self.conn_state.save_state(state_fd)
            state_fd.close()


def main():
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

        conn_state = ConnectionState(options.num_connections, filesize)
        pbar = ProgressBar(options.num_connections, conn_state)

        # Checking if we have a partial download available and resume
        state_file = output_file + ".st"
        try:
            os.stat(state_file)
        except OSError, o:
            #statefile is missing for all practical purposes
            pass
        else:
            state_fd = file(state_file, "r")
            conn_state.resume_state(state_fd)
            state_fd.close()

        #create output file
        out_fd = os.open(output_file, os.O_CREAT | os.O_WRONLY)

        start_offset = 0
        for i in range(options.num_connections):
            # each iteration should spawn a thread.
            # print start_offset, len_list[i]
            current_thread = FetchData(i, url, output_file, state_file, start_offset + conn_state.progress[i][0], conn_state)
            fetch_threads.append(current_thread)
            current_thread.start()
            start_offset += conn_state.chunks[i]

        while threading.active_count() > 1:
            #print "\n",progress               
            pbar.display_progress()
            time.sleep(1)

        # Blank spaces trail below to erase previous output. TODO: Need to
        # do this better.
        pbar.display_progress()

        #at this point we are sure dwnld completed and can delete the state file
        os.remove(state_file)


    except KeyboardInterrupt, k:
        for thread in fetch_threads:
            thread._need_to_quit = True

    except Exception, e:
        # TODO: handle other types of errors too.
        print e
        for thread in fetch_threads:
            thread._need_to_quit = True

if __name__ == "__main__":
    main()
