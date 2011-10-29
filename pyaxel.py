#! /usr/bin/env python
import cPickle
import math
import os
import socket
import sys
import threading
import time
import urllib2

from optparse import OptionParser


"""
1) Get url, determine file size, dest file name.
2) Get each chunk size
"""

BLK_SIZE = 1024*1024
MAX_RETRIES = 10

std_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; '
        'en-US; rv:1.9.2) Gecko/20100115 Firefox/3.6',
    'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
    'Accept': 'text/xml,application/xml,application/xhtml+xml,'
        'text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5',
    'Accept-Language': 'en-us,en;q=0.5',
}

class DownloadState:
    def __init__(self, n_conn, url, filesize, filename):
        self.n_conn = n_conn
        self.filesize = filesize
        self.filename = filename
        self.url = url
        self.ongoing_jobs = {}
        self.elapsed_time = 0

    def write_state(self):
        pass

class JobTracker:
    def __init__(self, download_state):
        self.download_state = download_state
        filesize = download_state.filesize
        self.job_count = (filesize + BLK_SIZE - 1) / BLK_SIZE
        self.next_todo_job = 0
        self.lock = threading.Lock()

    def get_next_job(self):
        self.lock.acquire()
        r = self.next_todo_job
        if r >= self.job_count:
            r = None
        else:
            self.next_todo_job += 1
        self.lock.release()
        return r
        
class Worker(threading.Thread):
    def __init__(self, name, download_state, job_tracker):
        threading.Thread.__init__(self)
        self.name = str(name)
        self.download_state = download_state
        self.job_tracker = job_tracker
        self.offset = 0
        self.length = 0
        self._need_to_quit = False

    def __update_offset_and_length(self, jobNo):
        self.offset = jobNo * BLK_SIZE
        self.length = min(BLK_SIZE, self.download_state.filesize - self.offset)

    def __open_output_file(self):
        try:
            out_fd = os.open(self.download_state.filename + ".part",
                             os.O_CREAT | os.O_WRONLY)
            os.lseek(out_fd, self.offset, os.SEEK_SET)
            return out_fd
        except OSError, e:
            # TODO: stop all threads and exit
            print e.message

    def __get_remote_data(self):
        request = urllib2.Request(self.download_state.url, None, std_headers)
        request.add_header('Range', 'bytes=%d-%d' % (self.offset,
                                                     self.offset + self.length - 1))
        attempts = 0
        remote_file = None
        data_block = None
        while attempts < MAX_RETRIES:
            try:
                remote_file = urllib2.urlopen(request)
                data_block = remote_file.read()
                if len(data_block) == self.length:
                    #print "Got bytes %d to %d" % \
                    #    (self.offset, self.offset + self.length - 1)
                    break
            except urllib2.URLError, u:
                print "Failed to open url with error: %s. " + \
                    "Retrying connection %s" % (u.message, self.name)
            attempts += 1

        if len(data_block) != self.length:
            print "Could not get data from offset %d to %d" \
                % (self.offset, self.offset + self.length)
            # TODO raise exception or exit
        return data_block

    def run(self):
        jobNo = self.job_tracker.get_next_job()
        while jobNo != None:
            self.__update_offset_and_length(jobNo)
            if self._need_to_quit: return
            output_file = self.__open_output_file()
            if self._need_to_quit: return
            self.download_state.ongoing_jobs[jobNo] = None
            remote_block = self.__get_remote_data()
            os.write(output_file, remote_block)
            os.close(output_file)
            del self.download_state.ongoing_jobs[jobNo]
            self.download_state.write_state()
            if self._need_to_quit: return
            jobNo = self.job_tracker.get_next_job()

def get_file_size(url):
    request = urllib2.Request(url, None, std_headers)
    data = urllib2.urlopen(request)
    content_length = data.info()['Content-Length']
    # print content_length
    return int(content_length)

def download(url, options):
    fetch_threads = []
    try:
        if not options.output_file:
            output_file = url.rsplit("/", 1)[1]   # basename of the url
        else:
            output_file = options.output_file

        if not output_file:
            print "Invalid URL or could not determine destination file name."
            sys.exit(1)

        print "Destination = ", output_file

        filesize = get_file_size(url)
        print "Need to fetch %d bytes" % filesize

        download_state = DownloadState(options.num_connections, url,
                                       filesize, output_file)
        job_tracker = JobTracker(download_state)

        for i in range(options.num_connections):
            current_thread = Worker(i, download_state, job_tracker)
            fetch_threads.append(current_thread)
            current_thread.start()

        while threading.active_count() > 1:
            time.sleep(1)

        # # at this point we are sure dwnld completed and can delete the
        # # state file and move the dwnld to output file from .part file
        # os.remove(state_file)
        os.rename(output_file + ".part", output_file)

    except KeyboardInterrupt, k:
        print "KeyboardInterrupt! Quitting."
        for thread in fetch_threads:
            thread._need_to_quit = True

    except Exception, e:
        # TODO: handle other types of errors too.
        print e
        for thread in fetch_threads:
            thread._need_to_quit = True


def urllib_conf():
    # General configuration
    urllib2.install_opener(urllib2.build_opener(urllib2.ProxyHandler()))
    urllib2.install_opener(urllib2.build_opener(
            urllib2.HTTPCookieProcessor()))
    socket.setdefaulttimeout(120)         # 2 minutes

def main(options, args):
    try:
        urllib_conf()
        url = args[0]
        download(url, options)

    except KeyboardInterrupt, k:
        sys.exit(1)

    except Exception, e:
        # TODO: handle other types of errors too.
        print e

if __name__ == "__main__":

    parser = OptionParser(usage="Usage: %prog [options] url")
    parser.add_option("-s", "--max-speed", dest="max_speed",
                      type="int", 
                      help="Specifies maximum speed (Kbytes per second)."
                      " Useful if you don't want the program to suck up"
                      " all of your bandwidth",
                      metavar="SPEED")
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose", default=True,
                      help="don't print status messages to stdout")
    parser.add_option("-n", "--num-connections", dest="num_connections",
                      type="int", default=4,
                      help="You can specify the number of"
                      " connections here. Default is 4.",
                      metavar="NUM")
    parser.add_option("-o", "--output", dest="output_file",
                      help="By default, data does to a local file of "
                      "the same name. If this option is used, downloaded"
                      " data will go to this file.")
    
    (options, args) = parser.parse_args()

    print "Options: ", options
    print "args: ", args

    if len(args) != 1:
        parser.print_help()
        sys.exit(1)

    main(options, args)
