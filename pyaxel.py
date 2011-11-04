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

BLK_SIZE = 256*1024         # file writing block size.
CHUNK_SIZE = 1024*1024      # max download per GET request
MAX_RETRIES = 10

std_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; '
        'en-US; rv:1.9.2) Gecko/20100115 Firefox/3.6',
    'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
    'Accept': 'text/xml,application/xml,application/xhtml+xml,'
        'text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5',
    'Accept-Language': 'en-us,en;q=0.5',
}


class FileWriter:
    def __init__(self, filename):
        try:
            self.fd = os.open(filename,
                              os.O_CREAT | os.O_WRONLY)
        except OSError, e:
            # TODO: stop all threads and exit
            print e.message

    def seek(self, offset):
        try:
            os.lseek(self.fd, offset, os.SEEK_SET)
        except OSError, e:
            print "Seek error: %s" % (e.message)

    def write(self, block):
        try:
            os.write(self.fd, block)
        except OSError, e:
            print "Write error: %s" % (e.message)

    def close(self):
        try:
            os.close(self.fd)
        except OSError, e:
            print "File close error: %s" % (e.message)


class DownloadState:
    def __init__(self, n_conn, url, filesize, filename):
        self.n_conn = n_conn
        self.filesize = filesize
        self.filename = filename
        self.url = url
        self.continue_offset = 0
        self.todo_ranges = []
        self.inprogress_ranges = {}
        self.inprogress_lock = threading.Lock()
        self.elapsed_time = 0

    def update_inprogress_entry(self, thread_id, byte_offsets):
        self.inprogress_lock.acquire()
        self.inprogress_ranges[thread_id] = byte_offsets
        self.inprogress_lock.release()

    def delete_inprogress_entry(self, thread_id):
        self.inprogress_lock.acquire()
        del self.inprogress_ranges[thread_id]
        self.inprogress_lock.release()

    def write_state(self):
        pass


class JobTracker:
    def __init__(self, download_state):
        self.download_state = download_state
        filesize = download_state.filesize
        self.job_count = (filesize + BLK_SIZE - 1) / BLK_SIZE
        self.next_todo_job = 0
        self.lock = threading.Lock()

    def __get_next_chunk(self):
        start = self.download_state.continue_offset
        length = min(CHUNK_SIZE, self.download_state.filesize - start)
        print "le", length
        if length <= 0:
            return None
        end = start + length - 1
        self.download_state.continue_offset += length
        return (start, end)

    def get_next_job(self):
        self.lock.acquire()
        if self.download_state.todo_ranges:
            r = self.download_state.todo_ranges.pop()
        else:
            r = self.__get_next_chunk()
        self.lock.release()
        return r


class Worker(threading.Thread):
    def __init__(self, name, download_state, job_tracker):
        threading.Thread.__init__(self, name=name)
        self.download_state = download_state
        self.job_tracker = job_tracker
        self.start_offset = 0
        self.end_offset = 0
        self.fwriter = FileWriter(download_state.filename + ".part")
        self._need_to_quit = False
        self.isFailing = False

    def __update_offsets(self, job):
        self.start_offset, self.end_offset = job

    def __download_range(self):
        attempts = 0
        remote_file = None
        data_block = None
        while attempts < MAX_RETRIES:
            try:
                request = urllib2.Request(self.download_state.url, None, std_headers)
                request.add_header('Range', 'bytes=%d-%d' % (self.start_offset,
                                                             self.end_offset))
                remote_file = urllib2.urlopen(request)
                self.fwriter.seek(self.start_offset)
                bytes_read = 0
                while True:
                    data_block = remote_file.read(BLK_SIZE)
                    if not data_block:
                        self.download_state.delete_inprogress_entry(self.name)
                        return
                    bytes_read += len(data_block)
                    self.fwriter.write(data_block)
                    self.download_state.update_inprogress_entry(self.name,
                        (self.start_offset + bytes_read, self.end_offset))
                    if self._need_to_quit:
                        return
            except urllib2.URLError:
                attempts += 1
            except IOError:
                self.start_offset += bytes_read
                attempts += 1
        if attempts >= MAX_RETRIES:
            self.isFailing = True

    def run(self):
        job = self.job_tracker.get_next_job()
        while job != None:
            print job
            self.__update_offsets(job)
            self.__download_range()
            self.download_state.write_state()
            if self._need_to_quit or self.isFailing:
                break
            jobNo = self.job_tracker.get_next_job()
        self.fwriter.close()


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

        isFailing = False
        while threading.active_count() > 1:
            for thread in fetch_threads:
                if thread.isFailing == True:
                    isFailing = True
                    break
            if isFailing:
                for thread in fetch_threads:
                    thread._need_to_quit = True
            time.sleep(1)
        
        # # at this point we are sure dwnld completed and can delete the
        # # state file and move the dwnld to output file from .part file
        # os.remove(state_file)
        if isFailing:
            print "File downloading failed!"


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
