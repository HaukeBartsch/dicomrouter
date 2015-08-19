#!/usr/bin/python2.7
"""
Create a daemon process that listens to send messages and reads a DICOM file,
The information about the file is used to move the file to a propper destination.
Information required from the process writing into the pipe:
  <aetitle caller>, <aetitle called>, <caller IP>, <path to dicom file>, <dicom file name>

Usage:
  /usr/bin/python2.7 processSingleFile.py start

Hauke Bartsch, 2015
"""

import sys, os, time, atexit, stat, shutil, pickle
import logging, datetime, smtplib, threading
import dicom, json, re
from signal import SIGTERM
from dicom.filereader import InvalidDicomError
from email.mime.text import MIMEText
from threading import Thread

class Daemon:
        """
        A generic daemon class.
        
        Usage: subclass the Daemon class and override the run() method
        """
        def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
                    self.stdin    = stdin
                    self.stdout   = stdout
                    self.stderr   = stderr
                    self.pidfile  = pidfile
                    self.pipename = '/tmp/.processSingleFilePipe'
                    
        def daemonize(self):
                    """
                    do the UNIX double-fork magic, see Stevens' "Advanced
                    Programming in the UNIX Environment" for details (ISBN 0201563177)
                    http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
                    """
                    try:
                                pid = os.fork()
                                if pid > 0:
                                            # exit first parent
                                            sys.exit(0)
                    except OSError, e:
                                sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
                                sys.exit(1)

                    # decouple from parent environment
                    os.chdir("/")
                    os.setsid()
                    os.umask(0)
                
                    # do second fork
                    try:
                                pid = os.fork()
                                if pid > 0:
                                            # exit from second parent
                                            sys.exit(0)
                    except OSError, e:
                                sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
                                sys.exit(1)

                    # redirect standard file descriptors
                    sys.stdout.flush()
                    sys.stderr.flush()
		    # If I keep the redirects below I don't see error messages...
                    #si = file(self.stdin, 'r')
                    #so = file(self.stdout, 'a+')
                    #se = file(self.stderr, 'a+', 0)
                    #os.dup2(si.fileno(), sys.stdin.fileno())
                    #os.dup2(so.fileno(), sys.stdout.fileno())
                    #os.dup2(se.fileno(), sys.stderr.fileno())

                    # write pidfile
                    atexit.register(self.delpid)
                    pid = str(os.getpid())
                    file(self.pidfile,'w+').write("%s\n" % pid)
                    
        def delpid(self):
                    os.remove(self.pidfile)

        def delpipe(self):
                    os.remove(self.pipename)
                            
        def start(self):
                    """
                    Start the daemon
                    """
                    # Check for a pidfile to see if the daemon already runs
                    try:
                                pf = file(self.pidfile,'r')
                                pid = int(pf.read().strip())
                                pf.close()
                    except IOError:
                                pid = None
                                
                    if pid:
                            message = "pidfile %s already exist. Daemon already running?\n"
                            sys.stderr.write(message % self.pidfile)
                            sys.exit(1)
                            
                    # Start the daemon
                    print(' start the daemon')
                    self.daemonize()
                    print ' done'
                    self.run()

        def send(self,arg):
                    """
                    Send a message to the daemon via pipe. Just use bash to write to the pipe, that is faster than starting up python
                    """
                    # open a named pipe and write to it
                    if stat.S_ISFIFO(os.stat(self.pipename).st_mode):
                            try:
                                    wd = open(self.pipename, 'w')
                                    wd.write(arg + "\n")
                                    wd.flush()
                                    wd.close()
                            except IOError:
                                    print 'Error: could not open the pipe %s' % self.pipename
                    else:
                            sys.stderr.write(self.pipename)
                            sys.stderr.write("Error: the connection to the daemon does not exist\n")
                            sys.exit(1)

        def stop(self):
                    """
                    Stop the daemon
                    """
                    # Get the pid from the pidfile
                    try:
                            pf = file(self.pidfile,'r')
                            pid = int(pf.read().strip())
                            pf.close()
                    except IOError:
                            pid = None
                            
                    if not pid:
                            message = "pidfile %s does not exist. Daemon not running?\n"
                            sys.stderr.write(message % self.pidfile)
                            return # not an error in a restart
                                
                    # Try killing the daemon process
                    try:
                                while 1:
                                            os.kill(pid, SIGTERM)
                                            time.sleep(0.1)
                    except OSError, err:
                                err = str(err)
                                if err.find("No such process") > 0:
                                            if os.path.exists(self.pidfile):
                                                        os.remove(self.pidfile)
                                                        os.remove(self.pipename)
                                else:
                                            print str(err)
                                            sys.exit(1)
                                                        
        def restart(self):
                    """
                    Restart the daemon
                    """
                    self.stop()
                    self.start()
                    
        def run(self):
                    """
                    You should override this method when you subclass Daemon. It will be called after the process has been
                    daemonized by start() or restart().
                    """


class ProcessSingleFile(Daemon):
	"""
	Here we overwrite the daemons default method run
	"""
        def init(self):
                    self.routes = 0
                    self.routesFile = os.path.dirname(os.path.abspath(__file__)) + '/routes.json'
                    if os.path.exists(self.routesFile):
                            with open(self.routesFile,'r') as f:
                                    self.routes = json.load(f)
                    else:
                            print "Warning: no %s file could be found" % self.routesFile

        def run(self):
                try:
                        os.mkfifo(self.pipename)
                        atexit.register(self.delpipe)
                except OSError:
                        print 'OSERROR on creating the named pipe %s' % self.pipename
                        pass
                try:
                        rp = open(self.pipename, 'r')
                except OSError:
                        print 'Error: could not open named pipe for reading commands'
                        sys.exit(1)

		# start the email service that checks for study finish and sends out emails
		self.timer = EmailService()
		self.timer.start()
                while True:
                        response = rp.readline()[:-1]
                        if not response:
                                time.sleep(0.1)
                                continue
                        else:
                                # print 'Process: %s' % response
				[aet, aec, ip, p, f] = [x.strip() for x in response.split(',')]
				f = os.path.join(p,f)
				aec = aec.strip('"')
				# check if we have a route for this aec
				aetitles = []
				for entry in self.routes:
					aetitles.append(entry['AETITLE'])
				if aec not in aetitles:
					print "Error: the called AETitle \"%s\" could not be found in the list of known routes %s" % (aec, ', '.join(aetitles))
					continue
				# where to we need to save the data?
				moveFileTo = ""
				alertThem = []
				for entry in self.routes:
					if entry['AETITLE'] == aec:
						#print "we need to save this file %s in: %s" % (f, entry['PATH'])
						moveFileTo = entry['PATH']
						alertThem = entry['EMAIL']
				if moveFileTo == "":
					print "Error: The route %s does not have a path entry" % entry['AETITLE']
					logging.warning("Error: The route %s does not have a path entry, no processing will be done", entry['AETITLE'])
					continue
				if len(alertThem) == 0:
					logging.warning("Warning: The route %s does not have an email entry, no email will be send", entry['AETITLE'])
					print "Warning: The route %s does not have an email entry, no email will be send" % entry['AETITLE']
				
                                try:
                                        dataset = dicom.read_file(f)
                                except IOError:
                                        # print("Could not find file:", f)
					logging.info("Could not find file %s", f)
                                        continue
                                except InvalidDicomError:
                                        # print("Not a DICOM file: ", f)
					logging.info("Not a DICOM file %s", f)
                                        continue

				# create the file name the file will be saved under
				try:
					PatientID=dataset.PatientID.strip().translate(None, '-^_')
				except:
					PatientID='unknown'
				try:
					PatientName=dataset.PatientName.strip().translate(None, '-^_')
				except:
					PatientName='unknown'
				try:
					StudyDate=dataset.StudyDate.strip().translate(None, '-^_')
				except:
					StudyDate='unknown'
				try:
					StudyTime=dataset.StudyTime.strip().translate(None, '-^_')
				except:
					StudyTime='unknown'
				try:
					StudyDescription=dataset.StudyDescription.strip()
				except:
					StudyDescription='unknown'

				# we need to do some special things with patient ID's based on the project
				if aec == "PLING":
					PatientID = PatientID[0:5]
				if aec == "ANDVLBW":
					PatientID=PatientName
					
                                outdir = moveFileTo
                                if not os.path.exists(outdir):
					print "Error: the output directory %s does not exist for %s" % (outdir, aec)
					logging.info("Error: the output directory %s does not exist for %s", outdir, aec)
					continue
                                infile = os.path.basename(f)
				# create the path to store the input file under ( this time sorting series into different directories )
				# in the olden times we did not use sub-directories, we need to remain backward compatibility, therefore
				# test first if this is old style and that file exists already
				oldfn = os.path.join(outdir, PatientID + "_" + StudyDate + "_" + StudyTime, infile)
                                fn    = os.path.join(outdir, PatientID + "_" + StudyDate + "_" + StudyTime, dataset.SeriesInstanceUID)
				if os.path.exists(oldfn):
					logging.info("old style path detected, file %s already exists nothing is saved" % oldfn)
					# print "old style path detected, file %s already exists nothing is saved" % oldfn
					# but we should remove the file from our temporary input path
					try:
						os.remove(f)
					except:
						pass
					self.timer.addBadEvent(dataset.StudyInstanceUID, aec, fn, StudyDescription, alertThem)
					continue
				
                                if not os.path.exists(fn):
					logging.info("Create directory %s" % fn)
					# print "Create directory: %s" % fn
                                        os.makedirs(fn)
                                fn2 = os.path.join(fn, dataset.SOPInstanceUID)
                                if not os.path.exists(fn2):
					# move that file to the new location
					logging.info("Try to move file %s to new location %s", f, fn2)
					try:
						shutil.move(f, fn2)
					except:
						logging.info("move failed")
						self.timer.addBadEvent(dataset.StudyInstanceUID, aec, fn, alertThem)
						continue
					# we need to add the study instance uid to be able to send out events by email
					self.timer.addGoodEvent(dataset.StudyInstanceUID, aec, fn, StudyDescription, alertThem)
                                else:
					#print "file already at destination: %s" % fn2
					logging.info("File already at destination: %s" % fn2)
					self.timer.addBadEvent(dataset.StudyInstanceUID, aec, fn, StudyDescription, alertThem)
					continue # don't do anything because the file exists already
                rp.close()

class EmailService(Thread):
	def run(self):
		self.events = {}
		while 1:
			time.sleep(60)
			if len(self.events) > 0:
				if len(self.events) == 1:
					logging.info("we are waking up for a check on %s event" % len(self.events))
				else:
					logging.info("we are waking up for a check on %s events" % len(self.events))
			# do we have any old enough events? Send an email if we do...
			listToRemove = []
			for key, entry in self.events.iteritems():
				c = datetime.datetime.now() - pickle.loads(entry[0])
				if c.total_seconds() > 16:
					# we detected an end of study signal, lets send an email and remove from the list of actives
					listToRemove.append(key)
					logging.info("Send out emails to %s because the time since the last image was received is more than 16 seconds" % ', '.join(entry[6]))
					text="%s: MMILREC TRANSFER SUCCESS for %s files (total number of files received: %s)\nstudy description: \"%s\"\nfile destination: \"%s\"" % (entry[3], entry[1], entry[1]+entry[2], entry[5], entry[4])
					msg = MIMEText( text )
					msg['Subject'] = "%s TRANSFER SUCCESS" % (entry[3])
					msg['From'] = "mmilrec@ip97.ucsd.edu"
					for name in entry[6]:
						msg['To'] = name
						s = smtplib.SMTP('localhost')
						s.sendmail("mmilrec@ucsd.edu", [name], msg.as_string())
						s.quit()
										 
			if len(listToRemove) > 0:
				self.events = {key: value for key, value in self.events.iteritems() if key not in listToRemove}
			
	def addGoodEvent(self, StudyInstanceUID, aec, fn, StudyDescription, alertThem):
		for_storage = pickle.dumps(datetime.datetime.now())
		if StudyInstanceUID in self.events:
			self.events[StudyInstanceUID] = [ for_storage, self.events[StudyInstanceUID][1] + 1, self.events[StudyInstanceUID][2], aec, fn, StudyDescription, alertThem]
		else:
			self.events[StudyInstanceUID] = [ for_storage, 1, 0, aec, fn, StudyDescription, alertThem]
	def addBadEvent(self, StudyInstanceUID, aec, fn, StudyDescription, alertThem):
		for_storage = pickle.dumps(datetime.datetime.now())
		if StudyInstanceUID in self.events:
			self.events[StudyInstanceUID] = [ for_storage, self.events[StudyInstanceUID][1], self.events[StudyInstanceUID][2] + 1, aec, fn, StudyDescription, alertThem]
		else:
			self.events[StudyInstanceUID] = [ for_storage, 0, 1, aec, fn, StudyDescription, alertThem]


# There are two files that make this thing work, one is the .pid file for the daemon
# the second is the named pipe in /tmp/.processSingleFile
if __name__ == "__main__":
	logging.basicConfig(filename='/home/mmilrec/.storescpd_scratch_space/processSingleFile.log',format='%(levelname)s:%(asctime)s: %(message)s',level=logging.DEBUG)
        daemon = ProcessSingleFile('/home/mmilrec/bin/processSingleFile.pid')
        daemon.init()
        if len(sys.argv) == 2:
                if 'start' == sys.argv[1]:
			logging.info("start daemon")
                        daemon.start()
                elif 'stop' == sys.argv[1]:
			logging.info("stop daemon")
                        daemon.stop()
                elif 'restart' == sys.argv[1]:
			logging.info("restart daemon")
                        daemon.restart()
                else:
                        print "Unknown command"
                        sys.exit(2)
                sys.exit(0)
        elif len(sys.argv) == 3:
                if 'send' == sys.argv[1]:
                        daemon.send(sys.argv[2])
                sys.exit(0)
        else:
		print ""
                print "Process a single DICOM file fast using a daemon process that routes/moves files to destinations."
		print "Because this runs as a daemon no libraries need to be loaded which lets us to get away with slow"
		print "python and pydicom (still much better than matlab)."
		print ""
                print "Use 'start' to start the daemon in the background. Don't send file names for processing using 'send',"
		print "that would be too slow. Instead write the information about the DICOM file using bash:"
		print "  pipe=/tmp/.processSingleFilePipe
		print "  /usr/pubsw/packages/dcmtk/3.6.0/bin/storescp --fork \\"
		print "                                               --write-xfer-little \\"
		print "                                               --exec-on-reception "echo '#a,#c,#r,#p,#f' >$pipe" \\"
		print "                                               --sort-on-study-uid scp \\"
		print "                                               --output-directory "/tmp/archive" \\"
		print "                                               11113"
                print "Usage:"
		print "  python2.7 %s start|stop|restart|send" % sys.argv[0]
                print ""
                sys.exit(2)
