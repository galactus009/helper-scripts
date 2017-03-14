#!/usr/bin/env python

#
# Utility to Reap Dead ips from RAFT Layer in Consul
#


import time
from subprocess import Popen, PIPE
import re
import logging
from StringIO import StringIO
import sys
logging.basicConfig(level=logging.DEBUG)
logger=logging.getLogger(__name__)

def _usage():
    logger.debug("This Utility Works from Consul 0.7.x")
    sys.exit(-1)

def detect_consul_version():
    logger.debug("Detecting Consul Version")
    cmd = "/usr/local/bin/consul version"
    p = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
    sout,serr = p.communicate()
    lines = [x for x in StringIO(sout)]
    (stub,vstring) = lines[0].split(" ")
    version=float(vstring.replace('v','')[0:3])
    if version >= float("0.7") :
	   logger.debug("Detected Consul With Raft Support: {0}".format(vstring))
    else:
       _usage()
    return

def reap(a):
    logger.debug("Reaping stale ips in RAFT Layer {0}".format(a))
    cmd="/usr/local/bin/consul operator raft -remove-peer -address={0}".format(a)
    logging.debug("Running: {0}".format(cmd))
    p=Popen(cmd.split(),stdout=PIPE,stderr=PIPE)
    sout,serr=p.communicate()
    logging.debug(sout)

def check_stale_ips_in_raft():
    try:
     logging.debug("Looking for Dead Peers in Raft Layer")   
     cmd="/usr/local/bin/consul operator raft -list-peers"
     p=Popen(cmd.split(),stdout=PIPE,stderr=PIPE)
     ostream,estream=p.communicate()
     ctr=0
        for line in StringIO(ostream):
         #rbhaskar-mbp  192.168.1.118:8300  192.168.1.118:8300  leader  true
         line=line.strip()
         if re.search("unknown",line,re.IGNORECASE):
            ctr=ctr+1
            r=re.compile(r'(?P<name>\S+)\s+(?P<ip>\S+)\s+(?P<addr>\S+)\s+(?P<state>\S+)\s+(?P<voter>\S+)')
            m=r.search(line)
            if m is not None:
               ip=m.group('ip') 
               reap(ip)
     if ctr > 0:
	logging.debug("Dead Peers: {0}".format(ctr-1))
     else:
	logging.debug("No Dead Peers to Reap")	

    except Exception as err:
      logger.debug("Error Occured",err)
      pass


if __name__ == "__main__" :
    detect_consul_version()
    while True:
      check_stale_ips_in_raft()
      time.sleep(30)
