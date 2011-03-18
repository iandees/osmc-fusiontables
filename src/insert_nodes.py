from authorization.clientlogin import ClientLogin
from sql.sqlbuilder import SQL
import ftclient
from fileimport.fileimporter import CSVImporter
from xml.sax import make_parser
from xml.sax.handler import ContentHandler
import urllib2
import StringIO
import gzip
import time

class NodeInsertingHandler(ContentHandler):
  def __init__(self, ftclient, tableid):
    self.ft_client = ftclient
    self.tableid = tableid
    self.queries = []
    self.current_row = 0
    self.max_per_batch = 500

  def startElement(self, name, attrs):
    if name == 'node':
      timestamp = attrs['timestamp']
      lat = attrs['lat']
      lon = attrs['lon']
      if includeNode(float(lat), float(lon)):
        query = SQL().insert(self.tableid, 
        {
         'Timestamp': timestamp,
         'Location': '%s, %s' % (lat, lon)
        })
        self.queries.append(query)
        self.current_row += 1
        if self.current_row == self.max_per_batch:
          full_query = ';'.join(self.queries)
          makeQueryWithRetry(self.ft_client, full_query)
          print len(self.queries)
          self.current_row = 0
          self.queries = []
  
  def endDocument(self):
    if len(self.queries) > 0:
      full_query = ';'.join(self.queries)
      makeQueryWithRetry(self.ft_client, full_query)
      print len(self.queries)
      self.current_row = 0
      self.queries = []

#        top, left        bottom, right
bbox = ((42.13, 133.45), (32.81, 142.57))

def includeNode(lat, lon):
  return    ((lat <= bbox[0][0] and lat >= bbox[1][0])
         and (lon <= bbox[1][1] and lon >= bbox[0][1]))

def makeQueryWithRetry(ft_client, full_query):
  keepTrying = True
  sleeptime = 1
  while keepTrying:
    try:
      return ft_client.query(full_query)
    except urllib2.HTTPError, e:
      print e
      if e.code == 500:
        print "Will try again in %d seconds." % (sleeptime)
        time.sleep(sleeptime)
        sleeptime = sleeptime * 2
      else:
        keepTrying = False

def formatSqn(n):
  sqn = n.rjust(9, '0')
  return "%s/%s/%s" % (sqn[0:3], sqn[3:6], sqn[6:9])

if __name__ == "__main__":
  import sys, getpass
  username = sys.argv[1]
  tableid = sys.argv[2]
  password = getpass.getpass("Enter your password: ")
  
  token = ClientLogin().authorize(username, password)
  ft_client = ftclient.ClientLoginFTClient(token)
 
  keepGoing = True
  while keepGoing:
    # Check the state.txt
    statefile = open('state.txt', 'r')
    state = {}
    for line in statefile:
      if line.startswith('#'):
        continue
      else:
        (k, v) = line.strip().split('=')
        state[k] = v
    statefile.close()
    print state['timestamp']

    # Parse the sequence number
    sqnUrlPart = formatSqn(state['sequenceNumber'])

    # Fetch the osc.gz
    baseUrl = "http://planet.openstreetmap.org/hour-replicate"
    gzUrl = "%s/%s.osc.gz" % (baseUrl, sqnUrlPart)
    print gzUrl
    compresseddata = urllib2.urlopen(gzUrl).read()

    # Parse the osc XML and insert nodes
    parser = make_parser()
    handler = NodeInsertingHandler(ft_client, tableid)
    parser.setContentHandler(handler)
    compressedstream = StringIO.StringIO(compresseddata)
    parser.parse(gzip.GzipFile(fileobj=compressedstream))

    # When finished, grab the new state file and replace our current state.txt with it
    nextSqnUrlPart = formatSqn(str(int(state['sequenceNumber']) + 1))
    stateUrl = "%s/%s.state.txt" % (baseUrl, nextSqnUrlPart)
    u = urllib2.urlopen(stateUrl)
    f = open('state.txt', 'w')
    f.write(u.read())
    f.close()

