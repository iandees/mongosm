"""Applies an OSMChange file to the database"""

import sys
import os
import time
import urllib2
import StringIO
import gzip
from datetime import datetime
from xml.sax import make_parser
from xml.sax.handler import ContentHandler
from pymongo import Connection

def convert_time(isotime):
    "Returns the time string as a time tuple"
    t = datetime.strptime(isotime, "%Y-%m-%dT%H:%M:%SZ")
    return time.mktime(t.timetuple())

class OsmChangeHandler(ContentHandler):
    """This ContentHandler works with the OSMChange XML file"""
    def __init__(self, client):
        """Initializes the OsmChange object"""
        self.action = ""
        self.record = {}
        self.nodes = []
        self.ways = []
        self.relations = []
        self.client = client

    def fillDefault(self, attrs):
        """Fills in default attributes for new records"""
        self.record['id'] = long(attrs['id'])
        self.record['timestamp'] = convert_time(attrs['timestamp'])
        self.record['tags'] = {}
        if attrs.has_key('user'):
            self.record['user'] = attrs['user']
        if attrs.has_key('uid'):
            self.record['uid'] = long(attrs['uid'])
        if attrs.has_key('version'):
            self.record['version'] = int(attrs['version'])
        if attrs.has_key('changeset'):
            self.record['changeset'] = long(attrs['changeset'])

    def startElement(self, name, attrs):
        """Parse the XML element at the start"""
        if name in ['create', 'modify', 'delete']:
            self.action = name
        elif name == 'node':
            self.record = {}
            self.fillDefault(attrs)
            self.record['loc'] = {'lat': float(attrs['lat']), 'lon': float(attrs['lon'])}
        elif name == 'tag':
            # MongoDB doesn't let us have dots in the key names.
            k = attrs['k']
            k = k.replace('.', ',,')
            self.record['tags'][k] = attrs['v']
        elif name == 'way':
            self.fillDefault(attrs)
            self.record['nodes'] = []
        elif name == 'relation':
            self.fillDefault(attrs)
            self.record['members'] = []
        elif name == 'nd':
            ref = long(attrs['ref'])
            self.record['nodes'].append(ref)

            nodes2ways = self.client.osm.nodes.find_one({ 'id' : ref })
            if nodes2ways:
                if 'ways' not in nodes2ways:
                    nodes2ways['ways'] = []
                nodes2ways['ways'].append(self.record['id'])
                self.client.osm.nodes.save(nodes2ways)
            else:
                print "Node %d ref'd by way %d not in file." % \
                    (ref, self.record['id'])
        elif name == 'member':
            ref = long(attrs['ref'])
            member = {'type': attrs['type'],
                      'ref':  ref,
                      'role': attrs['role']}
            self.record['members'].append(member)
            
            if attrs['type'] == 'way':
                ways2relations = self.client.osm.ways.find_one({ 'id' : ref})
                if ways2relations:
                    if 'relations' not in ways2relations:
                        ways2relations['relations'] = []
                    ways2relations['relations'].append(self.record['id'])
                    self.client.osm.ways.save(ways2relations)
            elif attrs['type'] == 'node':
                nodes2relations = self.client.osm.nodes.find_one({ 'id' : ref})
                if nodes2relations:
                    if 'relations' not in nodes2relations:
                        nodes2relations['relations'] = []
                    nodes2relations['relations'].append(self.record['id'])
                    self.client.osm.nodes.save(nodes2relations)
        elif name == 'node':
            self.record['loc'] = {'lat': float(attrs['lat']),
                                  'lon': float(attrs['lon'])}
            self.fillDefault(attrs)

        elif name == 'tag':
            # MongoDB doesn't let us have dots in the key names.
            k = attrs['k']
            k = k.replace('.', ',,')
            self.record['tags'][k] = attrs['v']
        elif name == 'way':
            self.fillDefault(attrs)
            self.record['nodes'] = []
        elif name == 'relation':
            self.fillDefault(attrs)
            self.record['members'] = []
        elif name == 'nd':
            ref = long(attrs['ref'])
            self.record['nodes'].append(ref)
        elif name == 'member':
            ref = long(attrs['ref'])
            member = {'type': attrs['type'],
                      'ref':  ref,
                      'role': attrs['role']}
            self.record['members'].append(member)
    
    def endElement(self, name):
        """Finish parsing osm objects or actions"""
        if name in ('node', 'way', 'relation'):
            self.type = name
            if self.action == 'delete':
                self.record['visible'] = False
            if self.type == 'way':
                nodes = self.client.osm.nodes.find({ 'id': { '$in': self.record['nodes'] } },
                                                   { 'loc': 1, '_id': 0 })
                self.record['loc'] = []
                for node in nodes:
                    self.record['loc'].append(node['loc'])
            getattr(self, name + 's').append(self.record)
        elif name in ('create', 'modify', 'delete'):
            if name == 'create': 
                for coll in ('nodes', 'ways', 'relations'):
                    if getattr(self, coll):
                        getattr(self.client.osm, coll).insert(getattr(self, coll))
            elif name == 'modify':
                for coll in ('nodes', 'ways', 'relations'):
                    if getattr(self, coll):
                        primitive_list = getattr(self, coll)
                        for prim in primitive_list:
                            getattr(self.client.osm, coll).update({'id': prim['id']},
                                                                  prim)
            elif name == 'delete':
                for coll in ('nodes', 'ways', 'relations'):
                    if getattr(self, coll):
                        primitive_list = getattr(self, coll)
                        for prim in primitive_list:
                            getattr(self.client.osm, coll).remove({'id': prim['id']})
            self.action = None
        
if __name__ == "__main__":
    client = Connection()
    parser = make_parser()

    keepGoing = True

    while keepGoing:
        # Read the state.txt
        sf = open('state.txt', 'r')

        state = {}
        for line in sf:
            if line[0] == '#':
                continue
            (k, v) = line.split('=')
            state[k] = v.strip().replace("\\:", ":")

        # Grab the sequence number and build a URL out of it
        sqnStr = state['sequenceNumber'].zfill(9)
        url = "http://planet.openstreetmap.org/minute-replicate/%s/%s/%s.osc.gz" % (sqnStr[0:3], sqnStr[3:6], sqnStr[6:9])

        print "Downloading change file (%s)." % (url)
        content = urllib2.urlopen(url)
        content = StringIO.StringIO(content.read())
        gzipper = gzip.GzipFile(fileobj=content)

        print "Parsing change file."
        handler = OsmChangeHandler(client)
        parser.setContentHandler(handler)
        parser.parse(gzipper)

        # Download the next state file
        nextSqn = int(state['sequenceNumber']) + 1
        sqnStr = str(nextSqn).zfill(9)
        url = "http://planet.openstreetmap.org/minute-replicate/%s/%s/%s.state.txt" % (sqnStr[0:3], sqnStr[3:6], sqnStr[6:9])
        try:
            u = urllib2.urlopen(url)
            statefile = open('state.txt', 'w')
            statefile.write(u.read())
            statefile.close()
        except Exception, e:
            keepGoing = False
            print e

    client.disconnect()
