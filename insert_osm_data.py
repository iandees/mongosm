import sys
import os
import shelve
import time
import pymongo
from datetime import datetime
from xml.sax import make_parser
from xml.sax.handler import ContentHandler
from pymongo import Connection

class OsmHandler(ContentHandler):
    def __init__(self, client, shelf):
        self.record = {}
        self.client = client
        self.client.osm.nodes.ensure_index([('loc', pymongo.GEO2D), ('id', pymongo.ASCENDING)])
        self.shelf = shelf

    def fillDefault(self, attrs):
        self.record['id'] = long(attrs['id'])
        self.record['timestamp'] = self.isoToTimestamp(attrs['timestamp'])
        self.record['tags'] = {}
        if attrs.has_key('user'):
            self.record['user'] = attrs['user']
        if attrs.has_key('uid'):
            self.record['uid'] = long(attrs['uid'])

    def isoToTimestamp(self, isotime):
        t = datetime.strptime(isotime, "%Y-%m-%dT%H:%M:%SZ")
        return time.mktime(t.timetuple())

    def startElement(self, name, attrs):
        if name == 'node':
            self.fillDefault(attrs)

            self.record['loc'] = {'lat': float(attrs['lat']), 'lon': float(attrs['lon'])}
        elif name == 'changeset':
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

            nodes2ways = self.client.osm.nodes2ways.find_one({ '_id' : ref })
            if not nodes2ways:
                nodes2ways = { '_id' : ref, 'ways' : [] }
            nodes2ways['ways'].append(self.record['id'])
            self.client.osm.nodes2ways.save(nodes2ways)
        elif name == 'member':
            member = {'type': attrs['type'],
                      'ref':  attrs['ref'],
                      'role': attrs['role']}
            self.record['members'].append(member)
        
    def endElement(self, name):
        if name == 'node':
            #if str("node_%s" % (self.record['id'])) in self.shelf:
            #    print "Skipping node %s." % (self.record['id'])
            #else:
            self.client.osm.nodes.save(self.record)
            self.record = {}
                #self.shelf[str("node_%s" % self.record['id'])] = self.record
            #print "Sent node %s." % (self.record)
        elif name == 'way':
            #if str("way_%s" % (self.record['id'])) in self.shelf:
            #    print "Skipping way %s." % (self.record['id'])
            #else:
                self.client.osm.ways.save(self.record)
                self.record = {}
                #self.shelf[str("way_%s" % self.record['id'])] = self.record
                #print "Sent way %s." % (self.record)
        elif name == 'relation':
            self.client.osm.relations.save(self.record)
            self.record = {}

if __name__ == "__main__":

    filename = sys.argv[1]

    if not os.path.exists(filename):
        print "Path %s doesn't exist." % (filename)
        sys.exit(-1)

    #shelf = shelve.open("%s.db" % (filename))
    shelf = ""

    client = Connection()
    parser = make_parser()
    handler = OsmHandler(client, shelf)
    parser.setContentHandler(handler)
    parser.parse(open(filename))
    client.disconnect()
