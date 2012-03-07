"""This program parses an OSM XML file and inserts the data in a
MongoDB database"""

import sys
import os
import time
import pymongo
from datetime import datetime
from xml.sax import make_parser
from xml.sax.handler import ContentHandler
from pymongo import Connection

from globalmaptiles import GlobalMercator

class OsmHandler(ContentHandler):
    """Base class for parsing OSM XML data"""
    def __init__(self, client):
        self.proj = GlobalMercator()
        self.nodeRecords = []
        self.wayRecords = []
        self.relationRecords = []
        self.record = {}
        self.nodeLocations = {}
        self.client = client
        
        self.stats = {'nodes': 0, 'ways': 0, 'relations': 0}
        self.lastStatString = ""
        self.statsCount = 0

    def writeStatsToScreen(self):
        for char in self.lastStatString:
            sys.stdout.write('\b')
        self.lastStatString = "%d nodes, %d ways, %d relations" % (self.stats['nodes'],
                                                                     self.stats['ways'],
                                                                     self.stats['relations'])
        sys.stdout.write(self.lastStatString)

    def fillDefault(self, attrs):
        """Fill in default record values"""
        self.record['_id'] = int(attrs['id'])
        self.record['ts'] = self.isoToTimestamp(attrs['timestamp'])
        self.record['tg'] = [] 
        if attrs.has_key('user'):
            self.record['u'] = attrs['user']
        if attrs.has_key('uid'):
            self.record['uid'] = int(attrs['uid'])
        if attrs.has_key('version'):
            self.record['v'] = int(attrs['version'])
        if attrs.has_key('changeset'):
            self.record['c'] = int(attrs['changeset'])

    def isoToTimestamp(self, isotime):
        """Parse a date and return a time tuple"""
        t = datetime.strptime(isotime, "%Y-%m-%dT%H:%M:%SZ")
        return time.mktime(t.timetuple())

    def quadKey(self, lat, lon, zoom):
        (mx, my) = self.proj.LatLonToMeters(lat, lon)
        (tx, ty) = self.proj.MetersToTile(mx, my, zoom)
        return self.proj.QuadTree(tx, ty, zoom)

    def startElement(self, name, attrs):
        """Parse the XML element at the start"""
        if name == 'node':
            self.fillDefault(attrs)
            self.record['loc'] = {'lat': float(attrs['lat']),
                                  'lon': float(attrs['lon'])}
            self.record['qk'] = self.quadKey(float(attrs['lat']), float(attrs['lon']), 17)
            self.nodeLocations[self.record['_id']] = self.record['qk']
        elif name == 'changeset':
            self.fillDefault(attrs)
        elif name == 'tag':
            k = attrs['k']
            v = attrs['v']
            # MongoDB doesn't let us have dots in the key names.
            #k = k.replace('.', ',,')
            self.record['tg'].append((k, v))
        elif name == 'way':
            self.fillDefault(attrs)
            self.record['n'] = []
            self.record['loc'] = []
        elif name == 'relation':
            self.fillDefault(attrs)
            self.record['m'] = []
        elif name == 'nd':
            ref = int(attrs['ref'])
            self.record['n'].append(ref)
            refLoc = self.nodeLocations[ref]
            if refLoc not in self.record['loc']:
                self.record['loc'].append(refLoc)
        elif name == 'member':
            ref = int(attrs['ref'])
            member = {'type': attrs['type'],
                      'ref':  ref,
                      'role': attrs['role']}
            self.record['m'].append(member)
            
            if attrs['type'] == 'way':
                ways2relations = self.client.osm.ways.find_one({ '_id' : ref})
                if ways2relations:
                    if 'relations' not in ways2relations:
                        ways2relations['relations'] = []
                    ways2relations['relations'].append(self.record['_id'])
                    self.client.osm.ways.save(ways2relations)
            elif attrs['type'] == 'node':
                nodes2relations = self.client.osm.nodes.find_one({ '_id' : ref})
                if nodes2relations:
                    if 'relations' not in nodes2relations:
                        nodes2relations['relations'] = []
                    nodes2relations['relations'].append(self.record['_id'])
                    self.client.osm.nodes.save(nodes2relations)
        
    def endElement(self, name):
        """Finish parsing an element
        (only really used with nodes, ways and relations)"""
        if name == 'node':
            self.nodeRecords.append(self.record)
            if len(self.nodeRecords) > 1500:
                self.client.osm.nodes.insert(self.nodeRecords)
                self.nodeRecords = []
                self.writeStatsToScreen()
            self.record = {}
            self.stats['nodes'] = self.stats['nodes'] + 1
        elif name == 'way':
            # Clean up any existing nodes
            if len(self.nodeRecords) > 0:
                self.client.osm.nodes.insert(self.nodeRecords)
                self.nodeRecords = []

            self.wayRecords.append(self.record)
            if len(self.wayRecords) > 100:
                self.client.osm.ways.insert(self.wayRecords)
                self.wayRecords = []
                self.writeStatsToScreen()
            self.record = {}
            self.stats['ways'] = self.stats['ways'] + 1
        elif name == 'relation':
            self.client.osm.relations.save(self.record)
            self.record = {}
            self.statsCount = self.statsCount + 1
            if self.statsCount > 10:
                self.writeStatsToScreen()
                self.statsCount = 0
            self.stats['relations'] = self.stats['relations'] + 1

if __name__ == "__main__":
    filename = sys.argv[1]

    if not os.path.exists(filename):
        print "Path %s doesn't exist." % (filename)
        sys.exit(-1)

    client = Connection()
    parser = make_parser()
    handler = OsmHandler(client)
    parser.setContentHandler(handler)
    parser.parse(open(filename))
    client.disconnect()

    print
