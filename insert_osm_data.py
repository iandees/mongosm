"""This program parses an OSM XML file and inserts the data in a
MongoDB database"""

import sys
import os
import time
import pymongo
from datetime import datetime
#from xml.sax import make_parser
#from xml.sax.handler import ContentHandler
from pymongo import Connection
from xml.etree.cElementTree import iterparse

class OsmHandler(object):
    """Base class for parsing OSM XML data"""
    def __init__(self, client):
        self.client = client
        """
        self.client.osm.nodes.ensure_index([('loc', pymongo.GEO2D)])
        self.client.osm.nodes.ensure_index([('id', pymongo.ASCENDING),
                                            ('version', pymongo.DESCENDING)])
        self.client.osm.ways.ensure_index([('id', pymongo.ASCENDING),
                                           ('version', pymongo.DESCENDING)])
        self.client.osm.ways.ensure_index([('loc', pymongo.GEO2D)])
        self.client.osm.relations.ensure_index([('id', pymongo.ASCENDING),
                                                ('version', pymongo.DESCENDING)])
        """
        self.stat_nodes = 0
        self.stat_ways = 0
        self.stat_relations = 0
        self.lastStatString = ""
        self.statsCount = 0

    def writeStatsToScreen(self):
        for char in self.lastStatString:
            sys.stdout.write('\b')
        self.lastStatString = "%dk nodes, %dk ways, %d relations" % (self.stat_nodes / 1000,
                                                                     self.stat_ways / 1000,
                                                                     self.stat_relations)
        sys.stdout.write(self.lastStatString)

    def fillDefault(self, attrs):
        """Fill in default record values"""
        record = dict(_id=long(attrs['id']),
                      #ts=self.isoToTimestamp(attrs['timestamp']),
                      ts=attrs['timestamp'],
                      tg=[],
                      ky=[])
        #record['_id'] = long(attrs['id'])
        #record['timestamp'] = self.isoToTimestamp(attrs['timestamp'])
        #record['tags'] = [] 
        #record['keys'] = []
        if attrs.has_key('user'):
            record['un'] = attrs['user']
        if attrs.has_key('uid'):
            record['ui'] = long(attrs['uid'])
        if attrs.has_key('version'):
            record['v'] = int(attrs['version'])
        if attrs.has_key('changeset'):
            record['ch'] = long(attrs['changeset'])
        return record

    def isoToTimestamp(self, isotime):
        """Parse a date and return a time tuple"""
        t = datetime.strptime(isotime, "%Y-%m-%dT%H:%M:%SZ")
        return time.mktime(t.timetuple())

    def parse(self, file_obj):
        nodes = []
        ways = []
        
        for (event, elem) in iterparse(file_obj, events=('start', 'end')):
            name = elem.tag
            attrs = elem.attrib
            if 'start' == event:
                """Parse the XML element at the start"""
                if name == 'node':
                    record = self.fillDefault(attrs)
                    loc = [float(attrs['lat']),
                           float(attrs['lon'])]
                    record['loc'] = loc
                elif name == 'tag':
                    k = attrs['k']
                    v = attrs['v']
                    # MongoDB doesn't let us have dots in the key names.
                    #k = k.replace('.', ',,')
                    record['tg'].append((k, v))
                    record['ky'].append(k)
                elif name == 'way':
                    # Insert remaining nodes
                    if len(nodes) > 0:
                        self.client.osm.nodes.insert(nodes)
                        nodes = []

                    record = self.fillDefault(attrs)
                    record['nd'] = []
                elif name == 'relation':
                    # Insert remaining ways
                    if len(ways) > 0:
                        self.client.osm.ways.insert(ways)
                        ways = []

                    record = self.fillDefault(attrs)
                    record['mm'] = []
                elif name == 'nd':
                    ref = long(attrs['ref'])
                    record['nd'].append(ref)
                elif name == 'member':
                    record['mm'].append(dict(type=attrs['type'],
                                                  ref=long(attrs['ref']),
                                                  role=attrs['role']))
                    
                    if attrs['type'] == 'way':
                        ways2relations = self.client.osm.ways.find_one({ '_id' : ref})
                        if ways2relations:
                            if 'relations' not in ways2relations:
                                ways2relations['relations'] = []
                            ways2relations['relations'].append(record['_id'])
                            self.client.osm.ways.save(ways2relations)
                    elif attrs['type'] == 'node':
                        nodes2relations = self.client.osm.nodes.find_one({ '_id' : ref})
                        if nodes2relations:
                            if 'relations' not in nodes2relations:
                                nodes2relations['relations'] = []
                            nodes2relations['relations'].append(record['_id'])
                            self.client.osm.nodes.save(nodes2relations)
            elif 'end' == event:
                """Finish parsing an element
                (only really used with nodes, ways and relations)"""
                if name == 'node':
                    if len(record['tg']) == 0:
                        del record['tg']
                    if len(record['ky']) == 0:
                        del record['ky']
                    nodes.append(record)
                    if len(nodes) > 2500:
                        self.client.osm.nodes.insert(nodes)
                        nodes = []
                        self.writeStatsToScreen()

                    record = {}
                    self.stat_nodes = self.stat_nodes + 1
                elif name == 'way':
                    if len(record['tg']) == 0:
                        del record['tg']
                    if len(record['ky']) == 0:
                        del record['ky']
                    nds = self.client.osm.nodes.find({ '_id': { '$in': record['nd'] } }, { 'loc': 1, '_id': 0 })
                    record['loc'] = []
                    for node in nds:
                        record['loc'].append(node)

                    ways.append(record)
                    if len(ways) > 2000:
                        self.client.osm.ways.insert(ways)
                        ways = []

                    record = {}
                    self.statsCount = self.statsCount + 1
                    if self.statsCount > 1000:
                        self.writeStatsToScreen()
                        self.statsCount = 0
                    self.stat_ways = self.stat_ways + 1
                elif name == 'relation':
                    if len(record['tg']) == 0:
                        del record['tg']
                    if len(record['ky']) == 0:
                        del record['ky']
                    self.client.osm.relations.save(record)
                    record = {}
                    self.statsCount = self.statsCount + 1
                    if self.statsCount > 10:
                        self.writeStatsToScreen()
                        self.statsCount = 0
                    self.stat_relations = self.stat_relations + 1
            elem.clear()

if __name__ == "__main__":
    filename = sys.argv[1]

    if not os.path.exists(filename):
        print "Path %s doesn't exist." % (filename)
        sys.exit(-1)

    client = Connection()
    #parser = make_parser()
    handler = OsmHandler(client)
    #parser.setContentHandler(handler)
    #parser.parse(open(filename))
    handler.parse(open(filename))
    client.disconnect()
