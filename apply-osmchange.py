"""Applies an OSMChange file to the database"""

import sys
import os
import time
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
            self.record['loc'] = {'lat': float(attrs['lat']),                                  'lon': float(attrs['lon'])}
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
                self.record['visable'] = False
            getattr(self, name + 's').append(self.record)
        elif name in ('create', 'modify', 'delete'):
            self.action = None
    def endDocument(self):
        """Upon document completion, commit the changes"""
        if not self.record:
            # We never saw any records.
            return
        else:
            # Check the last record to see if it's in the
            # database. Complain in the future
            if getattr(self.client.osm, self.record.type).find_one(
                {'id': self.record['id'], 'version', self.record['version']}):
                # In the future, scream
                pass
        for coll in ('nodes', 'ways', 'relations'):
            if getattr(self, coll):
                getattr(self.client.osm, coll).insert(getattr(self, coll))
        
if __name__ == "__main__":
    filename = sys.argv[1]

    if not os.path.exists(filename):
        print "Path %s doesn't exist." % (filename)
        sys.exit(-1)

    client = Connection()
    parser = make_parser()
    handler = OsmChangeHandler(client)
    parser.setContentHandler(handler)
    parser.parse(open(filename))
    client.disconnect()
