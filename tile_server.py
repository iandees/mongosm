import pymongo
from pymongo import Connection
from xml.sax.saxutils import escape
import re

from globalmaptiles import GlobalMercator

class OsmApi:
    def __init__(self):
        self.client = Connection()
        self.proj = GlobalMercator()

    def getTile(self, zoom, x, y):
        (x, y) = self.proj.GoogleTile(x,y,zoom)
        quadkey = self.proj.QuadTree(x,y,zoom)
        print "Querying for %s." % (quadkey,)
        (minlat, minlon, maxlat, maxlon) = self.proj.TileLatLonBounds(x,y,zoom)

        # Nodes in the tile
        nodes = {}
        cursor = self.client.osm.nodes.find({'qk': {'$regex': "^%s" % (quadkey,)} })
        for row in cursor:
            nodes[row['_id']] = row

        # Ways with nodes in the tile
        ways = {}
        cursor = self.client.osm.ways.find({'loc': {'$regex': "^%s" % (quadkey,)} })
        for row in cursor:
            ways[row['_id']] = row

        # Nodes on ways that extend beyond the bounding box
        otherNids = set()
        for way in ways.values():
            for nid in way['nodes']:
                otherNids.add(nid)
        cursor = self.client.osm.nodes.find({'_id': {'$in': list(otherNids)} })
        for row in cursor:
            nodes[row['_id']] = row

        # Relations that contain any of the above as members
        relations = {}

        # Sort the results by id
        nodes = sorted(nodes.iteritems())
        ways = sorted(ways.iteritems())
        relations = sorted(relations.iteritems())

        doc = {'bounds': {'minlat': minlat,
                          'minlon': minlon,
                          'maxlat': maxlat,
                          'maxlon': maxlon},
               'nodes': nodes,
               'ways': ways,
               'relations': relations}

        return doc

class OsmXmlOutput:
    def addNotNullAttr(self, mappable, mappableElement, name, outName=None):
        if not outName:
            outName = name
        if name in mappable:
            mappableElement.setAttribute(escape(outName), escape(unicode(mappable[name])))

    def defaultAttrs(self, mappableElement, mappable):
        self.addNotNullAttr(mappable, mappableElement, "_id", "id")
        self.addNotNullAttr(mappable, mappableElement, "v", "version")
        self.addNotNullAttr(mappable, mappableElement, "user")

    def tagNodes(self, doc, mappableElement, mappable):
        for mappable in mappable['tags']:
            k,v = mappable
            tagElement = doc.createElement("tag")
            tagElement.setAttribute("k", k)
            tagElement.setAttribute("v", v)
            mappableElement.appendChild(tagElement)

    def iter(self, data):
        from xml.dom.minidom import Document
        doc = Document()

        yield '<osm generator="%s" version="%s">\n' % ("tiled mongosm 0.1", "0.6")

        if 'bounds' in data:
            yield '<bounds minlat="%s" minlon="%s" maxlat="%s" maxlon="%s"/>\n' % (
                    str(data['bounds']['minlat']),
                    str(data['bounds']['minlon']),
                    str(data['bounds']['maxlat']),
                    str(data['bounds']['maxlon']))

        if 'nodes' in data:
            for (id, node) in data['nodes']:
                nodeElem = doc.createElement("node")
                nodeElem.setAttribute("lat", str(node['loc']['lat']))
                nodeElem.setAttribute("lon", str(node['loc']['lon']))
                self.defaultAttrs(nodeElem, node)
                self.tagNodes(doc, nodeElem, node)
                yield "%s\n" % (nodeElem.toxml('UTF-8'),)

        if 'ways' in data:
            for (id, way) in data['ways']:
                wayElem = doc.createElement("way")
                self.defaultAttrs(wayElem, way)
                self.tagNodes(doc, wayElem, way)
                for ref in way['nodes']:
                    refElement = doc.createElement("nd")
                    refElement.setAttribute("ref", str(ref))
                    wayElem.appendChild(refElement)
                yield "%s\n" % (wayElem.toxml('UTF-8'),)

        if 'relations' in data:
            for (id, relation) in data['relations']:
                relationElem = doc.createElement("relation")
                self.defaultAttrs(relationElem, relation)
                self.tagNodes(doc, relationElem, relation)
                for member in relation['members']:
                    memberElem = doc.createElement("member")
                    memberElem.setAttribute("type", member['type'])
                    memberElem.setAttribute("ref", str(member['ref']))
                    memberElem.setAttribute("role", member['role'])
                    relationElem.appendChild(memberElem)
                yield "%s\n" % (relationElem.toxml('UTF-8'),)

        yield '</osm>\n'

import time, sys
import os
import urlparse
from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import HTTPException, NotFound

class Mongosm(object):

    def tileRequest(self, request, zoom, x, y):
        #(minlon, minlat, maxlon, maxlat) = request.args['bbox'].split(',')
        #bbox = [[float(minlat), float(minlon)],[float(maxlat), float(maxlon)]]

        api = OsmApi()
        data = api.getTile(int(zoom), int(x), int(y))

        outputter = OsmXmlOutput()

        return Response(outputter.iter(data), content_type='text/xml', direct_passthrough=True)

    def capabilitiesRequest(self, request):
        return Response("""
            <osm version="0.6" generator="mongosm 0.1">
                <api>
                    <version minimum="0.6" maximum="0.6"/>
                    <area maximum="0.5"/>
                </api>
            </osm>""")

    def __init__(self):
        self.url_map = Map([
            Rule('/tiles/0.6/<zoom>/<x>/<y>', endpoint='tileRequest'),
            Rule('/api/capabilities', endpoint='capabilitiesRequest'),
        ])

    def dispatch_request(self, request):
        adapter = self.url_map.bind_to_environ(request.environ)
        try:
            endpoint, values = adapter.match()
            return getattr(self, endpoint)(request, **values)
        except HTTPException, e:
            return e

    def wsgi_app(self, environ, start_response):
        request = Request(environ)
        response = self.dispatch_request(request)
        return response(environ, start_response)

    def __call__(self, environ, start_response):
        return self.wsgi_app(environ, start_response)

if __name__ == '__main__':
    from werkzeug.serving import run_simple
    app = Mongosm()
    run_simple('0.0.0.0', 5000, app, use_debugger=True, use_reloader=True)
