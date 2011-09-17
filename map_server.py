import pymongo
from pymongo import Connection
from xml.sax.saxutils import escape
import re

class OsmApi:
    def __init__(self):
        self.client = Connection()


    def getNodesInBounds(self, box):
        return self.getNodesQuery([('bbox', box)])
    
    def getNodesQuery(self, query):
        cursor = self.client.osm.nodes.find(query)

        nodes = {}
        for row in cursor:
            nodes[row['_id']] = row

        return nodes

    def getNodes(self, query):
        nodes = self.getNodesQuery(query)

        return {'nodes': nodes.values()}

    def getWaysInBounds(self, box):
        return self.getWaysQuery([('bbox', box)])

    def getWaysQuery(self, query):
        cursor = self.client.osm.ways.find(query)

        ways = {}
        for row in cursor:
            ways[row['_id']] = row

        return ways

    def getWays(self, query):
        ways = self.getWaysQuery(query)
    
        nodes = {}
        nodes = self.getNodesFromWays(ways, nodes)

        return {'nodes': nodes.values(), 'ways': ways.values()}

    def getNodesFromWays(self, ways, existingNodes):
        nodeIds = set() 

        for way in ways.values():
            for nodeId in way['nodes']:
                nodeIds.add(nodeId)

        cursor = self.client.osm.nodes.find({'id': {'$in': list(nodeIds)} })

        for row in cursor:
            if row['_id'] not in existingNodes:
                existingNodes[row['_id']] = row

        return existingNodes
        

    def getWaysFromNodes(self, nodes):
        wayIds = set()

        for node in nodes:
            wayIdsFromNode = self.getWayIdsUsingNodeId(node['id'])

            for wayId in wayIdsFromNode:
                wayIds.add(wayId)

        ways = []

        for wayId in wayIds:
            way = self.client.osm.ways.find_one({'id' : wayId})
            if way:
                ways.append(way)
            else:
                print "Error. Couldn't find way id %d." % wayId

        return ways

    def getWayIdsUsingNodeId(self, id):
        cursor = self.client.osm.nodes.find_one({'id' : id }, ['ways'])
        if cursor and 'ways' in cursor:
            return cursor['ways']
        else:
            return []
    
    def getRelationsFromWays(self, ways):
        relationIds = set()
        
        for (wid, way) in ways.items():
            id = way['id']
            relationIdsFromWay = self.getRelationIdsUsingWayId(id)

            for relationId in relationIdsFromWay:
                relationIds.add(relationId)

        relations = []

        for relationId in relationIds:
            relation = self.client.osm.relations.find_one({'id' : relationId})
            if relation:
                relations.append(relation)
            else:
                print "Error. Couldn't find relation id %d." % relationId

        return relations

    def getRelationIdsUsingWayId(self, id):
        cursor = self.client.osm.ways.find_one({'id' : id }, ['relations'])
        if cursor and 'relations' in cursor:
            return cursor['relations']
        else:
            return []

    def getNodeById(self, id):
        cursor = self.client.osm.nodes.find_one({'id' : id })
        if cursor:
            return {'nodes': [cursor]}
        else:
            return {}

    def getWayById(self, id):
        print id
        cursor = self.client.osm.ways.find_one({'id' : id })
        if cursor:
            return {'ways': [cursor]}
        else:
            return {}

    def getRelationById(self, id):
        cursor = self.client.osm.relations.ways.find_one({'id' : id })
        if cursor:
            return {'relations': [cursor]}
        else:
            return {}

    def getPrimitives(self, xapi_query):
        nodes = self.getNodesQuery(xapi_query)

        ways = self.getWaysQuery(xapi_query)

        nodes = self.getNodesFromWays(ways, nodes)

        return {'nodes': nodes.values(), 'ways': ways.values()}

    def getBbox(self, bbox):
        import time, sys

        start = time.time()
        
        nodes = self.getNodesInBounds(bbox)

        timeA = time.time()
        sys.stderr.write("<!-- Get nodes in bbox %s -->\n" % (timeA - start))

        ways = self.getWaysInBounds(bbox)

        timeB = time.time()
        sys.stderr.write("<!-- Get ways in bbox %s -->\n" % (timeB - timeA))

        wayNodes = self.getNodesFromWays(ways, nodes)

        timeC = time.time()
        sys.stderr.write("<!-- Get nodes from ways %s -->\n" % (timeC - timeB))


        timeD = time.time()
        sys.stderr.write("<!-- Collate nodes from ways %s -->\n" % (timeD - timeC))

        relations = self.getRelationsFromWays(ways)

        timeE = time.time()
        sys.stderr.write("<!-- Get relations %s -->\n" % (timeE - timeD))
        
        doc = {'bounds': {'minlat': bbox[0][0],
                          'minlon': bbox[0][1],
                          'maxlat': bbox[1][0],
                          'maxlon': bbox[1][1]},
               'nodes': nodes.values(),
               'ways': ways.values(),
               'relations': relations}

        return doc

class OsmXmlOutput:
    def addNotNullAttr(self, mappable, mappableElement, name):
        if name in mappable:
            mappableElement.setAttribute(escape(name), escape(unicode(mappable[name])))

    def defaultAttrs(self, mappableElement, mappable):
        self.addNotNullAttr(mappable, mappableElement, "id")
        self.addNotNullAttr(mappable, mappableElement, "version")
        self.addNotNullAttr(mappable, mappableElement, "user")

    def tagNodes(self, doc, mappableElement, mappable):
        for tag in mappable['tags'].items():
            tagElement = doc.createElement("tag")
            tagElement.setAttribute("k", tag[0])
            tagElement.setAttribute("v", tag[1])
            mappableElement.appendChild(tagElement)

    def iter(self, data):
        from xml.dom.minidom import Document
        doc = Document()

        yield '<osm generator="%s" version="%s">\n' % ("mongosm 0.1", "0.6")

        if 'bounds' in data:
            yield '<bounds minlat="%s" minlon="%s" maxlat="%s" maxlon="%s"/>\n' % (
                    str(data['bounds']['minlat']),
                    str(data['bounds']['minlon']),
                    str(data['bounds']['maxlat']),
                    str(data['bounds']['maxlon']))

        if 'nodes' in data:
            for node in data['nodes']:
                nodeElem = doc.createElement("node")
                nodeElem.setAttribute("lat", str(node['loc']['lat']))
                nodeElem.setAttribute("lon", str(node['loc']['lon']))
                self.defaultAttrs(nodeElem, node)
                self.tagNodes(doc, nodeElem, node)
                yield "%s\n" % (nodeElem.toxml('UTF-8'),)

        if 'ways' in data:
            for way in data['ways']:
                wayElem = doc.createElement("way")
                self.defaultAttrs(wayElem, way)
                self.tagNodes(doc, wayElem, way)
                for ref in way['nodes']:
                    refElement = doc.createElement("nd")
                    refElement.setAttribute("ref", str(ref))
                    wayElem.appendChild(refElement)
                yield "%s\n" % (wayElem.toxml('UTF-8'),)

        if 'relations' in data:
            for relation in data['relations']:
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
    def buildMongoQuery(self, xapiQuery):
        q = {}
        groups = re.findall(r'(?:\[(.*?)\])', xapiQuery)
        for g in groups:
            (left, right) = g.split('=')
            if left == '@user':
                q['user'] = right
            elif left == '@uid':
                q['uid'] = long(right)
            elif left is '@changeset':
                q['changeset'] = long(right)
            elif left == 'bbox':
                (minlon, minlat, maxlon, maxlat) = g[5:].split(',')
                bboxPolygon = [ [minlon,minlat],
                                [minlon,maxlat],
                                [maxlon,maxlat],
                                [maxlon,minlat] ]
                q['loc'] = { '$within': { '$polygon': bboxPolygon } }
            elif right == u'*':
                q['tags.%s' % (left,)] = {'$exists': True}
            else:
                q['tags.%s' % (left,)] = right

        print "Built query: %s" % (q,)

        return q

    def mapRequest(self, request):
        (minlon, minlat, maxlon, maxlat) = request.args['bbox'].split(',')
        bbox = [[float(minlat), float(minlon)],[float(maxlat), float(maxlon)]]
        api = OsmApi()
        data = api.getBbox(bbox)

        outputter = OsmXmlOutput()

        return Response(outputter.iter(data), content_type='text/xml', direct_passthrough=True)

    def changesetsRequest(self, request):
        return Response("<boop>%s</boop>" % (xapi_query,))

    def getNode(self, request, id):
        api = OsmApi()
        data = api.getNodeById(long(id))

        outputter = OsmXmlOutput()
        return Response(outputter.iter(data), content_type='text/xml')

    def getNodeQuery(self, request, xapi_query):
        query = self.buildMongoQuery(xapi_query)

        api = OsmApi()
        data = api.getNodes(query)

        outputter = OsmXmlOutput()
        return Response(outputter.iter(data), content_type='text/xml')

    def getWay(self, request, id):
        api = OsmApi()
        data = api.getWayById(long(id))

        outputter = OsmXmlOutput()
        return Response(outputter.iter(data), content_type='text/xml')

    def getWayQuery(self, request, xapi_query):
        query = self.buildMongoQuery(xapi_query)

        api = OsmApi()
        data = api.getWays(query)

        outputter = OsmXmlOutput()
        return Response(outputter.iter(data), content_type='text/xml')

    def getRelation(self, request, id):
        api = OsmApi()
        data = api.getRelationById(long(id))

        outputter = OsmXmlOutput()
        return Response(outputter.iter(data), content_type='text/xml')

    def getRelationQuery(self, request, xapi_query):
        query = self.buildMongoQuery(xapi_query)

        api = OsmApi()
        data = api.getRelations(query)

        outputter = OsmXmlOutput()
        return Response(outputter.iter(data), content_type='text/xml')

    def getPrimitiveQuery(self, request, xapi_query):
        query = self.buildMongoQuery(xapi_query)

        api = OsmApi()
        data = api.getPrimitives(query)

        outputter = OsmXmlOutput()
        return Response(outputter.iter(data), content_type='text/xml')

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
            Rule('/api/0.6/map', endpoint='mapRequest'),

            Rule('/api/0.6/changesets', endpoint='changesetsRequest'),

            Rule('/api/0.6/node/<id>', endpoint='getNode'),
            Rule('/api/0.6/way/<id>', endpoint='getWay'),
            Rule('/api/0.6/relation/<id>', endpoint='getRelation'),

            Rule('/api/0.6/node<xapi_query>', endpoint='getNodeQuery'),
            Rule('/api/0.6/way<xapi_query>', endpoint='getWayQuery'),
            Rule('/api/0.6/relation<xapi_query>', endpoint='getRelationQuery'),
            
            Rule('/api/0.6/*<xapi_query>', endpoint='getPrimitiveQuery'),

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
    run_simple('127.0.0.1', 5000, app, use_debugger=True, use_reloader=True)
