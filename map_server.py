import pymongo
from pymongo import Connection

class OsmApi:
    def __init__(self):
        self.client = Connection()

    def getNodesInBounds(self, box):
        cursor = self.client.osm.nodes.find({'loc' : { '$within' : { '$box' : box } } })
        nodes = []

        for row in cursor:
            nodes.append(row)

        return nodes

    def getNodesFromWays(self, ways):
        nodeIds = set()

        for way in ways:
            for nodeId in way['nodes']:
                nodeIds.add(nodeId)

        nodes = []
        for nodeId in nodeIds:
            node = self.client.osm.nodes.find_one({'id' : nodeId})
            if node:
                nodes.append(node)
            else:
                print "Error. couldn't find node id %d." % nodeId

        return nodes
        

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

#        cursor = self.client.osm.ways.find().where("{'id' : { '$all' : [%s] } }" % (','.join([str(y) for y in wayIds])))
#        for row in cursor:
#            ways.append(row)

        return ways

    def getWayIdsUsingNodeId(self, id):
        cursor = self.client.osm.nodes.find_one({'id' : id })
        if cursor and 'ways' in cursor:
            return cursor['ways']
        else:
            return []
    
    def getRelationsFromWays(self, ways):
        relationIds = set()
        
        for way in ways:
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
        cursor = self.client.osm.ways.find_one({'id' : id })
        if cursor and 'relations' in cursor:
            return cursor['relations']
        else:
            return []

    def getBbox(self, bbox):
        import time

        print "<!-- Start %s -->" % time.time()
        nodes = self.getNodesInBounds(bbox)
        print "<!-- Get nodes %s -->" % time.time()
        ways = self.getWaysFromNodes(nodes)
        print "<!-- Get ways %s -->" % time.time()

        wayNodes = self.getNodesFromWays(ways)
        for n in wayNodes:
            if n['id'] not in nodes:
                nodes.append(n)
        print "<!-- Get nodes from ways %s -->" % time.time()

        relations = self.getRelationsFromWays(ways)
        print "<!-- Get relations %s -->" % time.time()
        
        doc = {'bounds': {'minlat': bbox[0][0],
                          'minlon': bbox[0][1],
                          'maxlat': bbox[1][0],
                          'maxlon': bbox[1][1]},
               'nodes': nodes,
               'ways': ways,
               'relations': relations}

        return doc

class OsmXmlOutput:
    def defaultAttrs(self, item):
        return "id='%s' version='%s' user='%s'" % (item['id'], item['version'], item['user'])

    def write(self, data):
        print "<osm generator='mongosm 0.1' version='0.6'>"
        print "<bounds minlat='%f' minlon='%f' maxlat='%f' maxlon='%f'/>" % (data['bounds']['minlat'],
                                                                             data['bounds']['minlon'],
                                                                             data['bounds']['maxlat'],
                                                                             data['bounds']['maxlon'])

        for node in data['nodes']:
            print "<node lat='%s' lon='%s' %s>" % (node['loc']['lat'], node['loc']['lon'], self.defaultAttrs(node))
            for tag in node['tags'].items():
                print "<tag k='%s' v='%s'/>" % (tag[0], tag[1])
            print "</node>"

        for way in data['ways']:
            print "<way %s>" % (self.defaultAttrs(way),)
            for tag in way['tags'].items():
                print "<tag k='%s' v='%s'/>" % (tag[0], tag[1])
            for ref in way['nodes']:
                print "<nd ref='%d'/>" % (ref)
            print "</way>"

        for relation in data['relations']:
            print "<relation %s>" % (self.defaultAttrs(relation),)
            for tag in relation['tags'].items():
                print "<tag k='%s' v='%s'/>" % (tag[0], tag[1])
            for member in relation['members']:
                print "<member type='%s' ref='%d' role='%s'/>" % (member['type'], member['ref'], member['role'],)
            print "</relation>"
            
        print "</osm>"


if __name__ == '__main__':
    bbox = [[44.97357,-93.28899],[44.97638,-93.28581]]
    api = OsmApi()
    data = api.getBbox(bbox)
    
    outputter = OsmXmlOutput()
    outputter.write(data)
