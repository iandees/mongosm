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
            id = node['id']
            wayIdsFromNode = self.getWayIdsUsingNodeId(id)

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
        cursor = self.client.osm.nodes2ways.find_one({'_id' : id })
        if cursor:
            return cursor['ways']
        else:
            return []

    def getBbox(self, bbox):
        nodes = self.getNodesInBounds(bbox)
        ways = self.getWaysFromNodes(nodes)
        nodes.extend(self.getNodesFromWays(ways))
        
        doc = {'nodes': nodes,
               'ways': ways,
               'relations': []}

        return doc

class OsmXmlOutput:
    def write(self, data):
        print "<osm>"

        for node in data['nodes']:
            print "<node id='%d' user='%s'>" % (node['id'], node['user'],)
            for tag in node['tags'].items():
                print "<tag k='%s' v='%s'/>" % (tag[0], tag[1])
            print "</node>"

        for way in data['ways']:
            print "<way id='%d' user='%s'>" % (way['id'], way['user'],)
            for tag in way['tags'].items():
                print "<tag k='%s' v='%s'/>" % (tag[0], tag[1])
            for ref in way['nodes']:
                print "<nd ref='%d'/>" % (ref)
            print "</way>"

        for relation in data['relations']:
            print "<relation id='%d' user='%s'>" % (relation['id'], relation['user'],)
            for tag in relation['tags'].items():
                print "<tag k='%s' v='%s'/>" % (tag[0], tag[1])
            for member in relation['members']:
                print "<member type='%s' ref='%d' role='%s'/>" % (type, ref, role,)
            print "</relation>"
            
        print "</osm>"


if __name__ == '__main__':
    bbox = [[44.982709,-93.248777],[44.985965,-93.244985]]
    api = OsmApi()
    data = api.getBbox(bbox)
    
    outputter = OsmXmlOutput()
    outputter.write(data)
