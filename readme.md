MongOSM
=======

MongOSM is a set of Python utilities that manipulate OSM data in MongoDB:

- `insert_osm_data.py` reads an OSM file and writes it to a MongoDB database.
- `map_server.py` uses Werkzeug to start a WSGI server that responds to the
  read-only OSM APIs and most of the XAPI-style predicate queries.
- `apply-osmchange.py` is currently not tested, but it is supposed to read
  minutely change files from planet.osm.org and keep the MongoDB database
  up to date.
