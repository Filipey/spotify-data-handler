from arango import ArangoClient
from arango.database import StandardDatabase
from arango.graph import Graph

from .wrapper import DatabaseDriver


class ArangoDriver(DatabaseDriver):
    driver: ArangoClient
    db: StandardDatabase
    graph: Graph

    def __init__(self):
        self.driver = ArangoClient(hosts=self.connection_uri)
        sys_db = self.driver.db("_system", self.db_user, self.db_password)

        if not sys_db.has_database(self.database_name):
            sys_db.create_database(self.database_name)

        self.db = self.driver.db(self.database_name, self.db_user, self.db_password)

        if not self.db.has_graph(self.database_name):
            self.graph = self.db.create_graph(self.database_name)

        self.graph = self.db.graph(self.database_name)

        if not self.db.has_collection("Tracks"):
            self.db.create_collection("Tracks")

        if not self.db.has_collection("Genres"):
            self.db.create_collection("Genres")

        if not self.db.has_collection("HAS_GENRE"):
            self.db.create_collection("HAS_GENRE", edge=True)

        if not self.graph.has_vertex_collection("Tracks"):
            self.graph.create_vertex_collection("Tracks")

        if not self.graph.has_vertex_collection("Genres"):
            self.graph.create_vertex_collection("Genres")

        if not self.graph.has_edge_definition("HAS_GENRE"):
            self.graph.create_edge_definition("HAS_GENRE", ["Tracks"], ["Genres"])

    def check_if_supergenre_exists(self, supergenre: str):
        pass

    def create_supergenres_nodes(self, mapped_genres_dict: dict):
        pass
