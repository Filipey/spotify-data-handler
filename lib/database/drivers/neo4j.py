from dataclasses import dataclass
from typing import TypeVar, Union

from neo4j import Driver, GraphDatabase
from neo4j.exceptions import DatabaseError, DriverError

from lib.models.BaseModel import BaseModel

from .wrapper import DatabaseDriver

T = TypeVar("T")


class Neo4JDriver(DatabaseDriver):
    driver: Driver

    def __init__(self) -> None:
        self.driver = GraphDatabase.driver(
            uri=self.connection_uri, auth=(self.db_user, self.db_password)
        )

    def close(self) -> None:
        self.driver.close()

    def exec(self, query: str) -> Union[T, None]:
        try:
            with self.driver.session() as session:
                records = session.run(query)
                return records.data()
        except (DriverError, DatabaseError) as exception:
            print(f"Erro: {exception}")
        finally:
            self.close()

    def check_if_supergenre_exists(self, supergenre: str):
        query = f"MATCH(g: Genre {{ name: '{supergenre}' }}) RETURN g"
        result = self.exec(query)

        return True if result != [] else False

    def create_supergenres_nodes(self, mapped_genres_dict: dict):
        for supergenre, genres in mapped_genres_dict.items():
            if not self.check_if_supergenre_exists(supergenre):
                self.driver.exec(
                    f"CREATE (g: Genre {{ name: '{supergenre}', subgenres: {genres} }})"
                )

    def create_node_if_dont_exists(self, node: BaseModel):
        cypher_query = f'MATCH (t:{node._node_name}{{id: "{node.id}"}}) RETURN t'
        result = self.exec(cypher_query)

        node_exists = result != []

        if not node_exists:
            cypher_query = f"CREATE (t:{node._node_name} {node.to_cypher()}) RETURN t"
            node = self.exec(cypher_query)
            print(f"\n\033[92m Created Node {node.__repr__()}")
            return node[0]["t"]

        return result[0]["t"]

    def create_edge_if_dont_exists(self, from_track_id: str, to_genre_name: str):
        check_if_edge_exists_query = f"MATCH (t: Track {{ id: '{from_track_id}' }})-[:HAS_GENRE]->(g: Genre {{ name: '{to_genre_name}' }}) \
                                       RETURN COUNT(*) AS count"
        result = self.driver.exec(check_if_edge_exists_query)
        edge_exists = result[0]["count"] > 0

        if not edge_exists:
            cypher_query = f"MATCH (n: Track {{ id: '{from_track_id}' }}) MATCH (g: Genre {{ name: '{to_genre_name}' }}) \
                             CREATE (n)-[:HAS_GENRE]->(g)"
            self.driver.exec(cypher_query)

    def update_track_info(self, track_id: str, features: dict):
        set_attr_string = "SET "
        for index, (key, value) in enumerate(features.items()):
            set_attr_string += (
                f"t.{key} = {value}{', ' if index < len(features) - 1 else ';'}"
            )

        cypher_query = f"MATCH (t: Track {{ id: '{track_id}' }}) {set_attr_string}"
        self.exec(cypher_query)

    def search_tracks_from_genre(self, genre: str):
        all_tracks_nodes = self.driver.exec(
            f"MATCH(t: Track)-[:HAS_GENRE]->(g {{ name: '{genre}' }}) RETURN t"
        )

        clear_nodes = [node["t"] for node in all_tracks_nodes]
        return clear_nodes

    def get_possible_duplicated_tracks(
        self, track_name: str, track_id: str, genre: str
    ):
        query = f'MATCH (t: Track)-[:HAS_GENRE]->(g: Genre {{ name: "{genre}" }}) \
                 WHERE t.name CONTAINS "{track_name}" \
                 AND t.id <> "{track_id}" \
                 RETURN t ORDER BY t.popularity'

        nodes = self.driver.exec(query)

        if len(nodes) > 0:
            return nodes
        return False

    def remove_duplicate(self, node: dict):
        query = f"MATCH(t: Track {{ id: '{node['id']}' }}) DETACH DELETE t"
        self.driver.exec(query)

    def remove_no_features_musics(self):
        query = f"MATCH(t: Track) WHERE t.valence IS NULL DETACH DELETE t"
        self.driver.exec(query)
