from dataclasses import dataclass
from os import getenv

from dotenv import load_dotenv

from ...models.BaseModel import BaseModel

load_dotenv()

DB_USER = getenv("DB_USER")
DB_PASSWORD = getenv("DB_PASSWORD")
DB_HOST = getenv("DB_HOST")
DB_PROTOCOL = getenv("DB_PROTOCOL")
DB_PORT = getenv("DB_PORT")
DB_NAME = getenv("DB_NAME")

CONNECTION_URI = f"{DB_PROTOCOL}://{DB_HOST}:{DB_PORT}"


@dataclass
class DatabaseDriver:
    database_name: str = DB_NAME
    connection_uri: str = CONNECTION_URI
    db_user: str = DB_USER
    db_password: str = DB_PASSWORD
    db_port: str = DB_PORT
    db_host: str = DB_HOST

    def check_if_supergenre_exists(self, supergenre: str):
        pass

    def create_supergenres_nodes(self, mapped_genres_dict: dict):
        pass

    def create_node_if_dont_exists(self, node: BaseModel):
        pass

    def create_edge_if_dont_exists(self, from_node_id: str, to_node_id: str):
        pass

    def update_track_info(self, track_id: str, features: dict):
        pass

    def get_possible_duplicated_tracks(
        self, track_name: str, track_id: str, genre: str
    ):
        pass

    def check_dupe_tracks():
        pass

    def remove_duplicate(self, node: dict):
        pass

    def remove_no_features_musics(self):
        pass

    def search_tracks_from_genre(self, genre: str):
        pass
