from typing import List

import numpy as np

from ..database.drivers.neo4j import DatabaseDriver


class DataManager:
    similarity_threshold: float
    driver: DatabaseDriver
    managed_genres: List[str]

    def __init__(
        self,
        driver: DatabaseDriver,
        similarity_threshold: float,
        managed_genres: List[str],
    ) -> None:
        self.similarity_threshold = similarity_threshold
        self.driver = driver
        self.managed_genres = managed_genres

    def check_dupe_tracks(self):
        print()
        dupe_tracks_deleted = 0
        for genre in self.managed_genres:
            dupe_tracks_deleted_genre = 0
            all_tracks_nodes = self.driver.search_tracks_from_genre(genre)
            for node in all_tracks_nodes:
                node_name, node_id = node["name"], node["id"]
                similar_tracks = self.driver.get_possible_duplicated_tracks(
                    node_name, node_id, genre
                )

                if not similar_tracks:
                    continue

                for similar_node in similar_tracks:
                    similarity = self.compare_tracks_stats(node, similar_node)
                    if similarity <= self.similarity_threshold:
                        deleted_node = (
                            node
                            if node["popularity"] < similar_node["popularity"]
                            else similar_node
                        )
                        self.driver.remove_duplicate(deleted_node)
                        dupe_tracks_deleted_genre += 1
            print(f"Tracks deletadas em {genre} : {dupe_tracks_deleted_genre}")
            dupe_tracks_deleted += dupe_tracks_deleted_genre
        print(f"\n\nForam excluídas {dupe_tracks_deleted} tracks devido à duplicação.")

    def compare_tracks_stats(self, source, target):
        analysis_values = [
            "loudness",
            "liveness",
            "tempo",
            "valence",
            "instrumentalness",
            "danceability",
            "speechiness",
            "mode",
            "acousticness",
            "energy",
        ]
        attributes_source = np.array([source[key] for key in analysis_values])
        attributes_target = np.array([target[key] for key in analysis_values])

        similarity = np.linalg.norm(attributes_source - attributes_target)
        return similarity
