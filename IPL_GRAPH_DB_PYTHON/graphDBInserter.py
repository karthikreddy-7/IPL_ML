import time
import logging
from neo4j import GraphDatabase
from IPLGraph import IPLGraph
from temp import DataModelling

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class Neo4jLoader:
    def __init__(self, uri, user, password, match_file, ball_file):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        logging.info("Connected to Neo4j Database.")
        self.ipl_graph = IPLGraph()
        self.data_model = DataModelling(match_file, ball_file)

    def close(self):
        self.driver.close()
        logging.info("Closed connection to Neo4j Database.")

    def load_match_data(self):
        """Load match data after cleaning and validating, tracking performance."""
        logging.info("Processing match data before inserting into Neo4j...")

        match_data_list, _ = self.data_model.process_data()
        total_matches = len(match_data_list)
        start_time = time.time()

        with self.driver.session() as session:
            for i, match in enumerate(match_data_list, 1):
                try:
                    session.execute_write(self.ipl_graph._create_match_nodes, match)
                    session.execute_write(
                        self.ipl_graph._create_team_relationships, match
                    )
                    session.execute_write(
                        self.ipl_graph._create_player_relationships, match
                    )
                    session.execute_write(self.ipl_graph._create_match_outcomes, match)

                    if i % 50 == 0:  # Log every 100 matches
                        elapsed_time = time.time() - start_time
                        speed = i / elapsed_time
                        logging.info(
                            f"Inserted {i}/{total_matches} matches at {speed:.2f} matches/sec."
                        )

                except Exception as e:
                    logging.error(f"Error inserting match {match.match_id}: {str(e)}")

        total_time = time.time() - start_time
        speed = total_matches / total_time if total_time > 0 else 0
        logging.info(
            f"All {total_matches} matches inserted successfully in {total_time:.2f} sec "
            f"({speed:.2f} matches/sec)."
        )

    def load_ball_data(self):
        """Load ball-by-ball data after cleaning and validating, tracking performance."""
        logging.info("Processing ball-by-ball data before inserting into Neo4j...")

        _, ball_data_list = self.data_model.process_data()
        total_balls = len(ball_data_list)
        start_time = time.time()

        with self.driver.session() as session:
            for i, ball in enumerate(ball_data_list, 1):
                try:
                    session.execute_write(self.ipl_graph._create_delivery_nodes, ball)
                    session.execute_write(self.ipl_graph._update_player_stats, ball)
                    session.execute_write(
                        self.ipl_graph._create_wicket_relationships, ball
                    )

                    if i % 200 == 0:  # Log every 5000 balls
                        elapsed_time = time.time() - start_time
                        speed = i / elapsed_time
                        logging.info(
                            f"Inserted {i}/{total_balls} balls at {speed:.2f} balls/sec."
                        )

                except Exception as e:
                    logging.error(
                        f"Error inserting ball {ball.match_id}.{ball.over}.{ball.ball}: {str(e)}"
                    )

        total_time = time.time() - start_time
        speed = total_balls / total_time if total_time > 0 else 0
        logging.info(
            f"All {total_balls} balls inserted successfully in {total_time:.2f} sec "
            f"({speed:.2f} balls/sec)."
        )


if __name__ == "__main__":
    logging.info("Starting Neo4j data loader...")

    MATCH_FILE = r"C:\Users\basup\OneDrive\Desktop\IPL\IPL_Matches_2008_2022.csv"
    BALL_FILE = r"C:\Users\basup\OneDrive\Desktop\IPL\IPL_Ball_by_Ball_2008_2022.csv"

    loader = Neo4jLoader(
        "neo4j://localhost:7687", "neo4j", "password", MATCH_FILE, BALL_FILE
    )

    try:
        loader.load_match_data()
        loader.load_ball_data()
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
    finally:
        loader.close()
        logging.info("Data loading process completed.")
