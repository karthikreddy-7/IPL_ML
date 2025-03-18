import pandas as pd
import logging
from typing import List
from pydantic import ValidationError
from model import MatchData, BallData
import re

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class DataModelling:
    def __init__(self, match_file: str, ball_file: str):
        self.match_file = match_file
        self.ball_file = ball_file

    def load_data(self, file_path: str) -> pd.DataFrame:
        """Load CSV data into a Pandas DataFrame with basic preprocessing."""
        try:
            df = pd.read_csv(file_path)
            logging.info(f"Successfully loaded {file_path}, shape: {df.shape}")
            return df
        except Exception as e:
            logging.error(f"Error loading {file_path}: {e}")
            return pd.DataFrame()

    def clean_season(self, season):
        """Extract the first four digits (year) from the season string."""
        match = re.search(r"\d{4}", str(season))
        return int(match.group()) if match else None

    def clean_match_data(self, df: pd.DataFrame) -> List[MatchData]:
        """Convert DataFrame into a list of validated MatchData objects."""
        match_data_list = []
        df["Season"] = df["Season"].apply(self.clean_season)

        for idx, row in df.iterrows():
            try:
                margin = int(row["Margin"]) if pd.notna(row["Margin"]) and str(row["Margin"]).isdigit() else None

                match_data = MatchData(
                    match_id=int(row["ID"]),
                    city=row["City"] if pd.notna(row["City"]) else None,
                    date=row["Date"],
                    season=int(row["Season"]),
                    match_number=row["MatchNumber"],
                    team1=row["Team1"],
                    team2=row["Team2"],
                    venue=row["Venue"],
                    toss_winner=row["TossWinner"] if pd.notna(row["TossWinner"]) else None,
                    toss_decision=row["TossDecision"] if pd.notna(row["TossDecision"]) else None,
                    super_over="Yes" if pd.notna(row["WinningTeam"]) else "No",
                    winning_team=row["WinningTeam"] if pd.notna(row["WinningTeam"]) else None,
                    won_by=row["WonBy"] if pd.notna(row["WonBy"]) else None,
                    margin=margin,
                    player_of_match=row["Player_of_Match"] if pd.notna(row["Player_of_Match"]) else None,
                    team1_players=row["Team1Players"].split(",") if pd.notna(row["Team1Players"]) else None,
                    team2_players=row["Team2Players"].split(",") if pd.notna(row["Team2Players"]) else None,
                    umpire1=row["Umpire1"] if pd.notna(row["Umpire1"]) else None,
                    umpire2=row["Umpire2"] if pd.notna(row["Umpire2"]) else None
                )

                match_data_list.append(match_data)
            except Exception as e:
                logging.error(f"Validation error in match data at row {idx} (Match ID: {row['ID']}): {e}")
                logging.info(f"Row Data: {row.to_dict()}")  # Debugging: Print the entire row causing the error

        return match_data_list

    def clean_ball_data(self, df: pd.DataFrame) -> List[BallData]:
        """Convert DataFrame into a list of validated BallData objects."""
        ball_data_list = []

        for idx, row in df.iterrows():
            try:
                ball_data = BallData(
                    match_id=int(row["ID"]),
                    innings=int(row["innings"]),
                    over=int(row["overs"]),
                    ball=int(row["ballnumber"]),
                    batter=row["batter"],
                    bowler=row["bowler"],
                    non_striker=row["non_striker"],
                    extra_type=row["extra_type"] if pd.notna(row["extra_type"]) else None,
                    batsman_run=int(row["batsman_run"]),
                    extras_run=int(row["extras_run"]),
                    total_run=int(row["total_run"]),
                    non_boundary=int(row["non_boundary"]),
                    is_wicket_delivery=int(row["isWicketDelivery"]),
                    player_out=row["player_out"] if pd.notna(row["player_out"]) else None,
                    kind=row["kind"] if pd.notna(row["kind"]) else None,
                    fielders_involved=row["fielders_involved"] if pd.notna(row["fielders_involved"]) else None,
                    batting_team=row["BattingTeam"]
                )

                ball_data_list.append(ball_data)
            except ValidationError as e:
                logging.error(f"Validation error in ball data at row {idx} (Match ID: {row['ID']}): {e}")
                logging.info(f"Row Data: {row.to_dict()}")

        return ball_data_list


    def process_data(self):
        """Main function to load, clean, and validate data."""
        logging.info("Loading match and ball data...")
        match_df = self.load_data(self.match_file)
        ball_df = self.load_data(self.ball_file)

        logging.info("Cleaning and validating match data...")
        match_data = self.clean_match_data(match_df)

        logging.info("Cleaning and validating ball data...")
        ball_data = self.clean_ball_data(ball_df)

        logging.info(f"Successfully processed {len(match_data)} matches and {len(ball_data)} ball events.")
        return match_data, ball_data


if __name__ == "__main__":
    logging.info("Starting IPL Data Processing...")

    # Replace with actual file paths
    match_csv_path = r"C:\Users\basup\OneDrive\Desktop\IPL\IPL_Matches_2008_2022.csv"
    ball_csv_path = r"C:\Users\basup\OneDrive\Desktop\IPL\IPL_Ball_by_Ball_2008_2022.csv"

    data_model = DataModelling(match_csv_path, ball_csv_path)
    match_data, ball_data = data_model.process_data()
