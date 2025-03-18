import json
import pandas as pd
import os

# Define output CSV file names
MATCHES_CSV = "matches.csv"
BALL_BY_BALL_CSV = "ball_by_ball.csv"

def process_json(file_path, match_id):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    match_records = []
    ball_records = []

    # Extract match-level details
    city = data['info'].get('city', 'Unknown')
    date = data['info']['dates'][0]
    season = data['info'].get('season', 'Unknown')
    match_number = data['info'].get('event').get('match_number', 'Unknown')
    team1, team2 = data['info']['teams']
    venue = data['info'].get('venue', 'Unknown')
    toss_winner = data['info']['toss']['winner']
    toss_decision = data['info']['toss']['decision']
    super_over = 'Y' if data['info'].get('super_over', False) else 'N'
    winning_team = data['info']['outcome'].get('winner', 'No Result')
    won_by = 'Runs' if 'runs' in data['info']['outcome'].get('by', {}) else 'Wickets'
    margin = data['info']['outcome'].get('by', {}).get('runs', data['info']['outcome'].get('by', {}).get('wickets', 0))
    player_of_match = ', '.join(data['info'].get('player_of_match', []))
    team1_players = data['info']['players'][team1]
    team2_players = data['info']['players'][team2]
    umpire1, umpire2 = data['info']['officials']['umpires']

    match_records.append([
        match_id, city, date, season, match_number, team1, team2, venue,
        toss_winner, toss_decision, super_over, winning_team, won_by, margin,
        player_of_match, team1_players, team2_players, umpire1, umpire2
    ])

    print(f"Processed match ID: {match_id} | Teams: {team1} vs {team2}")

    # Process ball-by-ball data
    total_balls = 0
    innings_number = 0  # Track innings separately

    for inning in data['innings']:
        innings_number += 1
        batting_team = inning['team']  # Extract the batting team
        
        for over in inning['overs']:
            over_number = over['over']
            ball_number = 0  # Reset ball number at the start of each over
            
            for ball in over['deliveries']:
                ball_number += 1  # Manually increment the ball number

                batter = ball['batter']
                bowler = ball['bowler']
                non_striker = ball['non_striker']

                # Extract extras properly
                extras = ball.get('extras', {})
                extra_type = ', '.join(extras.keys()) if extras else 'NA'
                extras_run = sum(extras.values()) if extras else 0  # Sum all extra runs

                # Extract runs safely
                batsman_run = ball['runs']['batter']
                total_run = ball['runs']['total']
                non_boundary = ball.get('non_boundary', 0)

                # Handle wickets correctly
                is_wicket = 1 if 'wickets' in ball else 0
                if 'wickets' in ball:
                    player_out = ball['wickets'][0]['player_out']
                    kind = ball['wickets'][0]['kind']
                    fielders_involved = ', '.join([f['name'] for f in ball['wickets'][0].get('fielders', [])])
                else:
                    player_out = 'NA'
                    kind = 'NA'
                    fielders_involved = 'NA'

                # Append to ball-by-ball records
                ball_records.append([
                    match_id, innings_number, over_number, ball_number, batter, bowler, 
                    non_striker, extra_type, batsman_run, extras_run, total_run, 
                    non_boundary, is_wicket, player_out, kind, fielders_involved, batting_team
                ])
                total_balls += 1  # Increment total processed balls

    print(f"Total balls processed for Match ID {match_id}: {total_balls}\n")


    return match_records, ball_records

# Main execution
match_data = []
ball_data = []

data_folder = "C:/Users/basup/OneDrive/Desktop/IPL_ML/ipl_data"
files_processed = 0

for file_name in os.listdir(data_folder):
    if file_name.endswith(".json"):
        file_path = os.path.join(data_folder, file_name)
        print(f"Processing file: {file_name}")
        match_id = os.path.splitext(file_name)[0]
        matches, balls = process_json(file_path, match_id)
        match_data.extend(matches)
        ball_data.extend(balls)
        files_processed += 1

print(f"\nTotal files processed: {files_processed}")
print(f"Total matches extracted: {len(match_data)}")
print(f"Total ball-by-ball records extracted: {len(ball_data)}\n")

# Save to CSV
match_df = pd.DataFrame(match_data, columns=[
    "ID", "City", "Date", "Season", "MatchNumber", "Team1", "Team2", "Venue",
    "TossWinner", "TossDecision", "SuperOver", "WinningTeam", "WonBy", "Margin",
    "Player_of_Match", "Team1Players", "Team2Players", "Umpire1", "Umpire2"
])
ball_df = pd.DataFrame(ball_data, columns=[
    "ID", "Innings", "Overs", "BallNumber", "Batter", "Bowler", "NonStriker",
    "ExtraType", "BatsmanRun", "ExtrasRun", "TotalRun", "NonBoundary", "IsWicketDelivery",
    "PlayerOut", "Kind", "FieldersInvolved", "BattingTeam"
])

match_df.to_csv(MATCHES_CSV, index=False)
ball_df.to_csv(BALL_BY_BALL_CSV, index=False)

print(f"✅ Processing complete! Match data saved to {MATCHES_CSV}, Ball-by-ball data saved to {BALL_BY_BALL_CSV}.")
