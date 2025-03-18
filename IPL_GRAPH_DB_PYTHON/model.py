from typing import Optional, List
from pydantic import BaseModel

class MatchData(BaseModel):
    match_id: int
    city: Optional[str] = None
    date: str
    season: int
    match_number: str
    team1: str
    team2: str
    venue: str
    toss_winner: Optional[str] = None
    toss_decision: Optional[str] = None
    super_over: str
    winning_team: Optional[str] = None
    won_by: Optional[str] = None
    margin: Optional[int] = None
    player_of_match: Optional[str] = None
    team1_players: Optional[List[str]] = None
    team2_players: Optional[List[str]] = None
    umpire1: Optional[str] = None
    umpire2: Optional[str] = None


class BallData(BaseModel):
    match_id: int
    innings: int
    over: int
    ball: int
    batter: str
    bowler: str
    non_striker: str
    extra_type: Optional[str] = None
    batsman_run: int
    extras_run: int
    total_run: int
    non_boundary: int
    is_wicket_delivery: int
    player_out: Optional[str] = None
    kind: Optional[str] = None
    fielders_involved: Optional[str] = None
    batting_team: str
