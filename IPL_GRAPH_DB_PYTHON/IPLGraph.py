from typing import Dict
from neo4j import Transaction
from model import MatchData, BallData

class IPLGraph:
    
    @staticmethod
    def _create_match_nodes(tx: Transaction, match: MatchData):
        """Creates match nodes and relationships for the given match."""
        tx.run("""
        MERGE (season:Season {year: $season})
        MERGE (venue:Venue {name: $venue})
        MERGE (city:City {name: $city})
        MERGE (venue)-[:LOCATED_IN]->(city)
        
        MERGE (umpire1:Umpire {name: $umpire1})
        MERGE (umpire2:Umpire {name: $umpire2})
        
        MERGE (match:Match {id: $match_id})
        SET match.date = $date,
            match.match_number = $match_number,
            match.toss_decision = $toss_decision,
            match.super_over = $super_over,
            match.won_by = $won_by,
            match.margin = $margin
        
        MERGE (match)-[:HELD_AT]->(venue)
        MERGE (match)-[:PART_OF_SEASON]->(season)
        MERGE (match)-[:UMPIRED_BY]->(umpire1)
        MERGE (match)-[:UMPIRED_BY]->(umpire2)
        """, **match.model_dump())

    @staticmethod
    def _create_team_relationships(tx: Transaction, match: MatchData):
        """Links teams to the match and establishes 'played against' relationships."""
        tx.run("""
        MERGE (team1:Team {name: $team1})
        MERGE (team2:Team {name: $team2})
        WITH team1, team2
        MATCH (match:Match {id: $match_id})
        MERGE (match)-[:INVOLVES_TEAM]->(team1)
        MERGE (match)-[:INVOLVES_TEAM]->(team2)
        MERGE (team1)-[:PLAYED_AGAINST {match_id: $match_id}]->(team2)
        """, **match.model_dump())

    @staticmethod
    def _create_player_relationships(tx: Transaction, match: MatchData):
        """Creates player nodes and associates them with teams and matches."""
        for player in match.team1_players or []:
            tx.run("""
            MATCH (match:Match {id: $match_id})-[:INVOLVES_TEAM]->(team:Team {name: $team1})
            MERGE (player:Player {name: $player})
            MERGE (player)-[:PLAYED_IN {match_id: $match_id}]->(match)
            MERGE (player)-[:MEMBER_OF]->(team)
            """, match_id=match.match_id, team1=match.team1, player=player)

        for player in match.team2_players or []:
            tx.run("""
            MATCH (match:Match {id: $match_id})-[:INVOLVES_TEAM]->(team:Team {name: $team2})
            MERGE (player:Player {name: $player})
            MERGE (player)-[:PLAYED_IN {match_id: $match_id}]->(match)
            MERGE (player)-[:MEMBER_OF]->(team)
            """, match_id=match.match_id, team2=match.team2, player=player)

        # Player of the Match
        if match.player_of_match:
            tx.run("""
            MATCH (match:Match {id: $match_id})
            MERGE (player:Player {name: $player_of_match})
            MERGE (player)-[:PLAYER_OF_MATCH]->(match)
            """, match_id=match.match_id, player_of_match=match.player_of_match)

    @staticmethod
    def _create_match_outcomes(tx: Transaction, match: MatchData):
        """Creates relationships for match outcomes (winning and losing teams)."""
        if match.winning_team:
            losing_team = match.team2 if match.winning_team == match.team1 else match.team1
            tx.run("""
            MATCH (match:Match {id: $match_id})
            MATCH (winning_team:Team {name: $winning_team})
            MATCH (losing_team:Team {name: $losing_team})
            MERGE (winning_team)-[:WON]->(match)
            MERGE (losing_team)-[:LOST]->(match)
            """, match_id=match.match_id, winning_team=match.winning_team, losing_team=losing_team)

    @staticmethod
    def _create_delivery_nodes(tx: Transaction, delivery: BallData):
        """Ensures deliveries are connected to Innings and Match properly."""
        tx.run("""
        MATCH (match:Match {id: $match_id})
        MERGE (innings:Innings {match_id: $match_id, number: $innings})
        MERGE (match)-[:HAS_INNING]->(innings)
        
        MERGE (delivery:Delivery {
            match_id: $match_id,
            innings: $innings,
            over: $over,
            ball: $ball
        })
        SET delivery.batter = $batter,
            delivery.bowler = $bowler,
            delivery.non_striker = $non_striker,
            delivery.extra_type = $extra_type,
            delivery.batsman_run = $batsman_run,
            delivery.extras_run = $extras_run,
            delivery.total_run = $total_run,
            delivery.non_boundary = $non_boundary,
            delivery.batting_team = $batting_team

        MERGE (innings)-[:HAS_DELIVERY]->(delivery)
        MERGE (batter:Player {name: $batter})-[:BATTED_IN]->(delivery)
        MERGE (bowler:Player {name: $bowler})-[:BOWLED_IN]->(delivery)
        MERGE (non_striker:Player {name: $non_striker})-[:WAS_NON_STRIKER_IN]->(delivery)
        """, **delivery.model_dump())


    @staticmethod
    def _create_wicket_relationships(tx: Transaction, delivery: BallData):
        """Ensures wickets are connected properly."""
        if delivery.is_wicket_delivery:
            tx.run("""
            MATCH (delivery:Delivery {
                match_id: $match_id,
                innings: $innings,
                over: $over,
                ball: $ball
            })
            MERGE (wicket:Wicket {
                player_out: $player_out,
                type: $kind
            })
            SET wicket.fielders_involved = $fielders_involved
            
            MERGE (delivery)-[:RESULTED_IN]->(wicket)
            MERGE (dismissed:Player {name: $player_out})
            MERGE (wicket)-[:DISMISSED]->(dismissed)
            """, **delivery.model_dump())


    @staticmethod
    def _update_player_stats(tx: Transaction, delivery: BallData):
        """Updates player statistics based on deliveries played and bowled."""
        tx.run("""
        MATCH (player:Player {name: $batter})
        SET player.runs_scored = COALESCE(player.runs_scored, 0) + $batsman_run,
            player.balls_faced = COALESCE(player.balls_faced, 0) + 1,
            player.strike_rate = (1.0 * player.runs_scored / player.balls_faced) * 100
        WITH player 
               
        MATCH (player:Player {name: $bowler})
        SET player.balls_bowled = COALESCE(player.balls_bowled, 0) + 1,
            player.wickets = COALESCE(player.wickets, 0) + CASE WHEN $is_wicket_delivery = 1 THEN 1 ELSE 0 END,
            player.economy_rate = (1.0 * COALESCE(player.runs_conceded, 0) / player.balls_bowled) * 6
        """, **delivery.model_dump())
