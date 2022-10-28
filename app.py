import click
import pandas as pd
import sqlite3


def connect_database():
    database = './database.sqlite'
    conn = sqlite3.connect(database)
    return conn


# build a click group
@click.group()
def cli():
    """A simple CLI to run different operations on dataset"""


# build a click command
@cli.command()
def describe():
    """Load the data and print all the tables in dataset"""
    conn = connect_database()
    tables = pd.read_sql("""SELECT *
                        FROM sqlite_master
                        WHERE type='table';""", conn)
    print(tables['name'].values)


@cli.command()
def find_countries():
    """Find all countries in the database"""
    conn = connect_database()
    countries = pd.read_sql("""SELECT *
                            FROM Country;""", conn)
    print(countries)


@cli.command()
def find_leagues():
    """Find all leagues in the database"""
    conn = connect_database()
    leagues = pd.read_sql("""SELECT Country.name, League.name
                            FROM League 
                            JOIN Country ON Country.id = League.country_id;""", conn)
    print(leagues)


@cli.command()
def show_teams():
    """Find first 10 teams in the database"""
    conn = connect_database()
    teams = pd.read_sql("""SELECT *
                            FROM Team
                            ORDER BY team_long_name
                            LIMIT 10;""", conn)
    print(teams)


@cli.command()
def show_matches():
    """Find first 10 detailed matches in the database"""
    conn = connect_database()
    detailed_matches = pd.read_sql("""SELECT Match.id, 
                                            Country.name AS country_name, 
                                            League.name AS league_name, 
                                            season, 
                                            stage, 
                                            date,
                                            HT.team_long_name AS  home_team,
                                            AT.team_long_name AS away_team,
                                            home_team_goal, 
                                            away_team_goal                                        
                                    FROM Match
                                    JOIN Country on Country.id = Match.country_id
                                    JOIN League on League.id = Match.league_id
                                    LEFT JOIN Team AS HT on HT.team_api_id = Match.home_team_api_id
                                    LEFT JOIN Team AS AT on AT.team_api_id = Match.away_team_api_id
                                    WHERE country_name = 'England'
                                    ORDER by date
                                    LIMIT 10;""", conn)
    print(detailed_matches)


@cli.command()
def season_info():
    """Find detailed information for each league in each season"""
    conn = connect_database()
    leages_by_season = pd.read_sql("""SELECT Country.name AS country_name, 
                                            League.name AS league_name, 
                                            season,
                                            count(distinct stage) AS number_of_stages,
                                            count(distinct HT.team_long_name) AS number_of_teams,
                                            avg(home_team_goal) AS avg_home_team_scors, 
                                            avg(away_team_goal) AS avg_away_team_goals, 
                                            avg(home_team_goal-away_team_goal) AS avg_goal_dif, 
                                            avg(home_team_goal+away_team_goal) AS avg_goals, 
                                            sum(home_team_goal+away_team_goal) AS total_goals                                       
                                    FROM Match
                                    JOIN Country on Country.id = Match.country_id
                                    JOIN League on League.id = Match.league_id
                                    LEFT JOIN Team AS HT on HT.team_api_id = Match.home_team_api_id
                                    LEFT JOIN Team AS AT on AT.team_api_id = Match.away_team_api_id
                                    WHERE country_name in ('Spain', 'Germany', 'France', 'Italy', 'England')
                                    GROUP BY Country.name, League.name, season
                                    HAVING count(distinct stage) > 10
                                    ORDER BY Country.name, League.name, season DESC
                                    ;""", conn)
    print(leages_by_season)


@cli.command()
@click.option("--q", default="SELECT * FROM Country;", help="Input your sql query")
def myquery(q):
    """Query the database with your own queries"""
    conn = connect_database()
    results = pd.read_sql(q, conn)
    print(results)


# run the CLI
if __name__ == "__main__":
    cli()