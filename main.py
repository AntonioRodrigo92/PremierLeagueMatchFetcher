import os
import json
import sys

import pandas as pd

from FootballDataOrg import FootballDataOrg
from UnderStats import UnderstatStats
from WriteToMariaDB import WriteToMariaDB


def get_config_file(path):
    with open(path, 'r') as config_file:
        return json.load(config_file)


def get_undertstat_df(configuration):
    understat_config = configuration.get('understat')
    understat_leagues = understat_config.get('leagues')

    understat_cleaned_df_list = []
    for understat_league in understat_leagues:
        understat_league_name = understat_league.get('league')
        seasons = understat_league.get('seasons')

        for season in seasons:
            current_season = f"{season}-{season + 1}"
            understat_scraper = UnderstatStats(league=understat_league_name, season=season)
            understat_df = understat_scraper.get_stats()
            understat_df.insert(0, "season", current_season)
            understat_cleaned_df_list.append(understat_df)

    return UnderstatStats.list_to_dataframe(understat_cleaned_df_list)


def get_kaggle_df(configuration):
    kaggle_config = configuration.get('kaggle')
    kaggle_user = kaggle_config.get('username')
    kaggle_key = kaggle_config.get('key')
    dataset_id = kaggle_config.get('dataset_id')
    os.environ['KAGGLE_USERNAME'] = kaggle_user
    os.environ['KAGGLE_KEY'] = kaggle_key

    from Kaggle import Kaggle

    kaggle_scraper = Kaggle(league="Premier League", season="202526")
    return kaggle_scraper.get_stats(dataset_id)


def get_squad_value_df(configuration, understats_df):
    kaggle_config = configuration.get('kaggle')
    kaggle_user = kaggle_config.get('username')
    kaggle_key = kaggle_config.get('key')
    dataset_id = kaggle_config.get('player_val_dataset_id')
    os.environ['KAGGLE_USERNAME'] = kaggle_user
    os.environ['KAGGLE_KEY'] = kaggle_key

    from SquadValue import SquadValue

    squad_value = SquadValue(understats_df)
    return squad_value.get_squad_value(dataset_id)


def played_games(configuration, database_writer):
    understat_cleaned_df = get_undertstat_df(configuration)
    kaggle_cleaned_df = get_kaggle_df(configuration)
    squad_value_df = get_squad_value_df(configuration, understat_cleaned_df)

    match_data = pd.merge(understat_cleaned_df, kaggle_cleaned_df,
                          on=['game_date', 'home_team', 'away_team'],
                          how='left')

    # join do squad_value_df com match_data para HOME_TEAM
    match_data = pd.merge(match_data, squad_value_df,
                          left_on=['season', 'home_team'], right_on=['season', 'team'],
                          how='left')\
        .drop('team', axis=1)\
        .rename(columns={'avg_squad_value': 'home_team_avg_squad_value'})

    # join do squad_value_df com match_data para AWAY_TEAM
    match_data = pd.merge(match_data, squad_value_df,
                          left_on=['season', 'away_team'], right_on=['season', 'team'],
                          how='left')\
        .drop('team', axis=1)\
        .rename(columns={'avg_squad_value': 'away_team_avg_squad_value'})

    database_writer.write_dataframe(match_data, 'match_data')
    database_writer.write_dataframe(sorted(squad_value_df['team'].unique()), 'squad_value')

    # match_data.to_csv("C:\\Users\\Antonio\\PycharmProjects\\PremierLeagueMatchFetcher\\resources\\out.csv", index=False)


def unplayed_games(configuration, database_writer):
    football_data_org_config = configuration.get('football_data_org')
    api_key = football_data_org_config.get('api_key')

    football_data_org_scrapper = FootballDataOrg(api_key)
    unplayed_games_df = football_data_org_scrapper.get_upcoming_matches_as_df()

    database_writer.write_dataframe(unplayed_games_df, 'unplayed_games')


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    config = get_config_file('C:\\Users\\Antonio\\PycharmProjects\\PremierLeagueMatchFetcher\\resources\\application.conf')
    # config = get_config_file(sys.argv[1])

    mariadb_config = config.get('maria_db')
    db_writer = WriteToMariaDB(
        mariadb_config.get('user'),
        mariadb_config.get('password'),
        mariadb_config.get('hostname'),
        mariadb_config.get('port'),
        mariadb_config.get('database')
    )

    played_games(config, db_writer)
    unplayed_games(config, db_writer)
