import os
import kaggle
import numpy as np
import pandas as pd


class SquadValue:
    season_timeframe = None

    def __init__(self, games_df):
        self.season_timeframe = _get_season_timeframe(games_df)

    def get_squad_value(self, dataset_id):
        download_path = "./player_valuations"
        kaggle.api.dataset_download_files(dataset_id, path=download_path, unzip=True)
        file_path = os.path.join(download_path, "player_valuations.csv")
        df = pd.read_csv(file_path)
        df_with_season = _add_season_to_dataframe(self, df)
        squad_value_per_season = _get_squad_value_per_season(df_with_season)
        squad_value_per_season['team'] = _rename_team_names(squad_value_per_season, 'team')
        return squad_value_per_season


def _get_season_timeframe(games_df):
    return games_df.groupby('season')['game_datetime'].agg(
        game_datetime_min='min',
        game_datetime_max='max'
    ).reset_index()


def _add_season_to_dataframe(self, player_valuation_df):
    self.season_timeframe['start'] = pd.to_datetime(self.season_timeframe['game_datetime_min'])
    self.season_timeframe['end'] = pd.to_datetime(self.season_timeframe['game_datetime_max'])
    player_valuation_df['date'] = pd.to_datetime(player_valuation_df['date'])

    self.season_timeframe = self.season_timeframe.sort_values('start')
    player_valuation_df = player_valuation_df.sort_values('date')

    result_df = pd.merge_asof(player_valuation_df, self.season_timeframe,
                              left_on='date', right_on='start')
    return result_df


def _get_squad_value_per_season(df_with_season):
    # just 1 valuation per player / per team / per season
    max_valuation_per_player_per_team_per_season_df = df_with_season\
        .groupby(['season', 'current_club_name', 'player_id'])\
        ['market_value_in_eur'].max().reset_index()

    # group by season / team => top 23 most valuable players
    top_23_player_valuations_per_team_per_season_df = max_valuation_per_player_per_team_per_season_df\
        .groupby(['season', 'current_club_name'])['market_value_in_eur']\
        .apply(lambda x: x.nlargest(23).values).reset_index()

    # media da lista anterior
    mean_player_valuation_per_team_per_season = top_23_player_valuations_per_team_per_season_df
    mean_player_valuation_per_team_per_season['avg_squad_value'] = mean_player_valuation_per_team_per_season['market_value_in_eur']\
        .apply(np.mean)
    mean_player_valuation_per_team_per_season['team'] = mean_player_valuation_per_team_per_season['current_club_name']
    mean_player_valuation_per_team_per_season = mean_player_valuation_per_team_per_season[['season', 'team', 'avg_squad_value']]

    return mean_player_valuation_per_team_per_season


def _rename_team_names(df, col_name):
    return df[col_name].replace({
        "Arsenal FC": "Arsenal",
        "Brentford FC": "Brentford",
        "Burnley FC": "Burnley",
        "Chelsea FC": "Chelsea",
        "Crystal Palace FC": "Crystal Palace",
        "Liverpool FC": "Liverpool",
        "Manchester United FC": "Manchester United",
        "Nottingham Forest FC": "Nottingham Forest",
        "Sunderland AFC": "Sunderland",
        "West Ham United FC": "West Ham",
        "West Ham United": "West Ham",
        "AFC Bournemouth": "Bournemouth",
        "Aston Villa FC": "Aston Villa",
        "Brighton & Hove Albion FC": "Brighton",
        "Brighton & Hove Albion": "Brighton",
        "Everton FC": "Everton",
        "Fulham FC": "Fulham",
        "Leeds United FC": "Leeds",
        "Leeds United": "Leeds",
        "Manchester City FC": "Manchester City",
        "Newcastle United FC": "Newcastle United",
        "Tottenham Hotspur FC": "Tottenham",
        "Tottenham Hotspur": "Tottenham",
        "Wolverhampton Wanderers FC": "Wolverhampton Wanderers",
        "Swansea City": "Swansea",
        "Leicester City": "Leicester",
        "Hull City": "Hull",
        "Norwich City": "Norwich",
        "Watford FC": "Watford",
        "Middlesbrough FC": "Middlesbrough",
        "Huddersfield Town": "Huddersfield",
        "Cardiff City": "Cardiff",
        "Luton Town": "Luton",
        "Ipswich Town": "Ipswich",
        "Southampton FC": "Southampton",
        "Stoke City": "Stoke"
    })
