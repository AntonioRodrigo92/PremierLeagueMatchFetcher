import requests
import pandas as pd


class FootballDataOrg:
    def __init__(self, api_key):
        self.base_url = "https://api.football-data.org/v4/competitions/PL/matches"
        self.headers = {'X-Auth-Token': api_key}

    def get_upcoming_matches_as_df(self):
        # status=SCHEDULED retrieves only unplayed matches
        params = {'status': 'SCHEDULED'}

        response = requests.get(self.base_url, headers=self.headers, params=params)
        response.raise_for_status()
        data = response.json()
        df = pd.json_normalize(data.get('matches', []))
        filtered_df = filter_min_matchday_games(df)
        selected_columns_df = select_columns_dataframe(filtered_df)
        cleaned_dataframe = clean_dataframe(selected_columns_df)
        corrected_team_name_df = correct_team_name(cleaned_dataframe)
        return corrected_team_name_df


def select_columns_dataframe(df):
    cols_to_keep = {
        'utcDate': 'game_datetime',
        'matchday': 'matchday',
        'season.startDate': 'season_start_date',
        'season.endDate': 'season_end_date',
        'homeTeam.name': 'home_team',
        'awayTeam.name': 'away_team'
    }
    return df[list(cols_to_keep.keys())].rename(columns=cols_to_keep)


def clean_dataframe(df):
    season_start = df['season_start_date'].iloc[0][0:4]
    season_end = df['season_end_date'].iloc[0][0:4]
    season_lit = f"{season_start}-{season_end}"
    df['season'] = season_lit
    df['game_date'] = pd.to_datetime(df['game_datetime']).dt.date
    df['game_time'] = pd.to_datetime(df['game_datetime']).dt.time
    df['game_datetime'] = pd.to_datetime(df['game_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
    return df[['season', 'game_datetime', 'game_date', 'game_time', 'home_team', 'away_team']]


def filter_min_matchday_games(df):
    min_matchday = df['matchday'].iloc[0]
    return df[(df['matchday'] == min_matchday)]


def correct_team_name(df):
    df['home_team'] = rename_teams(df, 'home_team')
    df['away_team'] = rename_teams(df, 'away_team')
    return df


def rename_teams(df, col_name):
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
        "AFC Bournemouth": "Bournemouth",
        "Aston Villa FC": "Aston Villa",
        "Brighton & Hove Albion FC": "Brighton",
        "Everton FC": "Everton",
        "Fulham FC": "Fulham",
        "Leeds United FC": "Leeds",
        "Manchester City FC": "Manchester City",
        "Newcastle United FC": "Newcastle United",
        "Tottenham Hotspur FC": "Tottenham",
        "Wolverhampton Wanderers FC": "Wolverhampton Wanderers"
    })