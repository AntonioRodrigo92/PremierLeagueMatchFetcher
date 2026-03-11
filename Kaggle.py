import os
import kaggle
import pandas as pd


def rename_teams(df, col_name):
    return df[col_name].replace({
        "Wolves": "Wolverhampton Wanderers",
        "Nottm Forest": "Nottingham Forest",
        "Man City": "Manchester City",
        "Man United": "Manchester United",
        "Leeds": "Leeds United",
        "Newcastle": "Newcastle United"
    })


class Kaggle:
    def __init__(self, league="Premier League", season="202526"):
        self.league = league
        self.season = season

    def get_stats(self, dataset_id):
        download_path = "./epl_data_2025"
        kaggle.api.dataset_download_files(dataset_id, path=download_path, unzip=True)
        file_path = os.path.join(download_path, "football_matches.csv")
        df = pd.read_csv(file_path)
        df = df[df['League'] == self.league]
        df['game_date_ts'] = pd.to_datetime(df['Date'])
        df['game_date'] = df['game_date_ts'].dt.strftime('%Y-%m-%d')
        df.columns = df.columns.str.lower()
        df['home_team'] = rename_teams(df, 'home_team')
        df['away_team'] = rename_teams(df, 'away_team')
        return df[[
            'game_date', 'home_team', 'away_team',
            'home_team_shots', 'away_team_shots', 'home_team_shotsontarget',
            'away_team_shotsontarget', 'home_team_possession', 'away_team_possession',
            'home_team_fouls', 'away_team_fouls', 'home_team_offside',
            'away_team_offside'
                ]]
