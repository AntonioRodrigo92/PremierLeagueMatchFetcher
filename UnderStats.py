import asyncio
import pandas as pd
import aiohttp
from understat import Understat


class UnderstatStats:
    def __init__(self, league="EPL", season=2025):
        self.league = league
        self.season = season
        self.df = None

    async def fetch_data(self):
        """Fetches raw match data from Understat."""
        async with aiohttp.ClientSession() as session:
            understat = Understat(session)
            matches = await understat.get_league_results(
                league_name=self.league,
                season=self.season
            )
            self.df = pd.DataFrame(matches)
            return self.df

    def _extract_field(self, col_name, key):
        """Helper to safely extract keys from dicts or strings."""

        def getter(x):
            if isinstance(x, dict):
                return x.get(key)
            return None  # Handle cases where data might be missing

        return self.df[col_name].apply(getter)

    def process_data(self):
        """Cleans and formats the dataframe."""
        if self.df is None:
            raise ValueError("Dataframe is empty. Run fetch_data() first.")

        # Extract nested values
        self.df['home_team'] = self._extract_field('h', 'title')
        self.df['away_team'] = self._extract_field('a', 'title')
        self.df['home_team_goals'] = self._extract_field('goals', 'h')
        self.df['away_team_goals'] = self._extract_field('goals', 'a')
        self.df['home_team_xg'] = self._extract_field('xG', 'h')
        self.df['away_team_xg'] = self._extract_field('xG', 'a')

        # Format Dates
        self.df = self.df.rename(columns={'datetime': 'game_datetime'})
        self.df['game_datetime'] = pd.to_datetime(self.df['game_datetime'])
        self.df['game_date'] = self.df['game_datetime'].dt.date.astype(str)
        self.df['game_time'] = self.df['game_datetime'].dt.time.astype(str)

        # Filter and reorder columns
        cols = [
            "game_datetime", "game_date", "game_time",
            "home_team", "away_team", "home_team_goals",
            "away_team_goals", "home_team_xg", "away_team_xg"
        ]
        self.df = self.df[cols]
        return self.df

    def get_stats(self):
        """Convenience method to run the full pipeline."""
        loop = asyncio.get_event_loop()
        self.df = loop.run_until_complete(self.fetch_data())
        return self.process_data()

    def list_to_dataframe(understat_cleaned_df_list):
        return pd.concat(understat_cleaned_df_list, ignore_index=True)