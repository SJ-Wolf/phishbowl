import sqlite3
import re
import pandas as pd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1500)

BATTLETAG_RE = re.compile(r'\b[\w!]+\s?#\d+')


class ResponseColumns:
    TIMESTAMP = 'Timestamp'
    TEAM_CAPTAIN_DISCORD = 'Team Captain Discord'
    TEAM_CAPTAIN_BATTLETAG = 'Team Captain Battletag '
    TEAM_NAME = 'Team Name'
    AVERAGE_SR = 'Average Team SR (can be a rough estimate)'
    STARTER_BATTLETAGS = 'Starting Members Battletags (names with numbers)'
    SUB_BATTLETAGS = 'Substitutes Battletags (max of 2)'


def get_battletags_from_row(row):
    battletags = []
    battletags += BATTLETAG_RE.findall(row[ResponseColumns.TEAM_CAPTAIN_BATTLETAG])
    battletags += BATTLETAG_RE.findall(row[ResponseColumns.STARTER_BATTLETAGS])
    battletags += BATTLETAG_RE.findall(row[ResponseColumns.SUB_BATTLETAGS])
    return battletags


def clean_all_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings = lambda x: x.strip().lower() if isinstance(x, str) else x
    return df.applymap(trim_strings)


def main():
    df = pd.read_csv('responses.tsv', '\t')
    df = clean_all_columns(df)
    df[ResponseColumns.TEAM_CAPTAIN_BATTLETAG].fillna("", inplace=True)
    df[ResponseColumns.TEAM_CAPTAIN_BATTLETAG] = df[ResponseColumns.TEAM_CAPTAIN_BATTLETAG].apply(
        lambda x: x.replace('\n', ', '))
    df[ResponseColumns.STARTER_BATTLETAGS].fillna("", inplace=True)
    df[ResponseColumns.STARTER_BATTLETAGS] = df[ResponseColumns.STARTER_BATTLETAGS].apply(
        lambda x: x.replace('\n', ', '))
    df[ResponseColumns.SUB_BATTLETAGS].fillna("", inplace=True)
    df[ResponseColumns.SUB_BATTLETAGS] = df[ResponseColumns.STARTER_BATTLETAGS].apply(lambda x: x.replace('\n', ', '))
    battletags_data = []
    for idx, row in df.iterrows():
        battletags = get_battletags_from_row(row)
        for btag in battletags:
            d = {'battletag': btag,
                 'response_id': idx}
            battletags_data.append(d)

    with sqlite3.connect('responses.db') as db:
        df.to_sql('response', db, if_exists='replace', index_label='id')
        df.from_dict(battletags_data).to_sql('battletag', db, if_exists='replace', index=False)


if __name__ == '__main__':
    main()
