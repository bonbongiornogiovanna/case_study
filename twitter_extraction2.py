# Package importing
import pandas.core.frame
import requests
from backports import configparser
import json
import pandas as pd
import logging
import datetime

# Logger configuration
LOGGER_PATH = r'error_logger.txt'
logging.basicConfig(filename=LOGGER_PATH, level=logging.INFO)

# Saved data folders
JSON_FOLDER = 'tweets_json'
PARQUET_FOLDER = 'tweets_parquet'

# Twitter API documentation: https://developer.twitter.com/en/docs/twitter-api

# Variables initializing and definition
CONFIG = configparser.RawConfigParser()
CONFIG.read('config.ini')

BEARER_TOKEN = CONFIG['twitter']['BEARER_TOKEN']


def transform_dataframe(df) -> pandas.core.frame.DataFrame:
    """
    :param df: DataFrame to be transformed
    :return: Clean and optimized Pandas DataFrame
    """
    edit_history_tweet_ids = df['edit_history_tweet_ids'].explode()
    referenced_tweets = df['referenced_tweets'] \
        .explode() \
        .apply(pd.Series) \
        .drop([0, 'id'], axis=1)
    public_metrics = df['public_metrics'].apply(pd.Series)
    entities = df['entities'].apply(pd.Series)
    mentions = entities['mentions'] \
        .explode() \
        .apply(pd.Series) \
        .loc[:, 'username']
    urls = entities['urls'] \
        .explode() \
        .apply(pd.Series) \
        .loc[:, 'url']

    attachments = df.attachments.apply(pd.notnull)
    tweet_len = df.text\
        .apply(len)\
        .rename('tweet_length')

    df = df.drop(['entities', 'public_metrics', 'referenced_tweets', 'attachments',
                  'edit_history_tweet_ids', 'id', 'conversation_id'], axis=1)
    df = pd.concat(
        [df, referenced_tweets, public_metrics, mentions, urls, attachments, edit_history_tweet_ids, tweet_len],
        axis=1)

    df['lang'] = df['lang'].astype('category')
    df['source'] = df['source'].astype('category')
    df['edit_history_tweet_ids'] = df['edit_history_tweet_ids'].astype('int64')

    return df


class TwitterHook:
    """
    Class for Twitter API connection.
    """

    def __init__(self, user_id: int):
        """
        Initializes tweets extraction object for further data retrieval.
        :param user_id: User ID to be searched, which was mentioned in tweets.
                        Do not confuse with Twitter account name.
        """

        self.user_id = user_id
        self.url = f'https://api.twitter.com/2/tweets/search/all'
        self.params = {'tweet.fields': 'attachments,author_id,context_annotations,conversation_id,'
                                       'created_at,entities,geo,id,in_reply_to_user_id,'
                                       'lang,public_metrics,referenced_tweets,source,text,withheld',
                       'expansions': 'attachments.media_keys,author_id,entities.mentions.username,geo.place_id,'
                                     'in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id',
                       'place.fields': 'contained_within,country,country_code,full_name,geo,id,name,place_type',
                       'user.fields': 'created_at,description,entities,id,location,name,'
                                      'pinned_tweet_id,public_metrics,url,username,verified,withheld',
                       'max_results': '100',
                       }
        self.headers = {'Authorization': f'Bearer {BEARER_TOKEN}'}

    def get_tweets_by_user_ids(self):
        """
        :return: None. Operator that retrieves all tweets, mentioning specified user.
        """

        try:
            url = self.url
            params = self.params
            headers = self.headers

            response = requests.request("GET", url, params=params, headers=headers) \
                .json()
            #try:
            #    next_token = response['meta']['next_token']
            #except Exception as err:
            #    print(err)
            #    next_token = None

            response = response['data']

            with open(f'{JSON_FOLDER}/raw_data_{self.user_id}.json', 'w+') as outfile:
                json.dump(response, outfile)

            df = pd.DataFrame(response)
            df = transform_dataframe(df)

            df.to_parquet(path=f'{PARQUET_FOLDER}/parquet_data_{self.user_id}.parquet', engine='fastparquet')
            # TODO: Delete excel writer
            df.to_excel(excel_writer=f'{PARQUET_FOLDER}/excel_data_{self.user_id}.xlsx')

        except Exception as err:
            errtime = datetime.datetime.now()
            logging.error(f'In: {str(errtime)} raised: {str(err)};')
