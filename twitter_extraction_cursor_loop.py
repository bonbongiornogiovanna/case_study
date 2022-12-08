# Package importing
import requests
from backports import configparser
import json
import pandas as pd
import logging
import datetime

# Logger configuration
LOGGER_PATH = r'error_logger.txt'
logging.basicConfig(filename=LOGGER_PATH, level=logging.INFO)

# Saved data folder
JSON_FOLDER = 'tweets_json'
PICKLE_FOLDER = 'tweets_pickle'

# Twitter API documentation: https://developer.twitter.com/en/docs/twitter-api

# Variables initializing and definition
CONFIG = configparser.RawConfigParser()
CONFIG.read('config.ini')

BEARER_TOKEN = CONFIG['twitter']['BEARER_TOKEN']


def request_loop(url: str, params: dict, headers: dict) -> tuple:
    """
    :param url: Request url.
    :param params: Request parameters in Python dictionary format.
    :param headers: Authentication headers with bearer token.
    :return: Tuple pair: (concatenated tweet search in JSON, Pandas DataFrame of this data)
    """

    tweet_id_cursor = None
    json_collect = None

    try:
        while True:
            response = requests.request("GET", url, params=params, headers=headers) \
                .json()
            response = response['data']

            if json_collect is None:
                json_collect = response
            else:
                print('Concatenating JSONs!')

            if tweet_id_cursor is not None:
                json_collect.extend(response)
            else:
                print(r'First request is finished!')

            df = pd.DataFrame(response) \
                .astype({'id': 'int64'}) \
                .set_index('id')
            tweet_id_cursor = min(df.index) - 1
            params['until_id'] = tweet_id_cursor
    except Exception as err:
        print(err)
        print(r'You have reached the end of the archive!')

        df = pd.json_normalize(json_collect)
        return json_collect, df


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
        self.url = f'https://api.twitter.com/2/users/{self.user_id}/mentions?'
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
        self.tweet_id_cursor = None
        self.json_collect = None

    def get_tweets(self):
        """
        :return: None. Operator that retrieves all tweets, mentioning specified user.
        Saves data as a pickle file.
        """

        try:
            json_data, df = request_loop(url=self.url, params=self.params, headers=self.headers)

            with open(f'{JSON_FOLDER}/raw_data_{self.user_id}.json', 'w+') as outfile:
                json.dump(json_data, outfile)

            df.to_pickle(path=f'{PICKLE_FOLDER}/pickle_data_{self.user_id}.pkl')
        except Exception as err:
            error_time = datetime.datetime.now()
            logging.error(f'In: {str(error_time)} raised: {str(err)};')
