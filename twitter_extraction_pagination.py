# Package importing
import pandas.core.frame
import requests
from backports import configparser
import pandas as pd
import logging
import datetime

# Logger configuration
LOGGER_PATH = r'error_logger.txt'
logging.basicConfig(filename=LOGGER_PATH, level=logging.INFO)

# Saved data folder
PICKLE_FOLDER = 'tweets_pickle'

# Twitter API documentation: https://developer.twitter.com/en/docs/twitter-api

# Variables initializing and definition
CONFIG = configparser.RawConfigParser()
CONFIG.read('config.ini')

BEARER_TOKEN = CONFIG['twitter']['BEARER_TOKEN']


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
        self.next_token = None
        self.dataframes_list = []

    def paginate(self) -> pandas.core.frame.DataFrame:
        """
        :return: Pandas DataFrame with required tweets
        """

        self.params['pagination_token'] = self.next_token
        response = requests.request("GET", self.url, params=self.params, headers=self.headers) \
            .json()

        try:
            self.next_token = response['meta']['next_token']
        except AttributeError:
            self.next_token = 'closed'
            self.params.pop('pagination_token')

        response = response['data']
        df = pd.DataFrame(response)

        return df

    def get_tweets(self):
        """
        :return: None. Operator that retrieves all tweets, mentioning specified user.
        """

        try:
            response = requests.request("GET", self.url, params=self.params, headers=self.headers) \
                .json()

            self.next_token = response['meta']['next_token']

            response = response['data']
            df0 = pd.DataFrame(response)

            self.dataframes_list.append(df0)

            while self.next_token != 'closed':
                df_p = self.paginate()
                self.dataframes_list.append(df_p)
        except Exception as err:
            error_time = datetime.datetime.now()
            logging.error(f'In: {str(error_time)} raised: {str(err)};')

        df = pd.concat(self.dataframes_list, axis=0)

        df.to_pickle(path=f'{PICKLE_FOLDER}/pickle_data_{self.user_id}.pkl')
