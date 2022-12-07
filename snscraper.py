# Package importing
import snscrape.modules.twitter as sns
import pandas as pd

# Saved data folder
PARQUET_FOLDER = 'tweets_parquet'


def get_tweets(username: str):
    """
    :param username: Twitter account name to searched in archive of tweets.
    :return: None. Procedure, that saves retrieved tweets data in database-oriented format (Apache Parquet).
    """

    # Creating list to append tweet data to
    tweets = []

    for tweet in sns.TwitterSearchScraper(username).get_items():
        tweets.append([tweet.date, tweet.id, tweet.content, tweet.user.username, tweet.hashtags, tweet.lang,
                       tweet.likeCount, tweet.mentionedUsers, tweet.source, tweet.sourceUrl, tweet.sourceLabel,
                       tweet.quoteCount, tweet.replyCount, tweet.retweetCount, tweet.url, tweet.media,
                       tweet.inReplyToUser, tweet.place])

    # Creating a dataframe from the tweets list above
    df = pd.DataFrame(tweets, columns=['created_at', 'id', 'text', 'username', 'hashtags', 'lang', 'like_count',
                                       'mentioned_users', 'source', 'source_url',
                                       'source_label', 'quote_count', 'reply_count', 'retweet_count',
                                       'tweet_url', 'media', 'in_reply_to_user', 'place'])
    df.astype({'created_at': 'datetime64', 'id': 'int64', 'lang': 'category', 'like_count': 'int16',
               'source': 'category', 'source_url': 'object','source_label': 'object', 'quote_count': 'int16',
               'reply_count': 'int16', 'retweet_count': 'int16', 'tweet_url': 'object', 'media': 'object',
               'in_reply_to_user': 'object', 'place': 'object'})

    # Saving retrieved data in Apache Parquet file
    df.to_parquet(path=f'{PARQUET_FOLDER}/parquet_data_{username}.parquet', engine='fastparquet')
