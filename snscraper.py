# Package importing
import snscrape.modules.twitter as sns
import pandas as pd

# Saved data folder
PICKLE_FOLDER = 'tweets_pickle'


def get_tweets(username: str):
    """
    :param username: Twitter account name to searched in archive of tweets.
    :return: None. Procedure, that saves retrieved tweets data in pickle format.
    """

    # Creating list to append tweet data to
    tweets = []

    for tweet in sns.TwitterSearchScraper(username).get_items():
        tweets.append([tweet.date, tweet.id, tweet.content, tweet.user.id, tweet.user.username, tweet.user.created,
                       tweet.user.favouritesCount, tweet.user.followersCount, tweet.user.friendsCount,
                       tweet.user.location, tweet.user.verified, tweet.hashtags, tweet.lang, tweet.likeCount,
                       tweet.mentionedUsers, tweet.sourceLabel, tweet.quoteCount, tweet.replyCount, tweet.retweetCount,
                       tweet.url, tweet.media, tweet.inReplyToUser, tweet.place])

    # Creating a dataframe from the tweets list above
    df = pd.DataFrame(tweets, columns=['tweet_created_at', 'tweet_id', 'text', 'user_id', 'user_name', 'user_created_at',
                                       'user_favourites_count', 'user_followers_count', 'user_friends_count',
                                       'user_location', 'user_is_verified', 'hashtags', 'lang', 'tweet_like_count',
                                       'mentioned_users', 'source_label', 'tweet_quote_count', 'tweet_reply_count',
                                       'retweet_count', 'tweet_url', 'media', 'in_reply_to_user', 'place'])
    df.astype({'lang': 'category', 'source_label': 'category'})

    # Saving retrieved data in pickle file
    df.to_pickle(path=f'{PICKLE_FOLDER}/pickle_data_{username}.pkl')
