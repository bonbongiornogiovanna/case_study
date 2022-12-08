# Importing custom made data operator packages
import twitter_extraction_cursor_loop as te
import snscraper

user_id_dict = {
    '@stepstone_de': 47629619,
    '@TotaljobsUK': 34282316
}


def main():
    """
    :return: None. Procedure that starts data retrieval operators.
    """
    stepstone_tweets = te.TwitterHook(user_id=user_id_dict.get('@stepstone_de'))
    stepstone_tweets.get_tweets()

    totaljobs_tweets = te.TwitterHook(user_id=user_id_dict.get('@TotaljobsUK'))
    totaljobs_tweets.get_tweets()

    for username in user_id_dict.keys():
        snscraper.get_tweets(username)


if __name__ == '__main__':
    main()
