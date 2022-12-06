import twitter_extraction as te

user_id_dict = {
    '@stepstone_de': 47629619,
    '@TotaljobsUK': 34282316
}


def main():
    """
    :return:
    """
    stepstone_tweets = te.TwitterHook(user_id=user_id_dict.get('@stepstone_de'))
    stepstone_tweets.get_tweets()

    totaljobs_tweets = te.TwitterHook(user_id=user_id_dict.get('@TotaljobsUK'))
    totaljobs_tweets.get_tweets()


if __name__ == '__main__':
    main()
