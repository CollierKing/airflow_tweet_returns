import tweepy
import pandas as pd
from airflow.hooks.base_hook import BaseHook
import os


class TwitterHook(BaseHook):
    def __init__(self):
        self.twtr_consumer_key = os.environ['twtr_consumer_key']
        self.twtr_consumer_secret = os.environ['twtr_consumer_secret']

        self.auth = tweepy.AppAuthHandler(
            self.twtr_consumer_key,
            self.twtr_consumer_secret
        )

    def get_usr_tweets(self, screen_name):
        api = tweepy.API(self.auth)

        # initialize a list to hold all the tweepy Tweets
        alltweets = []

        print(f"Grabbing user: {screen_name} tweets")
        # make initial request for most recent tweets (200 is the maximum allowed count)
        new_tweets = api.user_timeline(screen_name=screen_name, count=200)

        # save most recent tweets
        alltweets.extend(new_tweets)

        # save the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1

        # keep grabbing tweets until there are no tweets left to grab
        while len(new_tweets) > 0:
            print(f"getting tweets before {oldest}")

            # all subsiquent requests use the max_id param to prevent duplicates
            new_tweets = api.user_timeline(screen_name=screen_name, count=200, max_id=oldest)

            # save most recent tweets
            alltweets.extend(new_tweets)

            # update the id of the oldest tweet less one
            oldest = alltweets[-1].id - 1

            print(f"...{len(alltweets)} tweets downloaded so far")

        tweet_df = \
            (pd.DataFrame({
                "id": [x._json.get('id') for x in alltweets],
                "datetime": [x._json.get('created_at') for x in alltweets],
                "text": [x._json.get('text') for x in alltweets],
                "rt_count": [x._json.get('retweet_count') for x in alltweets],
                "fav_count": [x._json.get('favorite_count') for x in alltweets],
                "symbols": [x._json.get('entities').get('symbols') for x in alltweets],
                "status": ["ready"] * len(alltweets),
            }))

        print(f"len of tweets: {len(tweet_df)}")
        tweet_df = tweet_df[tweet_df['symbols'].astype(str).str.contains("text")].reset_index(drop=True)
        tweet_df['symbols_flat'] = \
            (tweet_df
             # .apply(lambdas x: flatten_dicts(x['symbols']), axis=1)
             .apply(lambda x: [d.get("text") for d in x['symbols']], axis=1)
             .astype(str)
             .str.replace("[", "")
             .str.replace("]", "")
             .str.replace("'", "")
             )
        # tweet_df['datetime'] = \
        #     tweet_df['datetime'].astype(str).str[:19]
        # tweet_df['start_epoch'] = \
        #     pd.to_datetime(tweet_df['datetime']).dt.strftime('%s')
        tweet_df.drop(["symbols"], axis=1, inplace=True)
        tweet_df.rename(columns={"symbols_flat": "symbols"}, inplace=True)
        tweet_df = tweet_df.astype(str).tail(200)

        return tweet_df
