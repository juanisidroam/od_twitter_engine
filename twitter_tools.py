from pathlib import Path

from pandas import json_normalize
from searchtweets import ResultStream
from searchtweets import gen_rule_payload as payload_rule
from searchtweets import load_credentials as lc

from general_tools import localize_time

auth_path = (Path().resolve().parent / 'creds/creds.yaml').as_posix()


def search_30_args():
    """
    Creates arguments for authorization using Twitter Dev tokens under the 30 Days Search Premium
    """
    premium_args = lc(
        filename=auth_path,
        yaml_key='twitter_search_30d',
        env_overwrite=False
        )
    return premium_args


def search_v2_args():
    """
    Creates arguments for authorization using Twitter Dev tokens under the Search V2 scope
    """
    premium_args = lc(
        filename=auth_path,
        yaml_key='twitter_search_v2',
        env_overwrite=False
        )
    return premium_args


def rule_creator(from_date, search_string):
    """
    Creates search rules, based on parameters such as start date and
    the string query to search for.
    """
    rule = payload_rule(
        search_string,
        from_date=from_date,
        results_per_call=100,
        )
    return rule


def stream_tweets(rule, premium_args, max_results=25000, max_pages=2500):
    """
    Generates a tweet stream to capture either a specified number of tweets
    or a max number of pages, whichever comes first.
    """
    rs = ResultStream(
        rule_payload=rule,
        max_results=max_results,
        max_pages=max_pages,
        **premium_args
        )
    stream_result = rs.stream()
    stream_result = list(stream_result)
    stream_result = json_normalize(stream_result)
    return stream_result


def normalize_result(df):
    df.columns = df.columns.str.replace('.', '_')
    df = df[twitter_cols]
    df['source'] = df.source.apply(lambda x: x.split('>')[1].split('<')[0])
    df['created_at'] = localize_time(df['created_at'])
    df['user_created_at'] = localize_time(df['user_created_at'])
    df.loc[df.truncated, 'text'] = df.loc[df.truncated, 'extended_tweet_full_text']
    df.drop('extended_tweet_full_text', axis=1, inplace=True)
    inter_cols = ['quote_count', 'reply_count', 'retweet_count', 'favorite_count']
    df.insert(12, 'interactions', df[inter_cols].sum(axis=1))
    return df


twitter_cols = [
    'created_at', 'id_str', 'text', 'source',  'truncated',
    'in_reply_to_status_id_str', 'in_reply_to_user_id_str',
    'in_reply_to_screen_name', 'quote_count', 'reply_count',
    'retweet_count', 'favorite_count', 'user_id_str', 'user_name',
    'user_screen_name', 'user_location', 'user_url', 'user_description',
    'user_verified', 'user_followers_count', 'user_friends_count',
    'user_listed_count', 'user_favourites_count', 'user_statuses_count',
    'user_created_at', 'user_geo_enabled', 'user_profile_image_url',
    'user_profile_image_url_https', 'user_default_profile',
    'user_default_profile_image', 'extended_tweet_full_text']
