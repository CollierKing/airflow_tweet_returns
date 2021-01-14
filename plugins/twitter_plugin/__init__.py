from airflow.plugins_manager import AirflowPlugin

from twitter_plugin.hooks.twitter_hook import TwitterHook
# from plugins.twitter_plugin.operators.twitter_to_pandas_hook import TwitterToPandasOperator


class twitter_plugin(AirflowPlugin):
    name = "TwitterPlugin"
    operators = []
    # Leave in for explicitness
    hooks = [TwitterHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
