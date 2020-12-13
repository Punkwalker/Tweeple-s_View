
# set GOOGLE_APPLICATION_CREDENTIALS = "D:\Cloud Computing\Project\Tweeple's_View\venv\tweeple-sentiment-ac73f4e6b7b4.json"

from concurrent.futures import TimeoutError
from nltk.tokenize import WordPunctTokenizer
import tweepy
import re
from google.cloud import language
from google.cloud import pubsub_v1
from threading import Thread


# Project Variables Configuration---------------------
project_id = "tweeple-1"
subscription_id = "gettopicsub"
topic_id1 = "results"
topic_id2 = "done-bq"

#subscriber client for Pub/Sub API------------------------
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

#publisher client1 for Pub/Sub API-------------------
publisher1 = pubsub_v1.PublisherClient()
topic_path1 = publisher1.topic_path(project_id, topic_id1)

#publisher client2 for Pub/Sub API-------------------
publisher2 = pubsub_v1.PublisherClient()
topic_path2 = publisher2.topic_path(project_id, topic_id2)

#Global Variable for Tweet Topic-------------------
tweettopic = ''

#Twepy Authentication Method------------------
def tweepyauth():
    Consumer_Key = "h9AHnaEUfUMHaMm1bnCAVUbZW"
    Consumer_Secret = "JB9NYGPhRaJ7mQOvtNbLz47hQPYXuaBLVilV1mHkWASebzrv9u"

    Access_Token = "137401602-3qTQ1Ji38nZRECEDA1TOyw5xoGUrhtnDcfiMur9z"
    Access_Secret = "R2oFhN5vhIb5QcDn2bWsJ2XLbNoNYHuJ1jfTwuHTTFm7h"

    auth = tweepy.OAuthHandler(Consumer_Key, Consumer_Secret)
    auth.set_access_token(Access_Token, Access_Secret)
    api = tweepy.API(auth)
    return api

#Method to clean the tweets------------------
def format_tweets(tweet):
    remove_user = re.sub(r'@[A-Za-z0-9]+', '', tweet.decode('utf-8'))
    remove_link = re.sub('https?://[A-Za-z0-9./]+', '', remove_user)
    remove_numbers = re.sub('[^a-zA-Z]', ' ', remove_link)
    lower_case = remove_numbers.lower()
    tokens = WordPunctTokenizer()
    words = tokens .tokenize(lower_case)
    clean_tw = (' '.join(words)).strip()
    return clean_tw

#Method to get the Senitment Score from Natural Language API------------------
def get_score(tweet):
    client = language.LanguageServiceClient()
    type_ = language.Document.Type.PLAIN_TEXT
    document = {"content": tweet, "type_": type_}
    document_sentiment = client.analyze_sentiment(document=document).document_sentiment.score
    return document_sentiment

#Worker thread class for fetching the tweets and Passing it to Pub/Sub------------------
class tweetWorker(Thread):
    def run(self):
        print(f"Searching tweets for {tweettopic}....")
        api = tweepyauth()

        try:
            # Get Tweets
            get_tweets = tweepy.Cursor(api.search, q=tweettopic, result_type='recent', lang='en',
                                       tweet_mode='extended').items(1)
            for tweet in get_tweets:
                # Clean tweets
                clean_tweet = format_tweets(tweet.full_text.encode('UTF-8'))

                clean_tweet = str(clean_tweet)
                sentiment_score = get_score(clean_tweet)
                sentiment_score = str(sentiment_score)
                data = "{"+"\"tweet\"" + ":" + "\"" + clean_tweet + "\"" + "," + "\"score\"" + ":\"" + sentiment_score+ "\""+"}"

                print(data)
                data = data.encode('utf-8')
                # future = publisher1.publish(topic_path1,data)         #Publish Tweet to results Topic

            print("Sent Tweets and Score to Pubsub!!!")
            done = "1"
            future = publisher2.publish(topic_path2, done.encode('UTF-8'))

        except tweepy.error.TweepError:
            print("*****Error in fetching the Tweets******")

#Method to listen for message on gettopicsub subscription to start the worker Thread------------------
def listen_topic():
    def callback(message):
        message.ack()
        global tweettopic
        tweettopic = str(message.data)
        tweettopic = (tweettopic.lstrip('b').strip('\'') + ' -filter:retweets')
        t = tweetWorker()
        t.start()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    with subscriber:
        try:
            streaming_pull_future.result(timeout=None)
        except TimeoutError:
            streaming_pull_future.cancel()


if __name__ == "__main__":

    #Call for Method to listen the topic----------------
    listen_topic()