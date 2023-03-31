api = "https://api.tinproxy.com/proxy/get-new-proxy?api_key={}&authen_ips={}&location=random"

# crawler
import snscrape.modules.twitter as sntwitter
import time
import requests
import json 
import os 
import datetime
import re
# keep track of the progress
# from tqdm import tqdm
# google drive authentication
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials
# subprocess    
import multiprocessing

pattern = r'(\d{4}-\d{2}-\d{2})'

def load_secrecy(filepath='proxies.txt'):
    API_KEYS=[]
    for line in open(filepath):
        API_KEYS.append(line.strip())
    return API_KEYS
    
def authenticate():
    gauth = GoogleAuth()
    scope = ['https://www.googleapis.com/auth/drive']
    gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name('./credentials/service_account.json', scope)
    drive = GoogleDrive(gauth)
    return drive

API_KEYS = load_secrecy()
STOP_DATE = "2022-11-30"
FOLDER_ID = "1A5tBI7DAX5ht9wFOQBlfpzYJqT0QHEp6"
TWEETS_LIMIT = 5000

gdrive_auth = authenticate()

def upload_to_gdrive(filename, gdrive_auth, FOLDER_ID):
    file = gdrive_auth.CreateFile({'title': filename, 'parents': [{'id': FOLDER_ID}]})
    file.SetContentFile("./out/" + filename)
    file.Upload()
    print(f"Upload {filename} to Google Drive")
    return True

def get_new_proxy(api_key):
    # call api to get new proxy
    new_api = api.format(api_key, "178.547.54.158")
    print(new_api)
    response = requests.get(new_api)
    data = json.loads(response.text)
    print(data)
    print(data['status'])
    if data['status'] == 'active':
        http_ipv4 = data['data']['http_ipv4']
        username = data['data']['authentication']['username']
        password = data['data']['authentication']['password']
        # create an environment variable for proxy
        os.environ['http_proxy'] = f"http://{username}:{password}@{http_ipv4}"
        print(http_ipv4)
        return "Success"
    return "Error"

# Fill space in a string with '_'
def fill_space(a):
    return a.replace(" ", "_")

def get_yesterday(date):
    # get yesterday date, in form of yyyy-mm-dd hh:mm:ss
    date = datetime.datetime.strptime(date, "%Y-%m-%d")
    yesterday = date - datetime.timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")

def crawl_keyword(keyword, until, exclude_retweets, exclude_replies, api_key):
    file_name = fill_space(keyword) + "_" + STOP_DATE + "_" + until + ".json"
    log_file = open("./logs/" + fill_space(keyword) + "_" + STOP_DATE + "_" + until + ".txt", "w")
    total_tweet = 0
    # Search each keyword from day x to x+1 
    while (until != STOP_DATE):
        # Save the crawled tweets to the master json file 
        log_file.write(f"Start crawling {keyword} from {until} to {get_yesterday(until)}\n")
        last_tweet = None
        authentication = get_new_proxy(api_key)
        while authentication == "Error":
            log_file.write(f"Error getting new proxy, waiting 60 seconds to get new proxy\n")
            time.sleep(60)
        query = f"{keyword} until:{until} since:{get_yesterday(until)} lang:en"
        if not exclude_retweets:
            query += " -filter:retweets"
        if not exclude_replies:
            query += " -filter:replies"
        # Only save tweetID, date, content, username, retweet count, like count, reply count, hashtags
        num_tweets = 0
        with open("./out/" + file_name, 'a') as f:
            for tweet in sntwitter.TwitterSearchScraper(query).get_items():
                num_tweets += 1
                f.write(json.dumps({'tweetID': tweet.id, 'conversationId': tweet.conversationId, 'date': str(tweet.date), 'content': tweet.rawContent, 'username': tweet.user.username, 'retweetCount': tweet.retweetCount, 'likeCount': tweet.likeCount, 'replyCount': tweet.replyCount, 'hashtags': tweet.hashtags}) + "\n")
                last_tweet = tweet
        until = re.match(pattern, str(last_tweet.date)).group(1)
        total_tweet += num_tweets
        if (total_tweet > TWEETS_LIMIT):
            total_tweet = 0
    upload_to_gdrive(file_name)
    return True

def main():
    # initialize step 
    API_KEYS = load_secrecy('proxies.txt')
    number_of_threads = len(API_KEYS)
    print(number_of_threads)
    log_general = open("./logs/general_log.txt", "w")
    # time.sleep(10)
    keyword_queue = []
    subprocesses = [None] * number_of_threads
    while True: 
        # load keywords from queries.txt
        with open('queries.txt', 'r') as f:
            keywords = f.read().splitlines()
            for keyword in keywords:
                if keyword not in keyword_queue:
                    keyword_queue.append(keyword)
        open('queries.txt', 'w').close()
        if keyword_queue == []:
            log_general.write("No keywords to crawl, waiting 10 seconds to check again\n")
            time.sleep(10)
            continue
        for i in range(number_of_threads):
            if subprocesses[i] is None or not subprocesses[i].is_alive():
                if len(keyword_queue) > 0:
                    keyword = keyword_queue.pop()
                    subprocesses[i] = multiprocessing.Process(target=crawl_keyword, args=(keyword, "2023-03-25", False, True, API_KEYS[i]))
                    subprocesses[i].start()
                else:
                    log_general.write("No keywords to crawl. Waiting for new words\n")
                    break

if __name__ == '__main__': 
    main()

