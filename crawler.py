api = "https://api.tinproxy.com/proxy/get-new-proxy?API_KEY={}&AUTHEN_IPS={}&location=random"

# crawler
import snscrape.modules.twitter as sntwitter
import time
import requests
import json 
import os 
import datetime
# keep track of the progress
from tqdm import tqdm
# google drive authentication
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials
# subprocess    
import multiprocessing

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

gdrive_auth = authenticate()

def upload_to_gdrive(filename, gdrive_auth, FOLDER_ID):
    file = gdrive_auth.CreateFile({'title': filename, 'parents': [{'id': FOLDER_ID}]})
    file.SetContentFile("./test/" + filename)
    file.Upload()
    print(f"Upload {filename} to Google Drive")
    return True

def get_new_proxy(api_key):
    # call api to get new proxy
    new_api = api.format(api_key, "178.547.54.158")
    response = requests.get(new_api)
    data = json.loads(response.text)
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
    # Search each keyword from day x to x+1 
    while (until != STOP_DATE):
        # Save the crawled tweets to the master json file 
        print(f"Start crawling {keyword} from {until} to {get_yesterday(until)}")
        authentication = get_new_proxy(api_key)
        while authentication == "Error":
            print("Error, waiting 60 seconds to get new proxy")
            time.sleep(60)
        query = f"{keyword} until:{until} since:{get_yesterday(until)} lang:en"
        if not exclude_retweets:
            query += " -filter:retweets"
        if not exclude_replies:
            query += " -filter:replies"
        # Only save tweetID, date, content, username, retweet count, like count, reply count, hashtags
        num_tweets = 0
        with open("./test/" + file_name, 'a') as f:
            for tweet in tqdm(sntwitter.TwitterSearchScraper(query).get_items()):
                num_tweets += 1
                f.write(json.dumps({'tweetID': tweet.id, 'conversationId': tweet.conversationId, 'date': str(tweet.date), 'content': tweet.rawContent, 'username': tweet.user.username, 'retweetCount': tweet.retweetCount, 'likeCount': tweet.likeCount, 'replyCount': tweet.replyCount, 'hashtags': tweet.hashtags}) + "\n")
        until = get_yesterday(until)
        if (num_tweets < 100):
            print("Less than 100 tweets, waiting 30 seconds to get new proxy")
            time.sleep(30)
    upload_to_gdrive(file_name)
    return True

def main():
    # initialize step 
    number_of_threads = len(API_KEYS)
    keyword_stack = []
    subprocesses = [None] * number_of_threads
    
    while True: 
        # load keywords from queries.txt
        with open('queries.txt', 'r') as f:
            keywords = f.read().splitlines()
            for keyword in keywords:
                keyword_stack.append(keyword)
        if keyword_stack == []:
            print("Waiting for new keywords")
            time.sleep(10)
            continue
        for i in range(number_of_threads):
            if subprocesses[i] is None or not subprocesses[i].is_alive():
                if len(keyword_stack) > 0:
                    keyword = keyword_stack.pop()
                    subprocesses[i] = multiprocessing.Process(target=crawl_keyword, args=(keyword, "2023-03-25", False, True, API_KEYS[i]))
                    subprocesses[i].start()
                    print(f"Start crawling {keyword} with thread {i}")
                else:
                    print("No more keywords to crawl")
                    break

# if __name__ == '__main__': 
#     main()

print(load_secrecy())