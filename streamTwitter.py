import json
import oauth2 as oauth
import urllib2 as urllib
import datetime

def getCredentials():
    #you'll need to get these by registering for your own twitter developer account
    dict_llaves = json.load(open("private/keyFile.json"))
    api_key = dict_llaves["consumer_key"]
    api_secret = dict_llaves["consumer_secret"]
    access_token_key = dict_llaves["access_token"]
    access_token_secret = dict_llaves["access_secret"]

    oauth_token    = oauth.Token(key=access_token_key, secret=access_token_secret)
    oauth_consumer = oauth.Consumer(key=api_key, secret=api_secret)
    return (oauth_token, oauth_consumer)

def twitterreq(oauth_token, oauth_consumer, url, http_method, parameters):
    http_handler  = urllib.HTTPHandler(debuglevel=0)
    https_handler = urllib.HTTPSHandler(debuglevel=0)
    signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

    req = oauth.Request.from_consumer_and_token(oauth_consumer, token=oauth_token, http_method=http_method, http_url=url, parameters=parameters)
    req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)
    headers = req.to_header()
    if http_method == "POST":
        encoded_post_data = req.to_postdata()
    else:
        encoded_post_data = None
    url = req.to_url()
    opener = urllib.OpenerDirector()
    opener.add_handler(http_handler)
    opener.add_handler(https_handler)
    response = opener.open(url, encoded_post_data)
    return response

def main():
    (oauth_token, oauth_consumer) = getCredentials()

    http_method = "GET"

    url = "https://stream.twitter.com/1.1/statuses/filter.json?track=MakeAmericaMoreAmerican&locations=-80.477909,25.592786,-80.071144,26.044704"

    pars = []
    current_block = datetime.datetime.now()
    out_file = open("./raw_tweet_data/"+str(current_block.date())+"_"+str(current_block.time())+".json","w")
    response = twitterreq(oauth_token, oauth_consumer, url, http_method, pars)

    for line in response:
        now = datetime.datetime.now()
        diff = now - current_block
        if diff.seconds > 900:
            out_file.close()
            current_block = now
            out_file = open("./raw_tweet_data/"+str(current_block.date())+"_"+str(current_block.time())+".json","w")
        try:
            dic_line = json.loads(line)
            if dic_line["geo"] != None and len(dic_line["entities"]["hashtags"])!=0:
                out_file.write(line.strip()+"\n")
        except:
            continue

if __name__ == '__main__':
    main()