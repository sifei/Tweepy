import tweepy
import json
import MySQLdb
import time

consumer_key = '####'
consumer_secret = '####'
access_token = '##-####'
access_token_secret = '###'

auth = tweepy.auth.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)#, parser=tweepy.parsers.JSONParser())

def main():
    # get All tweetID
    con = MySQLdb.connect('localhost','username','password','database name')
    cur = con.cursor()
    query = """select tweetID from PCSM where original_tweetID is Null;"""
    cur.execute(query)
    tweetIDs = cur.fetchall()
    temp = []
    for ID in tweetIDs:
        temp.append(ID[0])
    tweetIDs = temp
    for i in range(0,len(tweetIDs)/100+1):
        try:
            tweetID_100 = tweetIDs[100*i:100*(i+1)]
        except:
            tweetID_100 = tweetIDs[100*i:]
        print len(tweetID_100)
        try:
            status = api.statuses_lookup(tweetID_100)
            print len(status)
            for json_str in status:#range(0,len(status)):
                json_str = json_str._json
                toDB = []
                try:
                    toDB =[json_str['id_str'], json_str['retweeted_status']['id_str'],json_str['user']['followers_count'],json_str['user']['listed_count'],json_str['user']['statuses_count'],json_str['user']['description'].encode('ascii','ignore'),json_str['user']['friends_count'],json_str['user']['location'].encode('ascii','ignore'),json_str['created_at']]
                    
                except:
                    toDB =[json_str['id_str'], json_str['id_str'],json_str['user']['followers_count'],json_str['user']['listed_count'],json_str['user']['statuses_count'],json_str['user']['description'].encode('ascii','ignore'),json_str['user']['friends_count'],json_str['user']['location'].encode('ascii','ignore'),json_str['created_at']]
                    
                toDB = map(str, toDB)
                cur.execute("""update PCSM set original_tweetID=%s,followers_count=%s,listed_count=%s,statuses_count=%s,description=%s,friends_count=%s,location=%s,created_at=%s where tweetID=%s""", (toDB[1],toDB[2],toDB[3],toDB[4],toDB[5],toDB[6],toDB[7],toDB[8],toDB[0]))
            con.commit()
        except:
            print "hit limitation"
            time.sleep(15*60)


main()

               
