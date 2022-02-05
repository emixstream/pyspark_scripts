
import collections


Tweet = collections.namedtuple("Tweet", "num date time text")
ClassifiedTweet = collections.namedtuple("ClassifiedTweet", "num sentiment")




# Analyse a text and detect if it is positive negative or neutral 
def sentiment(s): 
    positive = ("like", "love", "good", "great", "happy","cool", "amazing")
    negative = ("hate", "bad", "stupid")
    st=0;
    words = s.split(" ")
    for p in positive:
        for w in words:
            if p==w: 
                st = st+1
    negs=list(filter(lambda w: w in negative,words))
    num_neg=len(negs)
    
    st=st-num_neg
    if(st>0):
        return "positive"
    elif(st<0):
        return "negative"
    else:
        return "neutral"
    




tweet1= Tweet(1,"22/06/2016","08:00:00","I love the new phone by YYYY")
tweet2= Tweet(2,"22/06/2016","08:10:00","The new camera by ZZZZ is amazing")
tweet3 =Tweet(3,"23/06/2016","08:30:00","""I heard about the strike but it is 
                unbelivable we don’t move for more than one hour. I hate traffic jams""")
tweetsRDD=sc.parallelize([tweet1,tweet2,tweet3])



classifiedTweetsRDD=tweetsRDD.map(lambda t: ClassifiedTweet(t.num,sentiment(t.text)))
classifiedTweetsRDD.collect()


t0=classifiedTweetsRDD.map(lambda t: tuple( (t.num, t.sentiment )))
t0.collect()
t1=tweetsRDD.map(lambda t:(t.num, t.date)).join(t0)
#t1=tweetsRDD.map(lambda t:(t.num, t.date)).join(classifiedTweetsRDD)


t1.collect()



#for (num, (date, sentiment)) in t1.toLocalIterator():
#    print("%d %s %s" % (num, date, sentiment))
