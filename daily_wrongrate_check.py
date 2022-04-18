from pymongo import MongoClient
import datetime
# client = MongoClient(SERVER, username = UID, password = PWD, authSource = AUTHSOURCE)[DATABASE]
client=MongoClient('172.16.76.193',27017,
                    username = "apacuser",
                    password = "apacuser@prod",
                    authSource = "CDP_DataReceiver"
                   )
if client:
    print("connected")

db=client["CDP_DataReceiver"]

if db:
    print("acessed")
col=db["wrongDataV2"]
utc_now = datetime.datetime.utcnow()
before_day = (utc_now - datetime.timedelta(days = 1)).strftime('%Y-%m-%d') + 'T07:00:00Z'

def bhphotovideo_wrong():
    wrongdata = col.aggregate([{"$match":{"website":'www.bhphotovideo.com','receiveTime':{"$gt":datetime.datetime.strptime(before_day,'%Y-%m-%dT%H:%M:%SZ')}}},{"$group":{'_id':"$wrongs.issue",'count':{"$sum":1}}},{"$sort":{'count':-1}}])
    # wrongdata = col.find({'website':'www.canadacomputers.com','wrongs.issue':'PRODUCT_STOCK_NULL_WHEN_ACTIVE_TRUE','receiveTime':{'$gt':datetime.datetime.strptime('2022-04-10T07:00:00Z','%Y-%m-%dT%H:%M:%SZ')}})
    print("-----------------www.bhphotovideo.com-----------------")
    for row in wrongdata:
        print(row)

def amazon_wrong():
    wrongdata = col.aggregate([{"$match":{"website":'www.amazon.com','receiveTime':{"$gt":datetime.datetime.strptime(before_day,'%Y-%m-%dT%H:%M:%SZ')}}},{"$group":{'_id':"$wrongs.issue",'count':{"$sum":1}}},{"$sort":{'count':-1}}])
    print("-----------------www.amazon.com-----------------")
    for row in wrongdata:
        print(row)

def staples_wrong():
    wrongdata = col.aggregate([{"$match":{"website":'www.staples.com','receiveTime':{"$gt":datetime.datetime.strptime(before_day,'%Y-%m-%dT%H:%M:%SZ')}}},{"$group":{'_id':"$wrongs.issue",'count':{"$sum":1}}},{"$sort":{'count':-1}}])
    print("-----------------www.staples.com-----------------")
    for row in wrongdata:
        print(row)

def walmart_wrong():
    wrongdata = col.aggregate([{"$match":{"website":'www.walmart.com','receiveTime':{"$gt":datetime.datetime.strptime(before_day,'%Y-%m-%dT%H:%M:%SZ')}}},{"$group":{'_id':"$wrongs.issue",'count':{"$sum":1}}},{"$sort":{'count':-1}}])
    print("-----------------www.walmart.com-----------------")
    for row in wrongdata:
        print(row)

def officedepot_wrong():
    wrongdata = col.aggregate([{"$match":{"website":'www.officedepot.com','receiveTime':{"$gt":datetime.datetime.strptime(before_day,'%Y-%m-%dT%H:%M:%SZ')}}},{"$group":{'_id':"$wrongs.issue",'count':{"$sum":1}}},{"$sort":{'count':-1}}])
    print("-----------------www.officedepot.com-----------------")
    for row in wrongdata:
        print(row)

def microcenter_wrong():
    wrongdata = col.aggregate([{"$match":{"website":'www.microcenter.com','receiveTime':{"$gt":datetime.datetime.strptime(before_day,'%Y-%m-%dT%H:%M:%SZ')}}},{"$group":{'_id':"$wrongs.issue",'count':{"$sum":1}}},{"$sort":{'count':-1}}])
    print("-----------------www.microcenter.com-----------------")
    for row in wrongdata:
        print(row)

def bestbuy_wrong():
    wrongdata = col.aggregate([{"$match":{"website":'www.bestbuy.com','receiveTime':{"$gt":datetime.datetime.strptime(before_day,'%Y-%m-%dT%H:%M:%SZ')}}},{"$group":{'_id':"$wrongs.issue",'count':{"$sum":1}}},{"$sort":{'count':-1}}])
    print("-----------------www.bestbuy.com-----------------")
    for row in wrongdata:
        print(row)

if __name__ == "__main__":
    bhphotovideo_wrong()
    amazon_wrong()
    staples_wrong()
    walmart_wrong()
    officedepot_wrong()
    microcenter_wrong()
    bestbuy_wrong()
