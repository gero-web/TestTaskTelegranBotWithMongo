import pymongo
import bson
import datetime
import json
import telebot

bot = telebot.TeleBot('')
connect = None

def restore(path, conn, db_name):
    db = conn[db_name]
    with open(path, 'rb+') as dumb:
        db["sample"].insert_many(bson.decode_all(dumb.read()))



def aggregate(inp):
    match_stage = {
        "$match":
            {
                
                "dt":{
                    "$gte": datetime.datetime.fromisoformat(inp["dt_from"]), 
                    "$lt":  datetime.datetime.fromisoformat(inp["dt_upto"])
                }
            
            }
    }
     
    group = {        
            "$group": 
                {
                    "_id" : {
                            "year":{"$year":"$dt"} ,
                            "month":{"$month":"$dt"},      
                            "week":{"$week":"$dt"} if inp["group_type"] == "week"else None,
                            "day":{"$dayOfMonth":"$dt"} if inp["group_type"] == "day"else None,
                            "hour":{"$hour":"$dt"} if inp["group_type"] == "hour"else None,
                        } ,
                  
                    "dataset" : {"$sum" : "$value"}
                } ,
               
          
    }
    
    proj = {
        "$project":{
            "labels": "$_id",
            "dataset":"$dataset",
            "_id":False
        }
    }
    sort = {
            "$sort":{
                        "_id.yer":1, 
                        "_id.month":1, 
                        "_id.day":1, 
                        "_id.week":1, 
                        "_id.hour":1, 
                    }
        }
    query = [match_stage, group, sort,proj]
    return query

def cast_to_answer(agg):
    answer = {"dataset":[], "labels":[]}
    for ag in agg:
        answer["dataset"].append(ag["dataset"])
        time =    str(ag["labels"]["hour"]  or "00") + "/00/00"
        str_date = "/".join([ str(ag["labels"]["year"]), str(ag["labels"]["month"]), str(ag["labels"]["day"] or "01"),time])
        date =datetime.datetime.strptime(str_date, "%Y/%m/%d/%H/%M/%S")
        answer["labels"].append( date.isoformat())
    return answer


def main(inp, db: pymongo.database.Database):
    collection_name = "sample"
    agg_result =  db[collection_name].aggregate(aggregate(inp)) 
    return cast_to_answer(agg_result)


@bot.message_handler(commands=['start'])
def start(message):
    pass

@bot.message_handler(content_types=['text'])
def get_text_messages(message):
    data = json.loads(message.text)
    answer =  main(data, connect[db_name])
    print(answer)
    bot.send_message(message.from_user.id, json.dumps(answer) )

if __name__ == '__main__':
   
     user='mongo_admin'
     password=''   
     db_name = 'test'
     URL = f'mongodb://{user}:{password}@127.0.0.1:27017' 
   
        
     with  pymongo.MongoClient(URL) as conn:
        conn["test"].drop_collection("sample")
        restore("dump\sampleDB\sample_collection.bson", conn, "test")
        connect = conn
        bot.polling(none_stop=True, interval=0) 
       