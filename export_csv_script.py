import datetime
import elasticsearch
import csv

es = elasticsearch.Elasticsearch('http://localhost:9200')

def get_page(inp):
    print(inp)

    res = es.search(index="index_name", body={"from":int(inp),"size":1000,
    "_source": ["field1","field2","field3","field4","field5","field6","field7","field8","field9"],
    "query": {
	    "bool": {
	      "must" : [ 
		{ "terms": { "_type": ["input_type"] } },
        { "range": {"date": {"gte": "now-30d/d"}}} 
	      ]
	    }
	  } 

    })
    return  res["hits"]["hits"]


if __name__ == '__main__':
    print ("Exporting data started at " + str(datetime.datetime.now()))
    start = 0
    page = get_page(start)
    data = []
    while len(page) > 0:
    ##### CALCULATION LOGIC HERE####
        print ("Exporting batch number: " + str((start / 1000)+1) +" Starting from doc number: " + str(start))
        for doc in page:
            data.append(doc)
        start= start + 1000
        page = get_page(start)
    print(len(data))
    
    with open('/Download/export_file.csv', 'w') as outfile:
     header_present = False
     for doc in data:
        my_dict = doc['_source'] 
        if not header_present:

            #Re-ordering the columns 
            w = csv.DictWriter(outfile, ["field4","field8","field7","field5","field6","field2","field1","field3","field9"])
            w.writeheader()
            
            header_present = True

        w.writerow(my_dict)
    print ("Exporting data finished at "+str(datetime.datetime.now()))

