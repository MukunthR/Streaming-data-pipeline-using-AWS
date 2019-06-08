from flask import Flask, jsonify
import json
from multiprocessing import Value
from flask import Response
from datetime import datetime
from time import sleep

counter = Value('i', 0)
app = Flask(__name__)

global index_add_counter
@app.route('/', methods=['GET'])
def get_tasks():
    def streamer():
        while True:
            with counter.get_lock():
                counter.value += 1
            print(counter.value)
            i = 0
            with open('creditcard.json') as json_file:  
                data = json.load(json_file)
                for p in data:
                    if(i == counter.value):
                        yield "<p>{}</p>".format(p)
                        
                        # return jsonify({'tasks': p})
                    i+=1
    return Response(streamer())

@app.route("/time/")
def time():
    def streamer():
        while True:
            yield "<p>{}</p>".format(datetime.now())
            sleep(1)

    return Response(streamer())

if __name__ == '__main__':
    app.run(debug=True)