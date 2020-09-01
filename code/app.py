import findspark
findspark.init()

from flask import Flask, Blueprint, jsonify, request
from geo import GeoTweets

app = Flask(__name__)
global geo_tweets

@app.route('/count_tweets/')
def count_tweets():

    orig_lon = float(request.args.get('lo'))
    orig_lat = float(request.args.get('la'))
    dist = float(int(request.args.get('ra')) * 0.621371)

    geo_tweets = GeoTweets()

    return jsonify(counter_tweets = geo_tweets.get_counter_tweets(orig_lon, orig_lat, dist)), 200


if __name__ == '__main__':
    app.run()
