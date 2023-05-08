from flask import Flask, request
import pickle
import numpy as np
from flask import jsonify

app = Flask(__name__)

@app.route('/predict')
def predict():
    """
    Route to predict the value from features for model.

    Requests params:
        vol_moving_avg: Moving average of Volume for last 30days.
        adj_close_rolling_med: Adj Close median for last 30 days.

    Response:
        Predicted volume.
    """

    model = None
    with open('archive/finalized_model.pkl', 'rb') as f:
        model = pickle.load(f)

    vol_moving_avg = request.args.get('vol_moving_avg')
    adj_close_rolling_med = request.args.get('adj_close_rolling_med')
    
    features = np.array([[vol_moving_avg, adj_close_rolling_med]])
    prediction = model.predict(features)

    return jsonify({'predicted_volume': str(prediction[0])})


if __name__ == '__main__':
    app.run(port=5050, debug=True)