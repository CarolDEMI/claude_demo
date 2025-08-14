from flask import Flask, render_template, jsonify, request
import datetime

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/hello')
def hello_api():
    return jsonify({
        'message': 'Hello from Claude Demo!',
        'timestamp': datetime.datetime.now().isoformat()
    })

@app.route('/api/echo', methods=['POST'])
def echo():
    data = request.json
    return jsonify({
        'received': data,
        'timestamp': datetime.datetime.now().isoformat()
    })

@app.route('/about')
def about():
    return render_template('about.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)