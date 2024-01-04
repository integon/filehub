from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health():
    response = {"Status": "UP"}
    return jsonify(response), 200

if __name__ == '__main__':
    app.run(debug=True)
