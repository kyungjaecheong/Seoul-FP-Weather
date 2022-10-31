from flask import Flask, render_template, request
import numpy as np
import pickle

app = Flask(__name__)
model = pickle.load(open("./data/model.pkl", "rb"))

@app.errorhandler(404)
def page_not_found(error):
    return render_template('404.html'), 404

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        return render_template('index.html'), 200
    if request.method == 'POST':
        try:
            var1 = float(request.form['avgTa'])
            var2 = float(request.form['maxTa'])
            var3 = float(request.form['minTa'])
            var4 = float(request.form['sumRn'])
            var5 = float(request.form['avgWs'])
            var6 = float(request.form['avgRhm'])
            var7 = float(request.form['sumSsHr'])
            var8 = float(request.form['avgPs'])
            array = np.array([[var1, var2, var3, var4, var5, var6, var7, var8]])
            
            pred = int(model.predict(array).round(0))
            if pred < 0 :
                return render_template('404-1.html')
            else :
                return render_template('index.html',pred=pred)
        except:
            return render_template('404-2.html')
if __name__ == "__main__":
    app.run(debug=True)