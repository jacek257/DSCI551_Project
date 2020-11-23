from flask import Flask, redirect, url_for, request, render_template
import query_backend as back

app = Flask(__name__)


@app.route('/result/<name>')
def result(name):
    review, sentiment = back.get_info(name)
    return render_template('index.html', labels=review, content=sentiment)


@app.route('/search', methods=['POST', 'GET'])
def login():
    if request.method == 'POST':
        user = request.form['nm']
        return redirect(url_for('result', name=user))
    else:
        return render_template('search.html')


if __name__ == '__main__':
    app.run(debug=True)
