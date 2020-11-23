from flask import Flask, redirect, url_for, request, render_template
import query_backend as back

app = Flask(__name__)


@app.route('/keyword=<word>')
def keyword(word):
    review, sentiment, error = back.get_info(word.strip())
    if error:
        return render_template('search.html')
    else:
        return render_template('index.html', labels=sentiment, content=review)


@app.route('/', methods=['POST', 'GET'])
def login():
    if request.method == 'POST':
        word = request.form['nm']
        return redirect(url_for('keyword', word=word))
    else:
        return render_template('search.html')


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
