from flask import Flask, redirect, url_for, request, render_template
import query_backend as back

app = Flask(__name__)

@app.route('/<word>', methods=['POST','GET'])
def re_search(word):
    if request.method == 'GET':
        return render_template('search2.html', word=word)
    else:
        word = request.form['nm']
        return redirect(url_for('keyword', word=word))
        
    return render_template('search2.html', word=word)

@app.route('/keyword=<word>', methods=['POST', 'GET'])
def keyword(word):
    review, sentiment, error = back.get_info(word.strip())
    if error:
        return redirect(url_for('re_search', word=word))
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
