from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Get the user's inputted keywords from the form
        keywords = request.form['keywords']
        
        # Prompt the user for confirmation before proceeding
        return render_template('confirm.html', keywords=keywords)
    
    # If the request method is GET, simply render the form template
    return render_template('form.html')

@app.route('/submit', methods=['POST'])
def submit():
    # Get the confirmed keywords from the form
    keywords = request.form['keywords']
    
    # Write the keywords to the file "queries.txt", one keyword per line
    with open('queries.txt', 'a') as f:
        for keyword in keywords.split(','):
            f.write(keyword.strip() + '\n')
    
    # Render a thank you message to the user
    return render_template('thanks.html')

if __name__ == '__main__':
    app.run(debug=False)
