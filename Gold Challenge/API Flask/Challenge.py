import re
import pandas as pd
import sqlite3
import os
import io
from datetime import datetime

from flask import Flask, jsonify, g
from flask import request
from flask import send_file
from flasgger import Swagger, LazyString, LazyJSONEncoder
from flasgger import swag_from
from werkzeug.utils import secure_filename

app = Flask(__name__)

app.json_encoder = LazyJSONEncoder
swagger_template = dict(
    info={
        'title': "API Documentation for Data Processing and Modeling",
        'version': "1.0.0",
        'description': "Dokumentasi API untuk Data Processing dan Modeling",
    },
    host="127.0.0.1:5000/"
)

swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": "docs",
            "route": "/docs.json",
        }
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/docs/"
}
swagger = Swagger(app, template=swagger_template, config=swagger_config)

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect('database.db')
        g.cursor = g.db.cursor()
    return g.db, g.cursor

with app.app_context():
    db, cursor = get_db()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS text_processing (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            input_text TEXT,
            processed_text TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    db.commit()

with app.app_context():
    db, cursor = get_db()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_processing (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            input_file_content BLOB,  -- Change the column type to BLOB
            cleansed_file BLOB,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    db.commit()


def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect('database.db', check_same_thread=False)
        g.cursor = g.db.cursor()
    return g.db, g.cursor

@app.teardown_appcontext
def close_db(error):
    db = g.pop('db', None)
    if db is not None:
        db.close()

df_abusive = pd.read_csv("abusive.csv")
df_kamus = pd.read_csv("new_kamusalay.csv", encoding='latin1', header=None, names=['kata salah', 'kata benar'])

def cleanse_text(text):
    text = text.lower()
    text = re.sub(r'[^a-zA-Z0-9]', ' ', text)
    return text

kamus = dict(zip(df_kamus['kata salah'], df_kamus['kata benar']))

def word_correction(text):
    words = text.split()
    correction_text = []
    for word in words:
        if word in kamus:
            correction_text.append(kamus[word])
        else:
            correction_text.append(word)
    return ' '.join(correction_text)

abusive_patterns = df_abusive['ABUSIVE'].tolist()

def abusive_cleansing(text):
    for pattern in abusive_patterns:
        text = re.sub(pattern, '', text)
    text = re.sub('user', '', text)
    text = re.sub('kntl', '', text)
    return text

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == 'csv'


@swag_from("docs/text.yml", methods=['POST'])
@app.route('/text-processing', methods=['POST'])
def text_processing():
    with sqlite3.connect('database.db') as db:
        cursor = db.cursor()
        
    input_text = request.form.get('text')
    processed_text = cleanse_text(input_text)
    processed_text = word_correction(processed_text)
    processed_text = abusive_cleansing(processed_text)
    
    cursor.execute('''
        INSERT INTO text_processing (input_text, processed_text) VALUES (?, ?)
    ''', (input_text, processed_text))
    db.commit()
    
    json_response = {
        'status_code': 200,
        'description': "Teks yang sudah diproses",
        'data': processed_text,
    }
    response_data = jsonify(json_response)
    return response_data

@swag_from("docs/uploadfile.yml", methods=['POST'])
@app.route("/upload-file", methods=['POST'])
def upload_file():
    file = request.files['file']
    if file and allowed_file(file.filename):
        file_content = file.stream.read()  
        
        df = pd.read_csv(io.BytesIO(file_content), encoding='latin1')
        
        df['Tweet'] = df['Tweet'].apply(lambda x: re.sub(r'\\', '', x) if isinstance(x, str) else x)
        df['Tweet'] = df['Tweet'].apply(lambda x: re.sub(r'x\d+', '', x) if isinstance(x, str) else x)
        
        df['Tweet'] = df['Tweet'].apply(cleanse_text)
        df['Tweet'] = df['Tweet'].apply(word_correction)
        df['Tweet'] = df['Tweet'].apply(abusive_cleansing)
        
        cleansedfile = df.to_csv(index=False)
        
        db, cursor = get_db()
        
        cursor.execute('''
            INSERT INTO file_processing (input_file_content, cleansed_file) VALUES (?, ?)
        ''', (sqlite3.Binary(file_content), cleansedfile.encode()))
        db.commit()
        
        json_response = {
            'status_code': 200,
            'description': "File processed",
            'cleansed_file': cleansedfile,
        }
        response_data = jsonify(json_response)
        return response_data

if __name__ == '__main__':
    app.run()
