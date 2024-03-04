from flask import Flask, jsonify
from flask import request
import sqlite3

app = Flask(__name__)

@app.route('/size', methods=['GET'])
def get_size():
    # Connect to SQLite database
    conn = sqlite3.connect('/storage/hash_database.db')
    conn.row_factory = sqlite3.Row  # This enables column access by name: row['column_name']

    # Retrieve data
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM hash_mapping")
    #rows = cur.fetchall()

    row_count = cursor.fetchone()[0]

    # Close the connection
    conn.close()
    return jsonify({'size':row_count})


@app.route('/list', methods=['POST'])
def get_list():
    # Connect to SQLite database
    conn = sqlite3.connect('/storage/hash_database.db')
    conn.row_factory = sqlite3.Row  # This enables column access by name: row['column_name']

    data = request.form
    input_CHOs = list(data.values())
    placeholders = ', '.join(['?'] * len(input_CHOs))
    
    # Retrieve data
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM hash_mapping WHERE ProvidedCHO IN ({placeholders})",input_CHOs)
    rows = cur.fetchall()
    
    # Convert rows to dictionary format
    objects = []
    for row in rows:
        objects.append({"ProvidedCHO": row["ProvidedCHO"], "ThumbnailURL": row["ThumbnailURL"], "ThumbnailID": row["ThumbnailID"]})
    
    # Close the connection
    conn.close()

    return jsonify(objects)

if __name__ == '__main__':
    app.run(debug=True,port=6008)
