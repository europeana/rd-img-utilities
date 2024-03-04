import sqlite3
import time

input_CHOs = [
    '/00738/plink__f_5_226086',
    '/00738/plink__f_5_226084'    
]


conn = sqlite3.connect('/storage/hash_database.db')
conn.row_factory = sqlite3.Row  # This enables column access by name: row['column_name']
print('Connected')


print('Retrieving data...')

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



print(objects)