# Import sqlite3 things
import sqlite3
import sys
import time
import os

# Read the path to the sqlite database from arguments.
file = sys.argv[1] if len(sys.argv) >= 2 else "file.db"

# Inform that the file is not provided.
if not file:
    print("Please provide the path to the sqlite database file.")
    sys.exit(1)

# Figure out the full path to the file.
print("We use file %s" % file)

# Get the file path from that file, and list the files in the directory.
path = os.path.dirname(file)

if not path:
  print("Please provide absolute path to a sqlite database file.")
  sys.exit(1)

print("The directory is %s" % path)
print("Files in the directory are:")
for f in os.listdir(path):
    print(f)

# Check that we can write to a text file in that directory. File is file.txt
print("Creating a file in the directory")
with open(os.path.join(path, "created.txt"), "w") as f:
    f.write("Hello, world!")

# Now append some test to a file that already exists.
print("Appending to the file")
# If not exists throws an error, so we need to use "a" mode.
if not os.path.exists(os.path.join(path, "updated.txt")):
  raise Exception("File updated.txt does not exist")

with open(os.path.join(path, "updated.txt"), "a") as f:
    f.write("Hello, again!")

# Connect to the sqlite database.
conn = sqlite3.connect(file)

# Now from second argument read number of rows to insert.
rows_to_insert = int(sys.argv[2]) if len(sys.argv) >= 3 else 1000

# Read the mode from the third argument.
mode = sys.argv[3] if len(sys.argv) >= 4 else 'normal'

# Change the sqlite mode to the one selected.
# Available modes are:
# - normal (default journal mode) https://www.sqlite.org/lockingv3.html#rollback
# - wal (write-ahead logging) https://www.sqlite.org/wal.html#overview

if mode == 'wal':
    print("Using WAL mode")
    print("We use file %s" % file)
    print("Journal file is %s" % file + "-wal")
    print("Shared memory file is %s" % file + "-shm")
    conn.execute('PRAGMA journal_mode = WAL')
else:
    print("Using Journal mode")
    print("We use file %s" % file)
    print("Journal file is %s" % file + "-journal")
    conn.execute('PRAGMA journal_mode = DELETE')


# Create a basic table with A, B, C columns only if it doesn't exists.
conn.execute('''
CREATE TABLE IF NOT EXISTS test (
    A INTEGER,
    B INTEGER,
    C INTEGER
)
''')

# Start measuring the time from this point.
start = time.time()

# Ready number of rows there are in the table.
rows = conn.execute('SELECT COUNT(*) FROM test').fetchone()[0]

# Print the number of rows.
print("We currently have: %d" % rows)


# Insert those rows, but one at a time, each one in a transaction, to test the performance.
# Before inserting, make sure that similar row is not there.
# Each row is just rows + 1, rows + 2, rows + 3, etc.

for i in range(rows_to_insert):
    A = rows + i + 1
    B = rows + i + 2
    C = rows + i + 3

    # Begin transaction
    conn.execute('BEGIN')

    conn.execute('''
    INSERT INTO test (A, B, C)
    SELECT ?, ?, ?
    WHERE NOT EXISTS (
        SELECT 1 FROM test WHERE A = ? AND B = ? AND C = ?
    )
    ''', (A, B, C, A, B, C))

    # Commit transaction
    conn.execute('COMMIT')

    # Inform every 500 rows about the progress.
    if i % 500 == 0:
        print("Inserted %d rows" % i)

print("Executed %d transactions" % rows_to_insert)
elapsed = time.time() - start
per_row = elapsed / rows_to_insert
print("Elapsed time: %.2f seconds" % elapsed)
print("Per row: ~%.2f seconds" % per_row)


# Now insert the same number of rows, but in a single transaction.
rows = conn.execute('SELECT COUNT(*) FROM test').fetchone()[0]

start = time.time()
conn.execute('BEGIN')
for i in range(rows_to_insert):
    A = rows + i + 1
    B = rows + i + 2
    C = rows + i + 3

    conn.execute('''
    INSERT INTO test (A, B, C)
    SELECT ?, ?, ?
    WHERE NOT EXISTS (
        SELECT 1 FROM test WHERE A = ? AND B = ? AND C = ?
    )
    ''', (A, B, C, A, B, C))

    # Inform every 500 rows about the progress.
    if i % 500 == 0:
        print("Inserted %d rows" % i)

conn.execute('COMMIT')
print("Inserted %d rows in 1 transaction" % rows_to_insert)
elapsed = time.time() - start
per_row = elapsed / rows_to_insert
print("Elapsed time: %.2f seconds" % elapsed)
print("Per row: ~%.2f seconds" % per_row)