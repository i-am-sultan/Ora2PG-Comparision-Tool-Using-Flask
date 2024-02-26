from flask import Flask, render_template, request
import psycopg2
import cx_Oracle

app = Flask(__name__)

# Function to connect to PostgreSQL database
def connect_to_postgres(host, port, dbname, user, password):
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    return conn

# Function to connect to Oracle database
def connect_to_oracle(username, password, host, port, service_name, mode=cx_Oracle.SYSDBA):
    dsn_tns = cx_Oracle.makedsn(host, port, service_name=service_name)
    conn = cx_Oracle.connect(user=username, password=password, dsn=dsn_tns, mode=mode)
    return conn

# Route for the home page
@app.route('/', methods=['GET', 'POST']) #GET: accessing data from server to client
def index():
    #client to server - POST 
    #eg: filling out a web form.
    if request.method == 'POST':
        # Get form data
        oracle_username = request.form['oracle_username']
        oracle_password = request.form['oracle_password']
        oracle_host = request.form['oracle_host']
        oracle_port = request.form['oracle_port']
        oracle_service_name = request.form['oracle_service_name']

        postgres_host = request.form['postgres_host']
        postgres_port = request.form['postgres_port']
        postgres_dbname = request.form['postgres_dbname']
        postgres_user = request.form['postgres_user']
        postgres_password = request.form['postgres_password']

        # Get table names input
        table_names = request.form['table_names']
        table_names_list = [name.strip() for name in table_names.split(",")]

        # Connect to Oracle and PostgreSQL databases
        oracle_conn = connect_to_oracle(oracle_username, oracle_password, oracle_host, oracle_port, oracle_service_name)
        postgres_conn = connect_to_postgres(postgres_host, postgres_port, postgres_dbname, postgres_user,postgres_password)

        # Check if the table exists in Oracle and PostgreSQL databases
        oracle_cursor = oracle_conn.cursor()
        postgres_cursor = postgres_conn.cursor()

        table_counts = {}
        try:
            for table_name in table_names_list:
                oracle_row_count = 'NA'
                postgres_row_count = 'NA'
                difference = 'NA'

                # Query Oracle database for row count
                try:
                    oracle_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    oracle_row_count = oracle_cursor.fetchone()[0]
                except cx_Oracle.DatabaseError:
                    pass  # Table not found in Oracle, row count remains 'NA'

                # Query PostgreSQL database for row count
                try:
                    postgres_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    postgres_row_count = postgres_cursor.fetchone()[0]
                except psycopg2.DatabaseError:
                    pass  # Table not found in PostgreSQL, row count remains 'NA'

                # Calculate difference only if both row counts are numeric
                if isinstance(oracle_row_count, int) and isinstance(postgres_row_count, int):
                    difference = oracle_row_count - postgres_row_count

                table_counts[table_name] = {'oracle': oracle_row_count, 'postgres': postgres_row_count, 'difference': difference}
            return render_template('result.html', table_counts=table_counts)
        except (cx_Oracle.DatabaseError, psycopg2.DatabaseError) as e:
            return render_template('error.html', message=f"Error: {e}")
        finally:
            oracle_cursor.close()
            postgres_cursor.close()
            oracle_conn.close()
            postgres_conn.close()

    return render_template('index.html')
if __name__ == '__main__':
    app.run(debug=True)