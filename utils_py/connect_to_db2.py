import ibm_db
from script_skeleton import process_messages


def connect_to_db2(database, user, password):
    """Connect to DB2 database and execute SQL script"""

    # Database connection string
    dsn = f"DATABASE={database};HOSTNAME=;PORT=port;PROTOCOL=TCPIP;UID={user};PWD={password}"
    try:
        # Connect to the database
        conn = ibm_db.connect(dsn, "", "")

        # Open the SQL script file
        with open("path/to/script.sql", "r") as f:
            sql = f.read()

        # Prepare the SQL statement
        stmt = ibm_db.prepare(conn, sql)

        # Execute the SQL statement
        ibm_db.execute(stmt)

    except ibm_db.conn_error as e:
        print(f'Connection error: {e}')
        return

    except ibm_db.stmt_error as e:
        print(f'SQL error: {e}, SQLSTATE: {ibm_db.stmt_errormsg()}')
        return

    except ibm_db.Error as e:
        print(f'Error: {e}')
        return

    finally:
        # Close the statement and connection
        if conn:
            ibm_db.close(stmt)
            ibm_db.close(conn)
