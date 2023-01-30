import ibm_db
from job_setup import hostname, port

def check_db2_connection(database, user, password):
    """Check DB2 connection"""

    response = {}
    # Database connection string
    dsn = f"DATABASE={database};HOSTNAME={hostname};PORT={port};PROTOCOL=TCPIP;UID={user};PWD={password}"
    try:
        conn = ibm_db.connect(dsn, "", "")
        response["success"] = True
    except ibm_db.conn_error as e:
        response["success"] = False
        response["SQLSTATE"] = e
        response["error_msg"] = ibm_db.conn_errormsg()
    finally:
        # Close the connection
        if conn:
            ibm_db.close(conn)

    return response

def execute_sql_script(database, user, password, sql_script):
    """Connect to DB2 database and execute SQL script"""

    response = {}
    # Database connection string
    dsn = f"DATABASE={database};HOSTNAME=;PORT=port;PROTOCOL=TCPIP;UID={user};PWD={password}"
    try:
        conn = ibm_db.connect(dsn, "", "")

        with open(sql_script, "r") as f:
            sql = f.read()

        stmt = ibm_db.prepare(conn, sql)

        ibm_db.execute(stmt)
        response["success"] = True

    except ibm_db.stmt_error as e:
        response["error_msg"] = e
        response["SQLSTATE"] = ibm_db.stmt_errormsg()
        response["success"] = False

    finally:
        # Close the statement and connection
        if conn:
            ibm_db.close(stmt)
            ibm_db.close(conn)

    return response

def execute_sql_script_get_output(database, user, password, sql_script, output_file):
    """Connect to DB2 database and execute SQL script"""

    response = {}
    # Database connection string
    dsn = f"DATABASE={database};HOSTNAME=;PORT=port;PROTOCOL=TCPIP;UID={user};PWD={password}"
    try:
        conn = ibm_db.connect(dsn, "", "")

        with open(sql_script, "r") as f:
            sql = f.read()

        stmt = ibm_db.prepare(conn, sql)

        ibm_db.execute(stmt)
        response["success"] = True
        with open(output_file, "w") as file:
            result = ibm_db.fetch_tuple(stmt)
            while result:
                file.write(str(result) + '\n')
                result = ibm_db.fetch_tuple(stmt)


    except ibm_db.stmt_error as e:
        response["error_msg"] = e
        response["SQLSTATE"] = ibm_db.stmt_errormsg()
        response["success"] = False

    finally:
        # Close the statement and connection
        if conn:
            ibm_db.close(stmt)
            ibm_db.close(conn)

    return response