import ibm_db
from job_setup import hostname, port


def check_db2_connection(database, user, password):
    """Check DB2 connection"""

    response = {}
    dsn = f"DATABASE={database};HOSTNAME={hostname};PORT={port};PROTOCOL=TCPIP;UID={user};PWD={password}"
    conn = None
    try:
        conn = ibm_db.connect(dsn, "", "")
        response["success"] = True

    except Exception as e:
        response["success"] = False
        if ibm_db.conn_error() != "":
            response["SQLSTATE"] = ibm_db.conn_error()
            response["error_msg"] = ibm_db.conn_errormsg()
        else:
            response["error_msg"] = e

    finally:
        if conn:
            ibm_db.close(conn)

    return response


def execute_sql_script(database, user, password, sql_script):

    response = {}
    conn = None
    dsn = f"DATABASE={database};HOSTNAME={hostname};PORT={port};PROTOCOL=TCPIP;UID={user};PWD={password}"
    try:
        conn = ibm_db.connect(dsn, "", "")

        with open(sql_script, "r") as f:
            sql = f.read()

        stmt = ibm_db.prepare(conn, sql)

        ibm_db.execute(stmt)
        response["success"] = True

    except Exception as e:
        response["success"] = False
        if ibm_db.stmt_error() == "":
            response["error_msg"] = ibm_db.conn_errormsg()
            response["SQLSTATE"] = ibm_db.conn_error()
        elif ibm_db.conn_error() == "":
            response["error_msg"] = ibm_db.stmt_errormsg()
            response["SQLSTATE"] = ibm_db.stmt_error()
        else:
            response["error_msg"] = e

    finally:
        if conn:
            ibm_db.close(stmt)
            ibm_db.close(conn)

    return response


def execute_sql_script_get_output(database, user, password, sql_script, output_file):

    response = {}
    conn = None
    dsn = f"DATABASE={database};HOSTNAME={hostname};PORT={port};PROTOCOL=TCPIP;UID={user};PWD={password}"
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

    except Exception as e:
        response["success"] = False
        if ibm_db.conn_error() != "":
            response["SQLSTATE"] = ibm_db.conn_error()
            response["error_msg"] = ibm_db.conn_errormsg()
        elif ibm_db.stmt_error() != "":
            response["SQLSTATE"] = ibm_db.conn_error()
            response["error_msg"] = ibm_db.conn_errormsg()
        else:
            response["error_msg"] = e

    finally:
        # Close the statement and connection
        if conn:
            ibm_db.close(stmt)
            ibm_db.close(conn)

    return response


if __name__ == '__main__':
    print(execute_sql_script_get_output("", "", "", "temp.sql", "output.txt"))

