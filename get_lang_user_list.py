"""Returns a list of users of a set language"""
import db_connection as db_con

def get_user_list(language):
    # Create connection
    conn = db_con.db_engine.raw_connection()
    # Initialize cursor
    cur = conn.cursor()

    # SQL Query getting all estoninan user id's
    sql_query = f"SELECT user_id FROM {language}_users_10_prob"

    # Getting the data from database
    data = db_con.read_sql_inmem_uncompressed(sql_query, db_con.db_engine)

    # Making the user id's to a list
    user_list = data['user_id'].to_list()
    # Close connection
    cur.close()
    conn.close()
    return user_list