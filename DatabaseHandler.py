import mysql.connector as mysql
class Handler:
    def __init__(self):
        auth_table = "auth"     # Name of the auth table 
        self.DB_IP = "1.1.1.1"       # Database ip 
        self.DB_USER = "user"
        self.DB_DATABASE = "default"
        self.DB_PASS = "123"
        
        self.db = mysql.connect(host = self.DB_IP, database = self.DB_DATABASE, user = self.DB_USER, password = self.DB_PASS, auth_plugin = "mysql_native_password")  
    
    def reset_connection(self):
        self.db.close()
        self.db = mysql.connect(host = self.DB_IP, database = self.DB_DATABASE, user = self.DB_USER, password = self.DB_PASS, auth_plugin = "mysql_native_password")   
        
    def genel_sql(self, sql_str: str) -> int:
        db_cursor = self.db.cursor()
        db_cursor.execute(sql_str)
        self.db.commit()
        db_cursor.close()
        return db_cursor.rowcount    

    def get_item(self, sql_str: str):        
        db_cursor = self.db.db_connection.cursor()
        db_cursor.execute(sql_str)
        column_names = [desc[0] for desc in db_cursor.description]
        sonuc = db_cursor.fetchall()
        self.db.db_connection.commit()
        db_cursor.close()
        return sonuc, column_names