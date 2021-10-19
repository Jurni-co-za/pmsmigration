import pms_methods, connection
import pyodbc


def main():
    # r = api.Run()
    conn = pms_methods.PMS_Methods.read_json()
    # img = pms_methods.PMS_Methods.copy_images()
    # print(conn)
    # username= "Lukhanyo"
    # password= "Fjr%&P$181#"
    # host= "jurnitraveldev.database.windows.net"
    # database_name= "jurnitraveluat-db"
    cnxn = pyodbc.connect(
        'Driver={ODBC Driver 17 for SQL Server};'
        'Server=jurnitraveldev.database.windows.net;'
        'Database=jurnitraveluat-db;'
        'UID=Lukhanyo;'
        'PWD=Fjr%&P$181#;'
    )
    # cnxn = pyodbc.connect(DRIVER='{ODBC Driver 17 for SQL Server};SERVER=' +host + ';DATABASE=' +database_name + ';UID=' +username + ';PWD=' +password)
    # c = conn.cursor()
    # print(r2)



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # unittest.main()
    main()
    print("done")