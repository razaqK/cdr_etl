from sqlalchemy import create_engine

class cdr_settings():
    
#     engine = create_engine('sqlite:////home/osayamen/prog/python_etl/python_etl/script/cdr_etl_db.db')
    engine = create_engine("mysql+pymysql://root:rootroot@localhost/mydb", encoding='latin1')
    working_dir='/home/osayamen/prog/python_etl/python_etl/'
    archive_dir='/home/osayamen/prog/python_etl/python_etl/'
      
# sqlacodegen  mysql://root:r@@t@locahost/mydb --outfile mysql_db 