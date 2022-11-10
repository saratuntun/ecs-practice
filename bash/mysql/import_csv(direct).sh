source export_mysql_credentials.sh

# csvsql --dialect mysql --snifflimit 100000 bigdatafile.csv > maketable.sql

csvsql --db "mysql+mysqldb://root:${sql_root_pwd}@localhost:3306/movielog" --tables stream --insert 'data/movelog6-cleaned.csv'