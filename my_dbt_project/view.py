import duckdb
con = duckdb.connect('my_first_db.duckdb')
print(con.execute("SELECT * FROM main.cleaned_users").df())