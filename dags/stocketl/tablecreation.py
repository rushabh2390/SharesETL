from database import Database
con = Database.getConnection()
cur = con.cursor()

# creating stock table which will have the stock name
cur.execute("CREATE TABLE IF NOT EXISTS stock_details(id SERIAL,stock_name VARCHAR UNIQUE, stock_code text, stock_news_code text, stock_value_latest_date date, stock_news_latest_date date)")
# cur.execute("insert into stock_details (stock_name, stock_code, stock_news_code, stock_value_latest_date, stock_news_latest_date) values('ABB India','ABB','ABB','2000-01-01','2000-01-01') ON CONFLICT (stock_name) DO NOTHING;")
# cur.execute("insert into stock_details (stock_name, stock_code, stock_news_code, stock_value_latest_date, stock_news_latest_date) values('Bajaj Electricals','BE','BE','2000-01-01','2000-01-01') ON CONFLICT (stock_name) DO NOTHING;")
# cur.execute("insert into stock_details (stock_name, stock_code, stock_news_code, stock_value_latest_date, stock_news_latest_date) values('Bajaj Hindusthan Sugar','BH','BH06','2000-01-01','2000-01-01') ON CONFLICT (stock_name) DO NOTHING;")
cur.execute("insert into stock_details (stock_name, stock_code, stock_news_code, stock_value_latest_date, stock_news_latest_date) values('Infosys','IT','IT','2000-01-01','2000-01-01') ON CONFLICT (stock_name) DO NOTHING;")
# cur.execute("insert into stock_details (stock_name, stock_code, stock_news_code, stock_value_latest_date, stock_news_latest_date) values('Raymond Limited','R','R','2000-01-01','2000-01-01') ON CONFLICT (stock_name) DO NOTHING;")
cur.execute("insert into stock_details (stock_name, stock_code, stock_news_code, stock_value_latest_date, stock_news_latest_date) values('Hindalco Industries','H','HI','2000-01-01','2000-01-01') ON CONFLICT (stock_name) DO NOTHING;")

# creating data table which will have the stock data
cur.execute("CREATE TABLE IF NOT EXISTS stock_data_history(id SERIAL,stock_code text, ondate date,openat integer, high integer, low integer, closeat integer, volume integer, hl integer, oc integer)")
cur.execute("CREATE TABLE IF NOT EXISTS stock_news_history(id SERIAL,stock_code text, ondate date, headline text, news text, sentiment_analysis text)")

con.commit()


