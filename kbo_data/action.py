import sys
import kbodata
from datetime import date, timedelta
import sqlalchemy as db


if __name__ == "__main__":
    before_2days = date.today() - timedelta(2)
    schedule = kbodata.get_daily_schedule(before_2days.year,before_2days.month,before_2days.day,"chromedriver")
    print(before_2days)

    if len(schedule) == 0:
        print("이틀 전 경기 데이터가 없습니다.")
        pass
    else:
        data = kbodata.get_game_data(schedule,"chromedriver")
        print("데이터 수집이 완료되었습니다.")
        scoreboard = kbodata.scoreboard_to_Dict(data)
        batter = kbodata.batter_to_Dict(data)
        pitcher = kbodata.pitcher_to_Dict(data)
        print("데이터 변환이 완료되었습니다.")
        DB_URL = str(sys.argv[1])
    
        engine = db.create_engine(DB_URL)
        connection = engine.connect()
        metadata = db.MetaData()
        scoreboard_table = db.Table("scoreboard", metadata, autoload=True, autoload_with=engine)
        scd_query = db.insert(scoreboard_table)
        scd = connection.execute(scd_query, scoreboard)
        scd.close()
        print("scoreboard 데이터 적재가 완료되었습니다.")
    
        engine = db.create_engine(DB_URL)
        connection = engine.connect()
        metadata = db.MetaData()
        batter_table = db.Table("batter", metadata, autoload=True, autoload_with=engine)
        bat_query = db.insert(batter_table)
        bat = connection.execute(bat_query, batter)
        bat.close()
        print("batter 데이터 적재가 완료되었습니다.")
    
        engine = db.create_engine(DB_URL)
        connection = engine.connect()
        metadata = db.MetaData()
        pitcher_table = db.Table("pitcher", metadata, autoload=True, autoload_with=engine)
        pit_query = db.insert(pitcher_table)
        pit = connection.execute(pit_query, pitcher)
        pit.close()
        print("pitcher 데이터 적재가 완료되었습니다.")
