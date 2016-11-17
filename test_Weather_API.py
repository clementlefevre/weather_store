from models import DB_tool
import weather_API_service


def test_weather_API():
    db_tool = DB_tool()
    sites = db_tool.get_sites()
    site = sites[40]
    db_tool.delete_weather_data()

    weather_API_service.copy_API_data_to_db(site)

    assert len(db_tool.get_weather_data()) > 0
