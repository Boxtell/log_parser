import yaml

from pymongo import MongoClient


class MongoConnector:
    def __init__(self):
        with open("Config/config.yaml", "r") as yaml_file:
            self.config = yaml.load(yaml_file, Loader=yaml.FullLoader)

    def mongo_connect(self, db_name):
        return MongoClient("mongodb://{}:{}@{}:{}/{}".format(
            self.config["mongo"]["user"],
            self.config["mongo"]["password"],
            self.config["mongo"]["host"],
            self.config["mongo"]["port"],
            db_name)
        )

    @staticmethod
    def mongo_get_from_key(client, key_id):
        spiders_db = client["spiders"]
        return spiders_db.ads.find_one({"internal_id": key_id})
