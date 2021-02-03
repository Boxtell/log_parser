import os


class EnvConnector:
    def __init__(self, env_path):
        self.env_path = env_path

    def get_directory(self):
        for file in os.listdir(path=self.env_path):
            if not file.startswith('.'):
                yield file
