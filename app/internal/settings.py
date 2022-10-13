import os


class Settings:
    clusterId = os.getenv("CLUSTER_ID", "0")
    scheduler = os.getenv("SCHEDULER", "SGE")
    host = os.getenv("HOST", "localhost")
    port = int(os.getenv("PORT", "80"))
    root_path = os.getenv("ROOT_PATH", "/")

    @classmethod
    def read_environments(cls):
        cls.clusterId = os.getenv("CLUSTER_ID", "0")
        cls.scheduler = os.getenv("SCHEDULER", "SGE")
        cls.host = os.getenv("HOST", "localhost")
        cls.port = int(os.getenv("PORT", "80"))
        cls.root_path = os.getenv("ROOT_PATH", "/")
