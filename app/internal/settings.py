import os


class Settings:
    clusterId = os.getenv("CLUSTER_ID", "0")
    scheduler = os.getenv("SCHEDULER", "SGE")

    @classmethod
    def read_environments(cls):
        cls.clusterId = os.getenv("CLUSTER_ID", "0")
        cls.scheduler = os.getenv("SCHEDULER", "SGE")
