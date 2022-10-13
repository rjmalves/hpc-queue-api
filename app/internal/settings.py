import os


class Settings:
    clusterId = os.getenv("CLUSTER_ID")
    scheduler = os.getenv("SCHEDULER", "SGE")

    @classmethod
    def read_environments(cls):
        cls.clusterId = os.getenv("CLUSTER_ID")
        cls.scheduler = os.getenv("SCHEDULER")
