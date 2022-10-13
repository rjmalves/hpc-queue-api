import os


class Settings:
    clusterId = os.getenv("CLUSTER_ID")
    scheduler = os.getenv("SCHEDULER", "SGE")
