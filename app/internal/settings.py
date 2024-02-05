import os


class Settings:
    clusterId = os.getenv("CLUSTER_ID", "0")
    scheduler = os.getenv("SCHEDULER", "SGE")
    max_slots = int(os.getenv("INTERNAL_SCHEDULER_MAX_SLOTS", 16))
    programPathRule = os.getenv("PROGRAM_PATH_RULE", "PEMAWS")
    host = os.getenv("HOST", "localhost")
    port = int(os.getenv("PORT", "80"))
    root_path = os.getenv("ROOT_PATH", "/")

    @classmethod
    def read_environments(cls):
        cls.clusterId = os.getenv("CLUSTER_ID", "0")
        cls.scheduler = os.getenv("SCHEDULER", "SGE")
        cls.max_slots = int(os.getenv("INTERNAL_SCHEDULER_MAX_SLOTS", 16))
        cls.programPathRule = os.getenv("PROGRAM_PATH_RULE", "PEMAWS")
        cls.host = os.getenv("HOST", "localhost")
        cls.port = int(os.getenv("PORT", "80"))
        cls.root_path = os.getenv("ROOT_PATH", "/")
