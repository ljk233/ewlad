import staging
from core.env import create_env
from core import registry
from pipeline.stage import stage_files


if __name__ == "__main__":
    env = create_env()
    registry.register_functions(staging)
    manifiest = stage_files(env)
