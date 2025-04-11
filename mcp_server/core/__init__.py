from ..config import config
from .storage import load as load_storage
from .version import load as load_version


def load():
    # 加载配置
    cfg = config.load_config()

    # 版本
    load_version(cfg)
    # 存储业务
    load_storage(cfg)


