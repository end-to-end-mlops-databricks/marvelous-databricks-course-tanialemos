import logging


class Logger:
    def __init__(self, name, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        ch = logging.StreamHandler()
        ch.setLevel(level)

        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)

        self.logger.addHandler(ch)

    def info(self, message):
        self.logger.info(message)

    def error(self, message: str):
        self.logger.error(message)
