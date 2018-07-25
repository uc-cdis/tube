class SparkBase(object):
    """
    The main entry point into the index export process for the mutation indices
    """
    def __init__(self, sc, config):
        self.sc = sc
        self.config = config
