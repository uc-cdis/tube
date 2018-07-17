class RDDBuilder(object):
    def __init__(self, sc, config):
        self.config = config
        self.logger = config.logger
        self.sc = sc

    def build(self):
        """
        Builds rdd from hdfs files.
        """


