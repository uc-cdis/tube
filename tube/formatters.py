class BaseFormatter(object):
    def __init__(self):
        self.name = "Base"

    def format_line(self, s):
        return s


class HtmlFormatter(BaseFormatter):
    def __init__(self):
        super(HtmlFormatter, self).__init__()
        self.name = "Html"

    def format_line(self, s):
        return s + "<br/>\n"


class TextFileFormatter(BaseFormatter):
    def __init__(self):
        super(TextFileFormatter, self).__init__()
        self.name = "Html"

    def format_line(self, s):
        return s + "\n"
