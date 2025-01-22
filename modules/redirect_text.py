# --------------------------------------------------------------------------------------------------------------------------------
# Name:        redirect_text
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
# --------------------------------------------------------------------------------------------------------------------------------


class RedirectText:
    def __init__(self, text_var, log_output):
        self.output = text_var
        self.log_output = log_output

    def write(self, string):
        self.output.put(string)
        self.log_output.appendOutputToLog(string)

    def flush(self):  # Necessary for compatibility with `sys.stdout` and `sys.stderr`
        pass
