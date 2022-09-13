
#   logger named as root
keys=root

#   handler named as console
keys=console

#   formatter named as std_out
keys=std_out

#   log level as INFO and handler as console
handlers = console
level = INFO

#   definition of console handler 
class = logging.StreamHandler
level = INFO
formatter = std_out

#   definition of file handler 
class = logging.FileHandler

#class = logging.handlers.TimedRotatingFileHandler

kwargs = {"filename": "messages_conf.log", "when": "D","interval": "7"}

level = INFO
formatter = std_out

#   formatter definition: """%A-%B-%Y--levelname--name--message"""
[formatter_std_out]
format = %(asctime)s : %(levelname)s : %(name)s : %(message)s
datefmt = %Y-%m-%d %I:%M:%S