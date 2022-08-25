

import logging

#LOGGER UNIVERSIDAD TECNOLÓGICA NACIONAL
#create logger
logger = logging.getLogger('Universidad Tecnológica Nacional')
logger.setLevel(logging.DEBUG)
#create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
#create formater
format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#add formater to console handler
console_handler.setFormatter(format)
#add console handler to logger
logger.addHandler(console_handler)

logger.debug('mensaje')



#LOGGER UNIVERSIDAD NACIONAL TRES DE FEBRERO
#create logger
logger = logging.getLogger('Universidad Nacional Tres de Febrero')
logger.setLevel(logging.DEBUG)
#create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
#create formater
format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#add formater to console handler
console_handler.setFormatter(format)
#add console handler to logger
logger.addHandler(console_handler)

logger.debug('mensaje')