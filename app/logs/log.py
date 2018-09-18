import logging
logging.basicConfig(level=logging.INFO,
                    filename='/var/log/rulelog/error.log',
                    filemode='a',
                    format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )
