from common.secrets.secret import SecretsManager

import logging
import common.settings
import traceback

def main():
    try:
        common.settings.init_logging()
        secrets_manager = SecretsManager()
        #secrets_manager.getAllSecrets()
        
    except:
        logging.error(traceback.format_exc())

	


if __name__ == '__main__':
    main()
