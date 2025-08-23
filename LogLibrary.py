from loguru import logger
import json
import sys
import os

'''
# To use the Log Library, first import the necessary functions:
# 'Load_Config' for loading configuration settings.
# 'Loguru_Logging' for initializing the logger.
from LogLibrary import Load_Config, Loguru_Logging

# To call the functions and set up your logger:
# 1. Load your default configuration. This sets up how your logs will behave (e.g., where they're saved, log level).
config = Load_Config(default_config)
# 2. Initialize the logger with your configuration, program name, and version.
#    This makes the logger ready to record messages for your specific application.
logger = Loguru_Logging(config, Program_Name, Program_Version)

# To use default config minimum
default_config = {
            "log_Level": "DEBUG",
            "Log_Console": 1,  # Set to "true" to enable console logging.
            "log_Backup": 90,         # Log retention duration (number of backup files).
            "Log_Size": "10 MB"       # Maximum log file size before rotation.
        }

'''

def Load_Config(default_config,Program_Name):
    # Define the configuration file path.
    config_file_path = f'{Program_Name}.config.json'

    # Create config file with default values if it does not exist.
    if not os.path.exists(config_file_path):
        default_config = default_config 
        with open(config_file_path, 'w') as new_config_file:
            json.dump(default_config, new_config_file, indent=4)

    # Load configuration
    with open(config_file_path, 'r') as config_file:
        config = json.load(config_file)
    
    return config

# ----------------------- Loguru Logging Setup -----------------------
def Loguru_Logging(config,Program_Name,Program_Version):
    logger.remove()

    log_Backup = int(config['log_Backup'])
    Log_Size = config['Log_Size']
    log_Level = config['log_Level']

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    log_file_name = f'{Program_Name}_{Program_Version}.log'
    log_file = os.path.join(log_dir, log_file_name)

    if config['Log_Console'] == 1:
        logger.add(
            sys.stdout, 
            level=log_Level, 
            format="<green>{time}</green> | <blue>{level}</blue> | <cyan>{thread.id}</cyan> | <magenta>{function}</magenta> | {message}"
        )

    logger.add(
        log_file,
        format="{time} | {level} | {thread.id} | {function} | {message}",
        level=log_Level,
        rotation=Log_Size,
        retention=log_Backup,
        compression="zip"
    )

    logger.info('-' * 117)
    logger.info('')
    logger.info(f"Start {Program_Name} Version {Program_Version}")
    logger.info('')
    logger.info('-' * 117)

    return logger