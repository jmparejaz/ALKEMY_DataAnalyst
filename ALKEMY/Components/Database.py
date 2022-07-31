
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from decouple import config as cnfg
from dotenv import dotenv_values

config = dotenv_values(".env")
fileenv_keys=list(config.keys())
config_dict=dict(config.items())


def get_engine(user,passwd, host, port, db):
    url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
    if not database_exists(url):
        create_database(url)
    engine = create_engine(url,pool_size=5, echo=False)
    return engine


def get_engine_from_settings():
    keys=['USER_NAME','PASSWORD','HOST_NAME','PORT_ID','DATABASE_NAME']
    
    if not all(key in keys for key in fileenv_keys):
        raise Exception('Archivo Config Malo, Solo deben existir las siguientes variables de entorno: \n %s\n %s \n %s\n %s \n %s'%(keys[0],keys[1],keys[2],keys[3],keys[4]))
    return get_engine(config_dict['USER_NAME'],
                   config_dict['PASSWORD'],
                   config_dict['HOST_NAME'],
                   config_dict['PORT_ID'],
                   config_dict['DATABASE_NAME'])


def get_session():
    engine = get_engine_from_settings()
    session = sessionmaker(bind=engine)()
    return session
