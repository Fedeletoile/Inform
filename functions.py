import dotenv
import fdb
from sqlalchemy import create_engine

config = dotenv.dotenv_values()


def connectionDistriPPAL():
    return fdb.connect(
        database=config["FLXX_DISTRI_PPAL_DATABASE"],
        user=config["FLXX_DISTRI_PPAL_USER"], password=config["FLXX_DISTRI_PPAL_PASSWORD"], host=config["FLXX_DISTRI_PPAL_HOST"])


def connectionDistriDS():
    return fdb.connect(
        database=config["FLXX_DISTRI_DS_DATABASE"],
        user=config["FLXX_DISTRI_DS_USER"], password=config["FLXX_DISTRI_DS_PASSWORD"], host=config["FLXX_DISTRI_DS_HOST"])


def connectionDimesPPAL():
    return fdb.connect(
        database=config["FLXX_DIMES_PPAL_DATABASE"],
        user=config["FLXX_DIMES_PPAL_USER"], password=config["FLXX_DIMES_PPAL_PASSWORD"], host=config["FLXX_DIMES_PPAL_HOST"])


def connectionDimesDS():
    return fdb.connect(
        database=config["FLXX_DIMES_DS_DATABASE"],
        user=config["FLXX_DIMES_DS_USER"], password=config["FLXX_DIMES_DS_PASSWORD"], host=config["FLXX_DIMES_DS_HOST"])

def localCon():
    try:
        return create_engine("sqlite:///flxxLocal.sqlite")
    except Exception as e:
        print(e)