from time import sleep

from app import Utm, Result
from utils import parse_utm

while True:
    results = [parse_utm(utm) for utm in Utm.get_active()]
    Result.save_many(results)
    sleep(60)
