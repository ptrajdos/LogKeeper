import os
from log_keeper.log_keeper import LogKeeper
from joblib import delayed
from joblib import Parallel
import time
import logging
import sys

from settings import RESULSTPATH

if __name__ == '__main__':

    output_directory = os.path.join(RESULSTPATH,"./logging_test")
    os.makedirs(output_directory, exist_ok=True)

    log_dir = output_directory
    log_file = os.path.splitext(os.path.basename(__file__))[0]
    log_file_path = LogKeeper.generate_file_name(logging_dir_path=log_dir, name_prefix=log_file)
    print(log_file_path)
    queue = LogKeeper.generate_logging_queue()
    sh = logging.StreamHandler(stream=sys.stderr)
    lp  = LogKeeper(logging_queue=queue,log_file_path=log_file_path, run_threaded=False, additional_handlers=[sh] )
    lp.start()

    def compute(name, queue):
        logger = LogKeeper.get_client_logger(logging_queue=queue, logger_name="JLL")
        logger.debug(f"GG?: {name}")
        LogKeeper.shutdown_client_logger(logger)
        # raise Exception("A") #This causes EOF!
        return name

    logger = LogKeeper.get_client_logger(logging_queue=queue,logger_name="MPC")
    logger.debug("Before parallel")

    n_total = 5
    rets = Parallel(
            n_jobs=-1,
            total=n_total,
            desc=f"Computations",
        )(delayed(compute)(name,queue) for name in [f"N_{i}" for i in range(n_total)] )

    logger.debug("After Parallel!")
    time.sleep(3)
    logger.debug("After sleep")
    lp.quit()