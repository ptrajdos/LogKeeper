import io
import logging
import sys
import unittest
from log_keeper.log_keeper import LogKeeper
import tempfile
import os
from joblib import delayed
from joblib import Parallel
import time
import multiprocessing as mp
import threading as th
from pathlib import Path

def compute(name, queue):
            logger = LogKeeper.get_client_logger(logging_queue=queue, logger_name="JLL")
            logger.debug(f"GG?: {name}")
            LogKeeper.shutdown_client_logger(logger)
            return name

def is_test_deamonic():
    try:
        with mp.Manager():
            return False
    except Exception:
        return True

def join_with_timeout(q, timeout=5):
    done = []
    def _join():
        q.join()
        done.append(True)

    t = th.Thread(target=_join)
    t.start()
    t.join(timeout)
    return bool(done)

class LogKeeperTest(unittest.TestCase):

    def generate_logs(self, logger, n=100):
        for i in range(n):
            logger.error(f"Test log: {i} ")

    def has_nonempty_files(self, directory):
        return all(
            os.path.isfile(os.path.join(directory, f))
            and os.path.getsize(os.path.join(directory, f)) > 0
            for f in os.listdir(directory)
        )

    def is_gzip_file(self, filepath):
        try:
            with open(filepath, "rb") as f:
                return f.read(2) == b"\x1f\x8b"
        except Exception:
            return False

    def has_gz_files(self, directory):
        return any(
            os.path.isfile(os.path.join(directory, f))
            and self.is_gzip_file(os.path.join(directory, f))
            for f in os.listdir(directory)
        )

    def check_temp_dir_init(self,temp_dir):
        self.assertTrue(os.path.exists(temp_dir.name), "Temporary Log directory not exists before tests.")
        self.assertTrue(
            len(os.listdir(temp_dir.name)) == 0, "Temporary directory is not empty before tests"
        )
        self.assertTrue(os.access(temp_dir.name, os.W_OK), "Temp directory not writable before tests")
        self.assertTrue(os.access(temp_dir.name, os.R_OK), "Temp directory not readable before tests")
        self.assertTrue(os.access(temp_dir.name, os.X_OK), "Temp directory not executable before tests")


    def check_temp_dir_after(self,temp_dir, count_rows=True, exp_rows = 100):

        self.assertTrue(os.path.exists(temp_dir.name), "Temporary Log directory not exists.")
        self.assertTrue(os.access(temp_dir.name, os.W_OK), "Temp directory not writable after logging.")
        self.assertTrue(os.access(temp_dir.name, os.R_OK), "Temp directory not readable after logging.")
        self.assertTrue(os.access(temp_dir.name, os.X_OK), "Temp directory not executable after logging.")

        self.assertTrue(
            len(os.listdir(temp_dir.name)) > 0,
            "Temporary directory is empty after performing logging",
        )

        self.assertTrue(
            self.has_nonempty_files(temp_dir.name),
            "Temporary contain empty files after performing logging",
        )
        selected_files = list(Path(temp_dir.name).glob("logfile*.log*"))
        self.assertTrue(len(selected_files)>0, "Afert logging Temp directory contains no log files.")

        for f in selected_files:
            self.assertTrue(os.access(f, os.W_OK), f"Logfile {f} is not writable.")
            self.assertTrue(os.access(f, os.R_OK), f"Logfile {f} is not readable.")

        if count_rows:
            for file in selected_files:
                with open(file, "r") as fh:
                    count = sum(1 for _ in fh)
                    self.assertTrue(count == exp_rows, "Wrong number of rows")

    def test_start(self):
        #TODO failure in test_parallel
        # Number of rows in log -- not all rows appear in files
        temp_dir = tempfile.TemporaryDirectory()

        self.check_temp_dir_init(temp_dir=temp_dir)

        log_file_path = LogKeeper.generate_file_name(logging_dir_path=temp_dir.name)
        log_keeper = LogKeeper(log_file_path=log_file_path)
        log_keeper.start()

        logger = log_keeper.get_client_logger_instance()
        n=100
        self.generate_logs(logger, n=n)
        log_keeper.quit()

        self.check_temp_dir_after(temp_dir=temp_dir, count_rows=True, exp_rows=n)
        temp_dir.cleanup()
        

    def test_not_daemon(self):
        temp_dir = tempfile.TemporaryDirectory()
        
        self.check_temp_dir_init(temp_dir=temp_dir)

        log_file_path = LogKeeper.generate_file_name(logging_dir_path=temp_dir.name)
        log_keeper = LogKeeper(log_file_path=log_file_path, daemon=False)
        log_keeper.start()

        logger = log_keeper.get_client_logger_instance()
        n=100
        self.generate_logs(logger, n=n)
        log_keeper.quit()

        self.check_temp_dir_after(temp_dir=temp_dir, count_rows=True, exp_rows=n)
        temp_dir.cleanup()

    def test_root_logger(self):
        temp_dir = tempfile.TemporaryDirectory()

        self.check_temp_dir_init(temp_dir=temp_dir)

        log_file_path = LogKeeper.generate_file_name(logging_dir_path=temp_dir.name)
        log_keeper = LogKeeper(log_file_path=log_file_path, internal_logger_name=None)
        log_keeper.start()

        logger = log_keeper.get_client_logger_instance()
        n=100
        self.generate_logs(logger, n=n)
        log_keeper.quit()

        self.check_temp_dir_after(temp_dir=temp_dir, count_rows=True, exp_rows=n)
        temp_dir.cleanup()

    def test_joinable_queue_threading(self):
        temp_dir = tempfile.TemporaryDirectory()

        self.check_temp_dir_init(temp_dir=temp_dir)

        log_file_path = LogKeeper.generate_file_name(logging_dir_path=temp_dir.name)
        jq = mp.JoinableQueue()
        log_keeper = LogKeeper(log_file_path=log_file_path, logging_queue=jq, run_threaded=True)
        log_keeper.start()

        logger = log_keeper.get_client_logger_instance(logger_name="Fancy logger")
        n=100
        self.generate_logs(logger, n=n)
        log_keeper.quit()
        time.sleep(2)

        self.assertTrue(join_with_timeout(jq, timeout=3))

        self.check_temp_dir_after(temp_dir=temp_dir, count_rows=True, exp_rows=n)
        temp_dir.cleanup()


    def test_start_idempotent(self):
        #TODO failure in test parallel
        # Number of rows in log -- not all rows appear in files
        temp_dir = tempfile.TemporaryDirectory()

        self.check_temp_dir_init(temp_dir=temp_dir)

        log_file_path = LogKeeper.generate_file_name(logging_dir_path=temp_dir.name)
        log_keeper = LogKeeper(log_file_path=log_file_path)
        log_keeper.start()
        log_keeper.start()

        logger = log_keeper.get_client_logger_instance()
        n=100
        self.generate_logs(logger, n=n)
        log_keeper.quit()

        self.check_temp_dir_after(temp_dir=temp_dir, count_rows=True, exp_rows=n)
        temp_dir.cleanup()

    def test_quit_idempotent(self):
        temp_dir = tempfile.TemporaryDirectory()

        self.check_temp_dir_init(temp_dir=temp_dir)

        log_file_path = LogKeeper.generate_file_name(logging_dir_path=temp_dir.name)
        log_keeper = LogKeeper(log_file_path=log_file_path)
        log_keeper.start()

        logger = log_keeper.get_client_logger_instance()
        n=100
        self.generate_logs(logger, n=n)
        log_keeper.quit()
        log_keeper.quit()

        self.check_temp_dir_after(temp_dir=temp_dir, count_rows=True, exp_rows=n)
        temp_dir.cleanup()

    def test_start_quit_default(self):
        log_keeper = LogKeeper(run_threaded=False)
        log_keeper.start()
        if not is_test_deamonic():
            self.assertIsInstance(log_keeper._logging_process, mp.Process, "Not running in process")
        else:
            self.assertIsInstance(log_keeper._logging_process, th.Thread, "Running in thread")

        logger = log_keeper.get_client_logger_instance(logger_name="Fancy logger")
        n=100
        self.generate_logs(logger, n=n)
        LogKeeper.shutdown_client_logger(logger)
        log_keeper.quit()

    

    def test_start_quit_default_thread(self):
        log_keeper = LogKeeper(run_threaded=True)
        log_keeper.start()

        self.assertIsInstance(log_keeper._logging_process, th.Thread, "Not running in thread")

        logger = log_keeper.get_client_logger_instance()
        n=100
        self.generate_logs(logger, n=n)
        log_keeper.quit()

    def test_getting_client_loggers_loky(self):
        #TODO check logs!
        log_dir = tempfile.gettempdir()
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.splitext(os.path.basename(__file__))[0]
        log_file_path = LogKeeper.generate_file_name(
            logging_dir_path=log_dir, name_prefix=log_file
        )

        queue = LogKeeper.generate_logging_queue()

        lp = LogKeeper(
            logging_queue=queue,
            log_file_path=log_file_path,
            run_threaded=False,
        )
        lp.start()

        logger = LogKeeper.get_client_logger(logging_queue=queue, logger_name="MPC")
        logger.debug("Before parallel")

        n_total = 5
        deamonic_test = is_test_deamonic()

        rets = Parallel(
            n_jobs=-1,
            total=n_total,
            desc=f"Computations",
            backend="threading" if deamonic_test else "loky",
        )(delayed(compute)(name, queue) for name in [f"N_{i}" for i in range(n_total)])

        logger.debug("After Parallel!")
        time.sleep(3)
        logger.debug("After sleep")
        lp.quit()
        self.assertTrue(queue == lp.get_logging_queue(), "Queues are not equal")

    def test_getting_client_loggers_mp(self):
        #TODO check logs!
    
        log_dir = tempfile.gettempdir()
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.splitext(os.path.basename(__file__))[0]
        log_file_path = LogKeeper.generate_file_name(
            logging_dir_path=log_dir, name_prefix=log_file
        )

        queue = LogKeeper.generate_logging_queue()

        lp = LogKeeper(
            logging_queue=queue,
            log_file_path=log_file_path,
            run_threaded=False,
        )
        lp.start()


        logger = LogKeeper.get_client_logger(logging_queue=queue, logger_name="MPC")
        logger.debug("Before parallel")

        n_total = 5
        deamonic_test = is_test_deamonic()

        rets = Parallel(
            n_jobs=-1,
            backend="threading" if deamonic_test else "multiprocessing",
        )(delayed(compute)(name, queue) for name in [f"N_{i}" for i in range(n_total)])

        logger.debug("After Parallel!")
        time.sleep(3)
        logger.debug("After sleep")
        lp.quit()
        self.assertTrue(queue == lp.get_logging_queue(), "Queues are not equal")

    def test_rotation(self):
        temp_dir = tempfile.TemporaryDirectory()

        self.check_temp_dir_init(temp_dir=temp_dir)

        log_file_path = LogKeeper.generate_file_name(logging_dir_path=temp_dir.name)
        back_count = 3
        log_keeper = LogKeeper(
            log_file_path=log_file_path, max_bytes=10, backup_count=back_count
        )
        log_keeper.start()

        logger = log_keeper.get_client_logger_instance()
        self.generate_logs(logger, n=1000)
        log_keeper.quit()

        self.assertTrue(
            len(os.listdir(temp_dir.name)) == back_count + 1,
            "Wrong number of files after performing logging",
        )
        
        self.assertTrue(
            self.has_nonempty_files(temp_dir.name),
            "Temporary contain empty files after performing logging",
        )

        self.assertTrue(
            self.has_gz_files(temp_dir.name), "No Gzipped files in the output directory"
        )

    def test_rotation_no_gzip(self):
        temp_dir = tempfile.TemporaryDirectory()

        self.check_temp_dir_init(temp_dir=temp_dir)

        log_file_path = LogKeeper.generate_file_name(logging_dir_path=temp_dir.name)
        back_count = 3
        log_keeper = LogKeeper(
            log_file_path=log_file_path, max_bytes=10, backup_count=back_count,
            gzip_logs=False,
        )
        log_keeper.start()

        logger = log_keeper.get_client_logger_instance()
        self.generate_logs(logger, n=1000)
        log_keeper.quit()

        self.assertTrue(
            len(os.listdir(temp_dir.name)) == back_count + 1,
            "Wrong number of files after performing logging",
        )
        
        self.assertTrue(
            self.has_nonempty_files(temp_dir.name),
            "Temporary contain empty files after performing logging",
        )

        self.assertFalse(
            self.has_gz_files(temp_dir.name), "There are Gzipped files in the output directory"
        )

    def test_stream_handler_threaded(self):
        log_stream = io.StringIO()
        sh = logging.StreamHandler(stream=log_stream)

        #Handlers contain locks. Only thread run available
        log_keeper = LogKeeper(additional_handlers=[sh], run_threaded=True)
        log_keeper.start()

        logger = log_keeper.get_client_logger_instance(logger_name="Fancy logger")
        n=100
        self.generate_logs(logger, n=100)
        LogKeeper.shutdown_client_logger(logger)
        log_keeper.quit()

        stream_value = log_stream.getvalue()
        self.assertTrue(len(stream_value) > 0, "Stream is empty")
        self.assertTrue(f"Test log: {n-1} " in stream_value, f"String stream do not contain 'Test log: {n-1}")

if __name__ == "___main__":
    unittest.main()
