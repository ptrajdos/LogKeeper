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


class DebugJoinableQueue:
    def __init__(self):
        self.q = mp.JoinableQueue()
        self.manager = mp.Manager()
        self._counter = self.manager.Value("i", 0)
        self._lock = mp.Lock()

    def put(self, item):
        with self._lock:
            self._counter.value += 1
        self.q.put(item)

    def put_nowait(self, item):
        with self._lock:
            self._counter.value += 1
        self.q.put_nowait(item)


    def get(self):
        return self.q.get()

    def task_done(self):
        with self._lock:
            self._counter.value -= 1
        self.q.task_done()

    def join(self):
        self.q.join()

    def pending(self):
        return self._counter.value

    def __getattr__(self, name):
        """Forward other methods like qsize(), empty(), full() etc."""
        return getattr(self.q, name)

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

    def test_start(self):
        temp_dir = tempfile.TemporaryDirectory()

        self.assertTrue(
            len(os.listdir(temp_dir.name)) == 0, "Temporary directory is not empty"
        )

        log_file_path = LogKeeper.generate_file_name(logging_dir_path=temp_dir.name)
        log_keeper = LogKeeper(log_file_path=log_file_path)
        log_keeper.start()

        logger = log_keeper.get_client_logger_instance()
        self.generate_logs(logger, n=100)
        log_keeper.quit()

        self.assertTrue(
            len(os.listdir(temp_dir.name)) > 0,
            "Temporary directory is empty after performing logging",
        )

        self.assertTrue(
            self.has_nonempty_files(temp_dir.name),
            "Temporary contain empty files after performing logging",
        )

    def test_not_daemon(self):
        temp_dir = tempfile.TemporaryDirectory()

        self.assertTrue(
            len(os.listdir(temp_dir.name)) == 0, "Temporary directory is not empty"
        )

        log_file_path = LogKeeper.generate_file_name(logging_dir_path=temp_dir.name)
        log_keeper = LogKeeper(log_file_path=log_file_path, daemon=False)
        log_keeper.start()

        logger = log_keeper.get_client_logger_instance()
        self.generate_logs(logger, n=100)
        log_keeper.quit()

        self.assertTrue(
            len(os.listdir(temp_dir.name)) > 0,
            "Temporary directory is empty after performing logging",
        )

        self.assertTrue(
            self.has_nonempty_files(temp_dir.name),
            "Temporary contain empty files after performing logging",
        )

    def test_root_logger(self):
        temp_dir = tempfile.TemporaryDirectory()

        self.assertTrue(
            len(os.listdir(temp_dir.name)) == 0, "Temporary directory is not empty"
        )

        log_file_path = LogKeeper.generate_file_name(logging_dir_path=temp_dir.name)
        log_keeper = LogKeeper(log_file_path=log_file_path, internal_logger_name=None)
        log_keeper.start()

        logger = log_keeper.get_client_logger_instance()
        self.generate_logs(logger, n=100)
        log_keeper.quit()

        self.assertTrue(
            len(os.listdir(temp_dir.name)) > 0,
            "Temporary directory is empty after performing logging",
        )

        self.assertTrue(
            self.has_nonempty_files(temp_dir.name),
            "Temporary contain empty files after performing logging",
        )

    def test_joinable_queue(self):
        #TODO problem with joinable queue!
        # raise unittest.SkipTest()
        temp_dir = tempfile.TemporaryDirectory()

        self.assertTrue(
            len(os.listdir(temp_dir.name)) == 0, "Temporary directory is not empty"
        )

        log_file_path = LogKeeper.generate_file_name(logging_dir_path=temp_dir.name)
        jq = DebugJoinableQueue()
        log_keeper = LogKeeper(log_file_path=log_file_path, logging_queue=jq, run_threaded=True)
        log_keeper.start()

        logger = log_keeper.get_client_logger_instance(logger_name="Fancy logger")
        self.generate_logs(logger, n=100)
        log_keeper.quit()

        #TODO hangs there
        # pending = jq.pending()
        # self.assertTrue(pending == 0, "There are pending records in the queue")
        

        self.assertTrue(
            len(os.listdir(temp_dir.name)) > 0,
            "Temporary directory is empty after performing logging",
        )

        self.assertTrue(
            self.has_nonempty_files(temp_dir.name),
            "Temporary contain empty files after performing logging",
        )

    def test_start_idempotent(self):
        temp_dir = tempfile.TemporaryDirectory()

        self.assertTrue(
            len(os.listdir(temp_dir.name)) == 0, "Temporary directory is not empty"
        )

        log_file_path = LogKeeper.generate_file_name(logging_dir_path=temp_dir.name)
        log_keeper = LogKeeper(log_file_path=log_file_path)
        log_keeper.start()
        log_keeper.start()

        logger = log_keeper.get_client_logger_instance()
        self.generate_logs(logger, n=100)
        log_keeper.quit()

        self.assertTrue(
            len(os.listdir(temp_dir.name)) > 0,
            "Temporary directory is empty after performing logging",
        )
        self.assertTrue(
            self.has_nonempty_files(temp_dir.name),
            "Temporary contain empty files after performing logging",
        )

    def test_quit_idempotent(self):
        temp_dir = tempfile.TemporaryDirectory()

        self.assertTrue(
            len(os.listdir(temp_dir.name)) == 0, "Temporary directory is not empty"
        )

        log_file_path = LogKeeper.generate_file_name(logging_dir_path=temp_dir.name)
        log_keeper = LogKeeper(log_file_path=log_file_path)
        log_keeper.start()

        logger = log_keeper.get_client_logger_instance()
        self.generate_logs(logger, n=100)
        log_keeper.quit()
        log_keeper.quit()

        self.assertTrue(
            len(os.listdir(temp_dir.name)) > 0,
            "Temporary directory is empty after performing logging",
        )
        self.assertTrue(
            self.has_nonempty_files(temp_dir.name),
            "Temporary contain empty files after performing logging",
        )

    def test_start_quit_default(self):
        log_keeper = LogKeeper(run_threaded=False)
        log_keeper.start()
        self.assertIsInstance(log_keeper._logging_process, mp.Process, "Not running in process")

        logger = log_keeper.get_client_logger_instance(logger_name="Fancy logger")
        self.generate_logs(logger, n=100)
        LogKeeper.shutdown_client_logger(logger)
        log_keeper.quit()

    

    def test_start_quit_default_thread(self):
        log_keeper = LogKeeper(run_threaded=True)
        log_keeper.start()

        self.assertIsInstance(log_keeper._logging_process, th.Thread, "Not running in thread")

        logger = log_keeper.get_client_logger_instance()
        self.generate_logs(logger, n=100)
        log_keeper.quit()

    def test_getting_client_loggers(self):
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

        def compute(name, queue):
            logger = LogKeeper.get_client_logger(logging_queue=queue, logger_name="JLL")
            logger.debug(f"GG?: {name}")
            LogKeeper.shutdown_client_logger(logger)
            return name

        logger = LogKeeper.get_client_logger(logging_queue=queue, logger_name="MPC")
        logger.debug("Before parallel")

        n_total = 5
        rets = Parallel(
            n_jobs=-1,
            total=n_total,
            desc=f"Computations",
        )(delayed(compute)(name, queue) for name in [f"N_{i}" for i in range(n_total)])

        logger.debug("After Parallel!")
        time.sleep(3)
        logger.debug("After sleep")
        lp.quit()
        self.assertTrue(queue == lp.get_logging_queue(), "Queues are not equal")

    def test_rotation(self):
        temp_dir = tempfile.TemporaryDirectory()

        self.assertTrue(
            len(os.listdir(temp_dir.name)) == 0, "Temporary directory is not empty"
        )

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

        self.assertTrue(
            len(os.listdir(temp_dir.name)) == 0, "Temporary directory is not empty"
        )

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
