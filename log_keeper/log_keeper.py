import multiprocessing as mp
import sys
import threading as th
import logging
import logging.handlers
from datetime import datetime
import os
import gzip
import shutil
import uuid
import tempfile
import queue


class RotatingFileHandlerGz(logging.handlers.RotatingFileHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.rotator = self.gz_rotator
        self.namer = self.gz_namer

    def gz_rotator(self, source, dest):
        """
        Compress source log file to dest with gzip.
        """
        with open(source, "rb") as f_in:
            with gzip.open(dest, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        try:
            os.remove(source)
        except FileNotFoundError:
            pass

    def gz_namer(self, default_name):
        """
        Rename rotated file from
        """
        new_name = default_name.replace(".log.", ".log.gz.")
        return new_name


class LogKeeper:

    _sentinel = None

    @staticmethod
    def generate_file_name(logging_dir_path, name_prefix="logfile"):
        date_string = datetime.now().strftime("%Y_%m_%d_%H-%M-%S-%f")
        log_filename = "{}_{}.log".format(name_prefix, date_string)
        log_file_path = os.path.join(logging_dir_path, log_filename)

        return log_file_path

    @staticmethod
    def generate_logging_queue():
        try:
            return mp.Manager().Queue()
        except Exception as e:
            return queue.Queue()

    @staticmethod
    def get_client_logger(
        logging_queue, logging_level=logging.DEBUG, logger_name=None
    ) -> logging.Logger:
        qh = logging.handlers.QueueHandler(logging_queue)
        logger = logging.getLogger(name=logger_name)
        logger.setLevel(logging_level)
        logger.addHandler(qh)
        return logger

    @staticmethod
    def shutdown_client_logger(logger: logging.Logger):
        for handler in logger.handlers[:]:
            handler.flush()
            handler.close()
            logger.removeHandler(handler)

    @staticmethod
    def get_default_log_formatter():
        log_format_str = "%(asctime)s.%(msecs)03d;%(name)s;%(levelname)s;[%(processName)s - %(threadName)s]:%(message)s"
        log_date_format = "%Y-%m-%d %H:%M:%S"
        return logging.Formatter(fmt=log_format_str, datefmt=log_date_format)

    def __init__(
        self,
        name="Logger Base",
        daemon=True,
        log_file_path=None,
        logging_queue=None,
        logging_level=logging.DEBUG,
        max_bytes=2**30,
        backup_count=3,
        internal_logger_name="LoggerBaseInternal",
        log_format_str=None,
        log_date_format=None,
        run_threaded=False,
        additional_handlers=None,
        gzip_logs=True,
    ) -> None:

        self.name = name
        self.daemon = daemon
        self.log_file_path = log_file_path
        self.logging_queue = logging_queue
        self.logging_level = logging_level
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.internal_logger_name = internal_logger_name
        self.log_format_str = log_format_str
        self.log_date_format = log_date_format
        self.run_threaded = run_threaded
        self.additional_handlers = additional_handlers
        self.gzip_logs = gzip_logs

        self._logging_process = None

    def run(self) -> None:

        open(self.log_file_path, "a").close()
        assert os.path.exists(self.log_file_path), "Log file has not been created"

        if self.gzip_logs:
            handler = RotatingFileHandlerGz(
                filename=self.log_file_path,
                maxBytes=self.max_bytes,
                backupCount=self.backup_count,
                delay=True,
            )
        else:
            handler = logging.handlers.RotatingFileHandler(
                filename=self.log_file_path,
                maxBytes=self.max_bytes,
                backupCount=self.backup_count,
                delay=True,
            )

        handler.setFormatter(self.get_log_formatter())

        logging.captureWarnings(True)
        if self.internal_logger_name is not None:
            unique_logger_name = f"{self.internal_logger_name}_{uuid.uuid4()}"
        else:
            # Root logger
            unique_logger_name = self.internal_logger_name

        logger = logging.getLogger(unique_logger_name)        
        logger.setLevel(self.logging_level)
        logger.addHandler(handler)

        if self.additional_handlers is not None:
            for handler in self.additional_handlers:
                logger.addHandler(handler)

        has_task_done = hasattr(self.logging_queue, "task_done")

        while True:
            try:
                record = self.logging_queue.get()
                if record is LogKeeper._sentinel:
                    if has_task_done:
                        self.logging_queue.task_done()
                    break

                assert os.path.exists(self.log_file_path), "Log file has been deleted"
                logger.handle(record)
                if has_task_done:
                    self.logging_queue.task_done()
            except (EOFError, BrokenPipeError) as e:
                print(f"Exception during handling queue record: {e}",file=sys.stderr)
            except Exception as e:
                print(f"Unexpected exception during handling queue record: {e}",file=sys.stderr)
                raise e
            
        for i_handler in logger.handlers[:]:
            i_handler.flush()
            i_handler.close()
            logger.removeHandler(i_handler)
            
        return None

    def _enqueue_sentinel(self):
        self.logging_queue.put_nowait(LogKeeper._sentinel)

    def start(self):

        if self._logging_process is None:

            if self.logging_queue is None:
                self.logging_queue = LogKeeper.generate_logging_queue()

            if self.log_file_path is None:
                temp_dir = tempfile.mkdtemp(prefix=str(os.getpid()))
                self.log_file_path = LogKeeper.generate_file_name(
                    logging_dir_path=temp_dir
                )

            try:
                if self.run_threaded:
                    raise AssertionError("Force Thread")

                self._logging_process = mp.Process(
                    target=self.run,
                    name=self.name,
                    daemon=self.daemon,
                )
                self._logging_process.start()
            except AssertionError as e:
                self._logging_process = th.Thread(
                    target=self.run,
                    name=self.name,
                    daemon=self.daemon,
                )
                self._logging_process.start()
            except Exception as e:
                raise e

    def get_logging_queue(self):
        return self.logging_queue

    def get_client_logger_instance(
        self, logging_level=logging.DEBUG, logger_name="ClientLogger"
    ):
        return LogKeeper.get_client_logger(
            logging_queue=self.logging_queue,
            logging_level=logging_level,
            logger_name=logger_name,
        )

    def quit(self):
        if self._logging_process is not None:
            self._enqueue_sentinel()
            self._logging_process.join()
            self._logging_process = None

    def get_log_formatter(self):
        log_format_str = (
            "%(asctime)s.%(msecs)03d;%(name)s;%(levelname)s;[%(processName)s - %(threadName)s]:%(message)s"
            if self.log_format_str is None
            else self.log_format_str
        )
        log_date_format = (
            "%Y-%m-%d %H:%M:%S"
            if self.log_date_format is None
            else self.log_date_format
        )

        return logging.Formatter(fmt=log_format_str, datefmt=log_date_format)
