import hashlib
import argparse
import logging
import multiprocessing
import os
import time
from typing import Tuple


def get_sys_logger():
    logger = logging.getLogger('run')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)
    logger.setLevel(level=logging.INFO)
    return logger


log = get_sys_logger()

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_CONCURRENT = multiprocessing.cpu_count()


def get_file_md5(file_path):
    ins = hashlib.md5()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(4 * 1024)
            if data:
                ins.update(data)
            else:
                return ins.hexdigest()


def get_result_writer(result_file_name):
    writer = logging.getLogger('resultLog')
    writer.setLevel(logging.INFO)
    handler = logging.FileHandler(filename=result_file_name, encoding='utf-8', mode='w')
    handler.setFormatter(logging.Formatter('%(message)s'))
    writer.addHandler(handler)
    return writer


def parse_args():
    parser = argparse.ArgumentParser(description='File digest Calculator')
    parser.add_argument('--digest', type=str, default='MD5',
                        help='the digest method to use,default is MD5,only support MD5 now', choices=['MD5'])
    parser.add_argument('--scan', type=str, default='.',
                        help='the dir path to scan,default is current dir: ' + os.path.realpath('.'))
    parser.add_argument('--output', type=str, default='files_md5.csv',
                        help='csv file path for output result,default is in current dir,named files_md5.csv')
    parser.add_argument('--format', type=str, default='CSV', choices=['CSV'],
                        help='the output result format,default is CSV,only support CSV now')
    parser.add_argument('--concurrent', type=int, default=DEFAULT_CONCURRENT,
                        help=f'the digest worker number,default is cpu count {str(DEFAULT_CONCURRENT)}')
    args = parser.parse_args()
    return os.path.realpath(args.scan), os.path.realpath(args.output), args.concurrent


def calculate_worker(abspath: str):
    modify_time = os.path.getmtime(abspath)
    create_time = os.path.getctime(abspath)
    size = os.path.getsize(abspath)
    result = get_file_md5(abspath)
    return '%s,%s,%s,%s,%s,%s,%s' % (abspath, os.path.dirname(abspath), os.path.basename(abspath),
                                     time.strftime(TIME_FORMAT, time.localtime(modify_time)),
                                     time.strftime(TIME_FORMAT, time.localtime(create_time)), size, result)


def calculate_dir(scan_dir: str, result_file_name: str, concurrent: int):
    start_time = time.time()
    log.info('start scan files in %s, digest result is writing to %s', scan_dir, result_file_name)
    log.info('current digest worker number is %s', concurrent)
    result_writer = get_result_writer(result_file_name)
    result_writer.info('abspath,file_dir,file_name,modify_time,create_time,size(B),digest')
    worker_pool = multiprocessing.Pool(processes=concurrent)

    def calculate_callback(calculate_result):
        result_writer.info(calculate_result)

    file_count = 0
    for root, dirs, files in os.walk(scan_dir):
        dir_file_count = 0
        for file_name in files:
            file = os.path.join(root, file_name)
            worker_pool.apply_async(calculate_worker, (file,), callback=calculate_callback)
            file_count += 1
            dir_file_count += 1
        log.info('[%s] files count %s', root, dir_file_count)
    log.info('done scan dir files,total file count %s,waiting worker finish.', file_count)
    worker_pool.close()
    worker_pool.join()
    log.info('all done.result file was written to %s,cost %ss', result_file_name, (time.time() - start_time))


if __name__ == '__main__':
    calculate_dir(*parse_args())
